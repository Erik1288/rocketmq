/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.broker.pagecache.BatchManyMessageTransfer;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.broker.plugin.PullMessageResultHandler;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.topic.OffsetMovedEvent;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DefaultPullMessageResultHandler implements PullMessageResultHandler {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected final BrokerController brokerController;

    public DefaultPullMessageResultHandler(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public PullMessageResult handle(List<StoreReturnedResult> storeReturnedResults, RemotingCommand response) {
        PullMessageResult globalPullMessageResult = new PullMessageResult();

        List<Pair<StoreReturnedResult, PullMessageResult>> pullMessageResults = storeReturnedResults
                .stream()
                .map(storeReturnedResult -> new Pair<>(storeReturnedResult, handle(storeReturnedResult)))
                .collect(Collectors.toList());

        List<Pair<StoreReturnedResult, PullMessageResult>> pullMessageResultsWithData = pullMessageResults
                .stream()
                .filter(pair -> !pair.getObject2().isAsyncResponse())
                .collect(Collectors.toList());

        StoreReturnedResult sample = storeReturnedResults.get(0);
        boolean allHaveInsufficientData = pullMessageResultsWithData.size() == 0;
        boolean zeroCopy = !sample.isTransferMsgByHeap();
        boolean brokerAllowSuspend = sample.isBrokerAllowSuspend();

        if (allHaveInsufficientData) {
            // global long-polling
            final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(sample.getRequestHeader().getSysFlag());
            final long suspendTimeoutMillisLong = hasSuspendFlag ? sample.getRequestHeader().getSuspendTimeoutMillis() : 0;

            if (brokerAllowSuspend && hasSuspendFlag) {
                long pollingTimeMills = suspendTimeoutMillisLong;
                if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                }
                // the shared pull future.
                CompletableFuture<RemotingCommand> pullFuture = new CompletableFuture<>();

                final long finalPollingTimeMills = pollingTimeMills;
                List<Pair<StoreReturnedResult, PullRequest>> pullRequests = pullMessageResults
                        .stream()
                        .map(pair -> {
                            PullRequest pullRequest = new PullRequest(
                                    pair.getObject1().getRequest(),
                                    pair.getObject1().getClientHost(),
                                    finalPollingTimeMills,
                                    DefaultPullMessageResultHandler.this.brokerController.getMessageStore().now(),
                                    pair.getObject1().getRequestHeader().getQueueOffset(),
                                    pair.getObject1().getMessageFilter(),
                                    pullFuture
                            );
                            return new Pair<>(pair.getObject1(), pullRequest); })
                        .collect(Collectors.toList());

                // Suspend every single pullRequest
                pullRequests.forEach(pullRequestPair -> {
                    StoreReturnedResult storeReturnedResult = pullRequestPair.getObject1();
                    PullRequest pullRequest = pullRequestPair.getObject2();
                    String topic = storeReturnedResult.getRequestHeader().getTopic();
                    Integer queueId = storeReturnedResult.getRequestHeader().getQueueId();
                    DefaultPullMessageResultHandler.this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
                });

                globalPullMessageResult.setAsyncResponse(pullFuture);
                return globalPullMessageResult;
            }
            return globalPullMessageResult;
        }

        // Some of the topic-queues have no data. Return the currently fetched data and ignore requests that could not pull data.
        if (zeroCopy) {
            // global zero-copy
            List<ByteBuffer> headerByteBuffers = pullMessageResultsWithData
                    .stream()
                    .map(pair -> response.encodeHeader(pair.getObject1().getGetMessageResult().getBufferTotalSize()))
                    .collect(Collectors.toList());
            List<GetMessageResult> getMessageResults = pullMessageResultsWithData
                    .stream()
                    .map(pair -> pair.getObject1().getGetMessageResult())
                    .collect(Collectors.toList());
            BatchManyMessageTransfer batchManyMessageTransfer = new BatchManyMessageTransfer(headerByteBuffers, getMessageResults);
            // this call contains all the sub-request's callbacks to make sure all the reference-counted will be released.
            Runnable callback = () -> {
                // the callback to release ref-count objects.
                pullMessageResultsWithData
                        .stream()
                        .map(pair -> pair.getObject2().getSyncResponse().getCallback())
                        .collect(Collectors.toList())
                        .forEach(Runnable::run);
            };
            response.setAttachment(batchManyMessageTransfer);
            response.setCallback(callback);
            globalPullMessageResult.setSyncResponse(response);
            return globalPullMessageResult;
        }

        List<RemotingCommand> childResponses = null;
        RemotingCommand batchResponse = RemotingCommand.mergeChildResponses(childResponses);

        globalPullMessageResult.setSyncResponse(batchResponse);
        return globalPullMessageResult;
    }

    @Override
    public PullMessageResult handle(StoreReturnedResult storeReturnedResult) {
        final GetMessageResult getMessageResult = storeReturnedResult.getGetMessageResult();
        final RemotingCommand request = storeReturnedResult.getRequest();
        final PullMessageRequestHeader requestHeader = storeReturnedResult.getRequestHeader();
        final SocketAddress clientHost = storeReturnedResult.getClientHost();
        final SubscriptionGroupConfig subscriptionGroupConfig = storeReturnedResult.getSubscriptionGroupConfig();
        final boolean brokerAllowSuspend = storeReturnedResult.isBrokerAllowSuspend();
        final MessageFilter messageFilter = storeReturnedResult.getMessageFilter();
        final RemotingCommand response = storeReturnedResult.getResponse();
        final boolean transferMsgByHeap = storeReturnedResult.isTransferMsgByHeap();

        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();

        PullMessageResult pullMessageResult = new PullMessageResult();

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                        getMessageResult.getMessageCount());

                this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                        getMessageResult.getBufferTotalSize());

                this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());

                if (transferMsgByHeap) {

                    final long beginTimeMills = this.brokerController.getMessageStore().now();
                    final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                    this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId(),
                            (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                    response.setBody(r);
                    pullMessageResult.setSyncResponse(response);
                    return pullMessageResult;
                } else {
                    ManyMessageTransfer manyMessageTransfer =
                            new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                    Runnable callback = getMessageResult::release;
                    response.setAttachment(manyMessageTransfer);
                    response.setCallback(callback);
                    pullMessageResult.setSyncResponse(response);
                    return pullMessageResult;
                }
            case ResponseCode.PULL_NOT_FOUND:
                final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
                final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

                if (brokerAllowSuspend && hasSuspendFlag) {
                    long pollingTimeMills = suspendTimeoutMillisLong;
                    if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                        pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                    }

                    String topic = requestHeader.getTopic();
                    long offset = requestHeader.getQueueOffset();
                    int queueId = requestHeader.getQueueId();
                    CompletableFuture<RemotingCommand> pullFuture = new CompletableFuture<>();
                    PullRequest pullRequest = new PullRequest(request, clientHost, pollingTimeMills,
                            this.brokerController.getMessageStore().now(), offset, messageFilter, pullFuture);
                    this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);

                    pullMessageResult.setAsyncResponse(pullFuture);
                    return pullMessageResult;
                }
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                if (this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
                        || this.brokerController.getMessageStoreConfig().isOffsetCheckInSlave()) {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(requestHeader.getTopic());
                    mq.setQueueId(requestHeader.getQueueId());
                    mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

                    OffsetMovedEvent event = new OffsetMovedEvent();
                    event.setConsumerGroup(requestHeader.getConsumerGroup());
                    event.setMessageQueue(mq);
                    event.setOffsetRequest(requestHeader.getQueueOffset());
                    event.setOffsetNew(getMessageResult.getNextBeginOffset());
                    log.warn(
                            "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}",
                            requestHeader.getTopic(), requestHeader.getConsumerGroup(), event.getOffsetRequest(), event.getOffsetNew(),
                            responseHeader.getSuggestWhichBrokerId());
                } else {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    log.warn("PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}",
                            requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueOffset(),
                            responseHeader.getSuggestWhichBrokerId());
                }

                break;
            default:
                log.warn("[BUG] impossible result code of get message: {}", response.getCode());
                assert false;
        }
        pullMessageResult.setSyncResponse(response);
        return pullMessageResult;
    }

    protected byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                int sysFlag = bb.getInt(MessageDecoder.SYSFLAG_POSITION);
//                bornhost has the IPv4 ip if the MessageSysFlag.BORNHOST_V6_FLAG bit of sysFlag is 0
//                IPv4 host = ip(4 byte) + port(4 byte); IPv6 host = ip(16 byte) + port(4 byte)
                int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                int msgStoreTimePos = 4 // 1 TOTALSIZE
                    + 4 // 2 MAGICCODE
                    + 4 // 3 BODYCRC
                    + 4 // 4 QUEUEID
                    + 4 // 5 FLAG
                    + 8 // 6 QUEUEOFFSET
                    + 8 // 7 PHYSICALOFFSET
                    + 4 // 8 SYSFLAG
                    + 8 // 9 BORNTIMESTAMP
                    + bornhostLength; // 10 BORNHOST
                storeTimestamp = bb.getLong(msgStoreTimePos);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    protected void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG, msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(RemotingUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);

            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }
}
