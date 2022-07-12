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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.netty.FileRegionEncoder;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BatchProtocolTestPullTest extends BatchProtocolTest {
    private ClientChannelInfo clientChannelInfo;
    private BrokerConfig brokerConfig;
    @Before
    public void init() throws Exception {
        this.brokerConfig = new BrokerConfig();
        this.brokerController = new BrokerController(
                this.brokerConfig,
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();

        Channel mockChannel = mock(Channel.class);
        when(mockChannel.isWritable()).thenReturn(true);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        when(ctx.channel()).thenReturn(mockChannel);
        when(ctx.channel().isWritable()).thenReturn(true);
        clientChannelInfo = new ClientChannelInfo(mockChannel);
    }

    @After
    public void after() {
        brokerController.getMessageStore().destroy();
    }

    @Test
    public void testPullBatchProtocol() throws Exception {
        CommonBatchProcessor commonBatchProcessor = brokerController.getCommonBatchProcessor();
        int totalRequestNum = 20;
        String subscriptionGroup = "subscription-group";
        String producerGroup = "producer-group";
        String topicPrefix = "batch-protocol";
        Integer queue = 0;

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(subscriptionGroup);

        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        SubscriptionGroupManager subscriptionGroupManager = brokerController.getSubscriptionGroupManager();
        subscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);

        Map<Integer, RemotingCommand> expectedRequests = new HashMap<>();

        Long offset = 0L;
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < totalRequestNum; i++) {
            String topic = topicPrefix + "-" + i;

            topicConfigManager.getTopicConfigTable().put(topic, new TopicConfig(topic));
            RemotingCommand childPullRequest = createPullRequest(subscriptionGroup, topic, queue, offset);
            expectedRequests.put(childPullRequest.getOpaque(), childPullRequest);

            topics.add(topic);

            RemotingCommand sendRequest = createSendRequest(producerGroup, topic, queue);
            RemotingCommand sendResponse = brokerController.getSendProcessor().processRequest(ctx, sendRequest);
            assertThat(sendResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }

        ConsumerData consumerData = createConsumerData(subscriptionGroup, topics);
        brokerController.getConsumerManager().registerConsumer(
                consumerData.getGroupName(),
                this.clientChannelInfo,
                consumerData.getConsumeType(),
                consumerData.getMessageModel(),
                consumerData.getConsumeFromWhere(),
                consumerData.getSubscriptionDataSet(),
                false);

        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(expectedRequests.values()));
        batchRequest.setRemark(CommonBatchProcessor.DISPATCH_PULL);

        // turn [zero-copy] off
        this.brokerConfig.setTransferMsgByHeap(true);
        CompletableFuture<RemotingCommand> batchFuture = commonBatchProcessor.asyncProcessRequest(ctx, batchRequest, callback);

        RemotingCommand batchResponse = batchFuture.get();
        List<RemotingCommand> childResponses = RemotingCommand.parseChildren(batchResponse);
        Assert.assertEquals(totalRequestNum, childResponses.size());

        for (RemotingCommand actualChildResponse : childResponses) {
            int opaque = actualChildResponse.getOpaque();
            Assert.assertTrue(expectedRequests.containsKey(opaque));
            Assert.assertNotNull(actualChildResponse.getBody());

            PullMessageResponseHeader responseHeader =
                    (PullMessageResponseHeader) actualChildResponse.decodeCommandCustomHeader(PullMessageResponseHeader.class);

            ByteBuffer byteBuffer = ByteBuffer.wrap(actualChildResponse.getBody());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
        }
    }

    @Test
    public void testPullZeroCopyBatchProtocol() throws Exception {
        CommonBatchProcessor commonBatchProcessor = brokerController.getCommonBatchProcessor();
        int totalRequestNum = 20;
        String subscriptionGroup = "subscription-group";
        String producerGroup = "producer-group";
        String topicPrefix = "batch-protocol";
        Integer queue = 0;

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(subscriptionGroup);

        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        SubscriptionGroupManager subscriptionGroupManager = brokerController.getSubscriptionGroupManager();
        subscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);

        Map<Integer, RemotingCommand> expectedRequests = new HashMap<>();

        Long offset = 0L;
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < totalRequestNum; i++) {
            String topic = topicPrefix + "-" + i;

            topicConfigManager.getTopicConfigTable().put(topic, new TopicConfig(topic));
            RemotingCommand childPullRequest = createPullRequest(subscriptionGroup, topic, queue, offset);
            expectedRequests.put(childPullRequest.getOpaque(), childPullRequest);

            topics.add(topic);

            RemotingCommand sendRequest = createSendRequest(producerGroup, topic, queue);
            RemotingCommand sendResponse = brokerController.getSendProcessor().processRequest(ctx, sendRequest);
            assertThat(sendResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }

        ConsumerData consumerData = createConsumerData(subscriptionGroup, topics);
        brokerController.getConsumerManager().registerConsumer(
                consumerData.getGroupName(),
                this.clientChannelInfo,
                consumerData.getConsumeType(),
                consumerData.getMessageModel(),
                consumerData.getConsumeFromWhere(),
                consumerData.getSubscriptionDataSet(),
                false);

        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(expectedRequests.values()));
        batchRequest.setRemark(CommonBatchProcessor.DISPATCH_PULL);

        // turn [zero-copy] on
        this.brokerConfig.setTransferMsgByHeap(false);

        // issue an batch-send-message request.
        CompletableFuture<RemotingCommand> batchFuture = commonBatchProcessor.asyncProcessRequest(ctx, batchRequest, callback);

        assertThat(batchFuture.isDone()).isTrue();
        RemotingCommand batchResponse = batchFuture.get();
        assertThat(batchResponse.getAttachment()).isNotNull();

        FileRegion fileRegion = (FileRegion) batchResponse.getAttachment();

        FileRegionEncoder fileRegionEncoder = new FileRegionEncoder();
        ByteBuf batchResponseBuf = Unpooled.buffer((int) fileRegion.count());
        fileRegionEncoder.encode(null, fileRegion, batchResponseBuf);

        // strip 4 bytes to simulate NettyDecoder.
        batchResponseBuf.readerIndex(4);
        RemotingCommand decodeBatchResponse = RemotingCommand.decode(batchResponseBuf);

        List<RemotingCommand> childResponses = RemotingCommand.parseChildren(decodeBatchResponse);
        Assert.assertEquals(totalRequestNum, childResponses.size());

        for (RemotingCommand actualChildResponse : childResponses) {
            int opaque = actualChildResponse.getOpaque();
            Assert.assertTrue(expectedRequests.containsKey(opaque));
            Assert.assertNotNull(actualChildResponse.getBody());

            PullMessageResponseHeader responseHeader =
                    (PullMessageResponseHeader) actualChildResponse.decodeCommandCustomHeader(PullMessageResponseHeader.class);

            ByteBuffer byteBuffer = ByteBuffer.wrap(actualChildResponse.getBody());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
        }
    }

    @Test
    public void testPartialPullLongPollingBatchProtocol() throws Exception {
        CommonBatchProcessor commonBatchProcessor = brokerController.getCommonBatchProcessor();
        int totalRequestNum = 20;
        String subscriptionGroup = "subscription-group";
        String producerGroup = "producer-group";
        String topicPrefix = "batch-protocol";
        Integer queue = 0;

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(subscriptionGroup);

        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        SubscriptionGroupManager subscriptionGroupManager = brokerController.getSubscriptionGroupManager();
        subscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);

        Map<Integer, RemotingCommand> expectedRequests = new HashMap<>();

        Integer longPollingOpaque = null;

        Long offset = 0L;
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < totalRequestNum; i++) {
            String topic = topicPrefix + "-" + i;

            topicConfigManager.getTopicConfigTable().put(topic, new TopicConfig(topic));
            RemotingCommand childPullRequest = createPullRequest(subscriptionGroup, topic, queue, offset);
            expectedRequests.put(childPullRequest.getOpaque(), childPullRequest);

            topics.add(topic);

            if (i == 4) {
                longPollingOpaque = childPullRequest.getOpaque();
                continue;
            }
            // sending message to topic
            RemotingCommand sendRequest = createSendRequest(producerGroup, topic, queue);
            RemotingCommand sendResponse = brokerController.getSendProcessor().processRequest(ctx, sendRequest);
            assertThat(sendResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }

        ConsumerData consumerData = createConsumerData(subscriptionGroup, topics);
        brokerController.getConsumerManager().registerConsumer(
                consumerData.getGroupName(),
                this.clientChannelInfo,
                consumerData.getConsumeType(),
                consumerData.getMessageModel(),
                consumerData.getConsumeFromWhere(),
                consumerData.getSubscriptionDataSet(),
                false);

        RemotingCommand batchRequest = RemotingCommand.mergeChildren(new ArrayList<>(expectedRequests.values()));
        batchRequest.setRemark(CommonBatchProcessor.DISPATCH_PULL);

        // turn [zero-copy] off
        this.brokerConfig.setTransferMsgByHeap(true);

        // issue an batch-send-message request.
        CompletableFuture<RemotingCommand> batchFuture = commonBatchProcessor.asyncProcessRequest(ctx, batchRequest, callback);

        assertThat(batchFuture.isDone()).isTrue();
        RemotingCommand batchResponse = batchFuture.get();

        List<RemotingCommand> childResponses = RemotingCommand.parseChildren(batchResponse);
        Assert.assertEquals(totalRequestNum, childResponses.size());

        for (RemotingCommand actualChildResponse : childResponses) {
            int opaque = actualChildResponse.getOpaque();
            Assert.assertTrue(expectedRequests.containsKey(opaque));

            if (Objects.equals(longPollingOpaque, opaque)) {
                assertThat(actualChildResponse.getCode()).isEqualTo(ResponseCode.PULL_NOT_FOUND);
            } else {
                assertThat(actualChildResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
            }
        }
    }

    static ConsumerData createConsumerData(String group, List<String> topics) {
        ConsumerData consumerData = new ConsumerData();
        consumerData.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumerData.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerData.setGroupName(group);
        consumerData.setMessageModel(MessageModel.CLUSTERING);
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        for (String topic : topics) {
            SubscriptionData subscriptionData = new SubscriptionData();
            subscriptionData.setTopic(topic);
            subscriptionData.setSubString("*");
            subscriptionData.setSubVersion(100L);
            subscriptionDataSet.add(subscriptionData);
        }

        consumerData.setSubscriptionDataSet(subscriptionDataSet);
        return consumerData;
    }
}
