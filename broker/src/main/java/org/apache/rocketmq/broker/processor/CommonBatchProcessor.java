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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.CommonBatchRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class CommonBatchProcessor extends AsyncNettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public CommonBatchProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        throw new RuntimeException("not supported.");
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    @Override
    public CompletableFuture<RemotingCommand> asyncProcessRequest(
            ChannelHandlerContext ctx,
            RemotingCommand request,
            RemotingResponseCallback responseCallback) throws Exception {
        log.debug("receive common-batch request command, {}", request);

        AsyncNettyRequestProcessor asyncNettyRequestProcessor = dispatchProcessor(request);
        List<RemotingCommand> requestChildren = RemotingCommand.parseChildren(request);

        Map<Integer /* opaque */, CompletableFuture<RemotingCommand>> opaqueToFuture = new HashMap<>();

        for (RemotingCommand childRequest : requestChildren) {
            CompletableFuture<RemotingCommand> childFuture = asyncNettyRequestProcessor.asyncProcessRequest(ctx, childRequest, responseCallback);
            int opaque = childRequest.getOpaque();
            opaqueToFuture.put(opaque, childFuture);
        }

        MergeBatchResponseStrategy strategy = selectStrategy(asyncNettyRequestProcessor);
        return strategy.merge(request.getOpaque(), opaqueToFuture);
    }

    private MergeBatchResponseStrategy selectStrategy(AsyncNettyRequestProcessor asyncNettyRequestProcessor) {
        if (asyncNettyRequestProcessor instanceof PullMessageProcessor) {
            return PullMessageCommonMergeStrategy.getInstance();
        } else {
            return CommonMergeBatchResponseStrategy.getInstance();
        }
    }

    private AsyncNettyRequestProcessor dispatchProcessor(RemotingCommand batchRequest) throws RemotingCommandException {
        final CommonBatchRequestHeader requestHeader =
                (CommonBatchRequestHeader) batchRequest.decodeCommandCustomHeader(CommonBatchRequestHeader.class);

        int dispatchCode = requestHeader.getCode();

        Pair<NettyRequestProcessor, ExecutorService> processorPair = this.brokerController.getRemotingServer().getProcessorPair(dispatchCode);
        return (AsyncNettyRequestProcessor) processorPair.getObject1();
    }
}
