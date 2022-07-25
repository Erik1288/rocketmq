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

import com.google.common.base.Preconditions;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

public abstract class MergeBatchResponseStrategy {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final String SUFFIX = " (merge batch response strategy)";
    public static final String REMARK_PULL_NOT_FOUND = "pull not found" + SUFFIX;
    public static final String REMARK_SYSTEM_ERROR = "system error" + SUFFIX;
    public static final String REMARK_RATE_LIMIT = "rate limit" + SUFFIX;
    public static final String REMARK_SUCCESS = "success" + SUFFIX;

    /**
     * Merge the responses into a batch-response.
     * @param batchOpaque the opaque that the batch-response to be returned should have.
     * @param opaqueToFuture responses
     * @return batch future
     */
    public abstract CompletableFuture<RemotingCommand> merge(
            Integer batchOpaque,
            Map<Integer, CompletableFuture<RemotingCommand>> opaqueToFuture) throws Exception;

    protected RemotingCommand mergeChildren(List<RemotingCommand> responses, int expectedResponseNum, int batchOpaque) throws RemotingCommandException {
        Preconditions.checkArgument(!responses.isEmpty());
        Preconditions.checkArgument(responses.size() == expectedResponseNum);

        RemotingCommand batchResponse = RemotingCommand.mergeChildren(responses);
        batchResponse.setOpaque(batchOpaque);
        batchResponse.setCode(SUCCESS);
        batchResponse.setRemark(REMARK_SUCCESS);
        return batchResponse;
    }

    protected RemotingCommand nonNullableResponse(Integer childOpaque, RemotingCommand childResp) {
        if (childResp == null) {
            // rate limit case.
            return RemotingCommand.createResponse(childOpaque, SYSTEM_ERROR, REMARK_RATE_LIMIT);
        }
        return childResp;
    }

    protected void completeBatchFuture(
            CompletableFuture<RemotingCommand> batchFuture,
            ConcurrentMap<Integer, RemotingCommand> responses,
            int expectedResponseNum,
            int batchOpaque,
            int childOpaque) {
        if (responses.size() != expectedResponseNum) {
            return ;
        }

        if (batchFuture.isDone()) {
            return ;
        }

        try {
            batchFuture.complete(mergeChildren(new ArrayList<>(responses.values()), expectedResponseNum, batchOpaque));
        } catch (Exception e) {
            log.error("completeBatchFuture failed. batch: {}, child: {}.", batchOpaque, childOpaque, e);
            batchFuture.complete(RemotingCommand.createResponse(batchOpaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
        }
    }

}
