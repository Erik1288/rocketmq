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
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.pagecache.BatchManyMessageTransfer;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

public abstract class MergeBatchResponseStrategy {
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

        RemotingCommand sample = responses.get(0);
        boolean zeroCopy = sample.getAttachment() instanceof FileRegion;
        // zero-copy
        if (zeroCopy) {
            return doMergeZeroCopyChildren(responses, batchOpaque);
        } else {
            return doMergeCommonChildren(responses, batchOpaque);
        }
    }

    protected RemotingCommand nonNullableResponse(Integer childOpaque, RemotingCommand childResp) {
        if (childResp == null) {
            // rate limit case.
            return RemotingCommand.createResponse(childOpaque, SYSTEM_ERROR, REMARK_RATE_LIMIT);
        }
        return childResp;
    }

    private RemotingCommand doMergeCommonChildren(List<RemotingCommand> responses, int batchOpaque) throws RemotingCommandException {
        RemotingCommand batchResponse = RemotingCommand.mergeChildren(responses);
        batchResponse.setOpaque(batchOpaque);
        return batchResponse;
    }

    private RemotingCommand doMergeZeroCopyChildren(List<RemotingCommand> responses, int batchOpaque) {
        List<ManyMessageTransfer> manyMessageTransferList = new ArrayList<>();

        int bodyLength = 0;
        for (RemotingCommand resp : responses) {
            if (resp.getAttachment() != null) {
                ManyMessageTransfer manyMessageTransfer = (ManyMessageTransfer) resp.getAttachment();
                manyMessageTransferList.add(manyMessageTransfer);

                bodyLength += manyMessageTransfer.count();
            }
        }

        RemotingCommand zeroCopyResponse = RemotingCommand.createResponse(batchOpaque, SUCCESS, REMARK_SUCCESS);

        ByteBuffer batchHeader = zeroCopyResponse.encodeHeader(bodyLength);
        BatchManyMessageTransfer batchManyMessageTransfer = new BatchManyMessageTransfer(batchHeader, manyMessageTransferList);

        Runnable releaseBatch = batchManyMessageTransfer::close;
        zeroCopyResponse.setAttachment(batchManyMessageTransfer);
        zeroCopyResponse.setFinallyCallback(releaseBatch);
        return zeroCopyResponse;
    }
}
