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
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;

public abstract class MergeBatchResponseStrategy {
    public static final String REMARK_PULL_NOT_FOUND = "pull not found (merge batch response strategy)";
    public static final String REMARK_SYSTEM_ERROR = "system error (merge batch response strategy)";
    public static final String REMARK_SUCCESS = "success (merge batch response strategy)";

    public abstract CompletableFuture<RemotingCommand> merge(
            Integer batchOpaque,
            Map<Integer, CompletableFuture<RemotingCommand>> opaqueToFuture);

    protected RemotingCommand mergeChildren(List<RemotingCommand> responses, int expectedResponseNum, int batchOpaque) {
        Preconditions.checkArgument(responses.size() == expectedResponseNum);

        RemotingCommand sample = responses.get(0);
        boolean zeroCopy = sample.getAttachment() instanceof FileRegion;
        // zero-copy
        if (zeroCopy) {
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
        } else {
            RemotingCommand batchResponse = RemotingCommand.mergeChildren(responses);
            batchResponse.setOpaque(batchOpaque);
            return batchResponse;
        }
    }
}
