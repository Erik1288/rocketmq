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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.rocketmq.common.protocol.ResponseCode.PULL_NOT_FOUND;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

public class PullMessageCommonMergeStrategy extends MergeBatchResponseStrategy {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final PullMessageCommonMergeStrategy instance = new PullMessageCommonMergeStrategy();

    private PullMessageCommonMergeStrategy() {
    }

    @Override
    public CompletableFuture<RemotingCommand> mergeResponses(
            Integer batchOpaque,
            Map<Integer, CompletableFuture<RemotingCommand>> opaqueToFuture) {

        Preconditions.checkNotNull(batchOpaque, "batchOpaque shouldn't be null.");
        Preconditions.checkNotNull(opaqueToFuture, "opaqueToFuture shouldn't be null.");
        Preconditions.checkArgument(!opaqueToFuture.isEmpty());

        final CompletableFuture<RemotingCommand> batchFuture = new CompletableFuture<>();

        final int expectedResponseNum = opaqueToFuture.size();

        AtomicBoolean completeStat = new AtomicBoolean(false);
        ConcurrentHashMap<Integer, RemotingCommand> doneResults = new ConcurrentHashMap<>(expectedResponseNum);
        ConcurrentHashMap<Integer, CompletableFuture<RemotingCommand>> undoneResults = new ConcurrentHashMap<>();

        opaqueToFuture.forEach((childOpaque, childFuture) -> childFuture.whenComplete((childResp, throwable) -> {
            if (completeStat.compareAndSet(false, true)) {
                opaqueToFuture.forEach((cOpaque, cFuture) -> {
                    if (cFuture.isDone()) {
                        doneResults.put(cOpaque, nonNullableResponse(cOpaque, extractResult(batchOpaque, cOpaque, cFuture)));
                    } else {
                        undoneResults.put(cOpaque, cFuture);
                    }
                });
                completeUndoneResults(undoneResults);
            } else {
                doneResults.put(childOpaque, nonNullableResponse(childOpaque, childResp));
            }

            completeBatchFuture(batchFuture, doneResults, expectedResponseNum, batchOpaque, childOpaque);
        }));

        return batchFuture;
    }

    @Override
    protected RemotingCommand mergeChildren(List<RemotingCommand> responses, int expectedResponseNum, int batchOpaque) throws RemotingCommandException {
        if (zeroCopy(responses)) {
            return this.mergeZeroCopyChildren(responses, batchOpaque);
        } else {
            return super.mergeChildren(responses, expectedResponseNum, batchOpaque);
        }
    }

    private boolean zeroCopy(List<RemotingCommand> responses) {
        List<RemotingCommand> withAttachment = new ArrayList<>();
        List<RemotingCommand> withoutAttachment = new ArrayList<>();
        List<RemotingCommand> withoutAttachmentHavingNoData = new ArrayList<>();

        for (RemotingCommand response : responses) {
            if (response.getAttachment() instanceof FileRegion) {
                withAttachment.add(response);
            } else {
                withoutAttachment.add(response);
                if (response.getCode() == PULL_NOT_FOUND) {
                    withoutAttachmentHavingNoData.add(response);
                }
            }
        }

        if (withoutAttachment.size() == responses.size()) {
            return false;
        }

        if (withAttachment.size() == responses.size()) {
            return true;
        }

        // zero copy + partial long-polling
        if (!withAttachment.isEmpty() && withoutAttachmentHavingNoData.size() == withoutAttachment.size()) {
            return true;
        } else {
            withAttachment.forEach(childResp -> {
                if (childResp.getFinallyReleasingCallback() != null) {
                    childResp.getFinallyReleasingCallback().run();
                }
            });
            throw new RuntimeException("inconsistent config: transfer by heap.");
        }
    }

    private RemotingCommand extractResult(Integer batchOpaque, Integer childOpaque, CompletableFuture<RemotingCommand> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("extractResult failed. batch: {}, child: {}.", batchOpaque, childOpaque, e);
            return RemotingCommand.createResponse(childOpaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR);
        }
    }

    private void completeUndoneResults(Map<Integer, CompletableFuture<RemotingCommand>> undoneResults) {
        if (undoneResults.isEmpty()) {
            return ;
        }
        undoneResults.forEach(this::completeUndoneResult);
    }

    private void completeUndoneResult(Integer opaque, CompletableFuture<RemotingCommand> undoneResult) {
        // complete it with a PULL_NOT_FOUND value.
        boolean triggered = undoneResult.complete(RemotingCommand.createResponse(opaque, PULL_NOT_FOUND, REMARK_PULL_NOT_FOUND));
        if (!triggered) {
            try {
                assert undoneResult.isDone();
                RemotingCommand response = undoneResult.get();
                if (response != null && response.getFinallyReleasingCallback() != null) {
                    response.getFinallyReleasingCallback().run();
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("completeUnRespondedResult failed.", e);
                throw new RuntimeException("completeUndoneResult failed.", e);
            } finally {
                try {
                    RemotingCommand response = undoneResult.get();
                    if (response != null && response.getFinallyReleasingCallback() != null) {
                        response.getFinallyReleasingCallback().run();
                    }
                } catch (ExecutionException | InterruptedException e) {
                    log.error("completeUndoneResult failed.", e);
                }
            }
        }
    }

    private RemotingCommand mergeZeroCopyChildren(List<RemotingCommand> responses, int batchOpaque) {
        List<ManyMessageTransfer> manyMessageTransferList = new ArrayList<>();

        int bodyLength = 0;
        for (RemotingCommand resp : responses) {
            if (resp.getAttachment() == null) {
                // responses whose attachment is null are allowed to be omitted from broker
                continue;
            }
            ManyMessageTransfer manyMessageTransfer = (ManyMessageTransfer) resp.getAttachment();
            manyMessageTransferList.add(manyMessageTransfer);

            bodyLength += manyMessageTransfer.count();
        }
        RemotingCommand zeroCopyResponse = RemotingCommand.createResponse(batchOpaque, SUCCESS, REMARK_SUCCESS);

        ByteBuffer batchHeader = zeroCopyResponse.encodeHeader(bodyLength);
        BatchManyMessageTransfer batchManyMessageTransfer = new BatchManyMessageTransfer(batchHeader, manyMessageTransferList);

        Runnable releaseBatch = batchManyMessageTransfer::close;
        zeroCopyResponse.setAttachment(batchManyMessageTransfer);
        zeroCopyResponse.setFinallyReleasingCallback(releaseBatch);
        return zeroCopyResponse;
    }

    public static PullMessageCommonMergeStrategy getInstance() {
        return instance;
    }
}
