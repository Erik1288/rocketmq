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
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.rocketmq.common.protocol.ResponseCode.PULL_NOT_FOUND;

public class PullMessageCommonMergeStrategy extends MergeBatchResponseStrategy {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final PullMessageCommonMergeStrategy instance = new PullMessageCommonMergeStrategy();

    private PullMessageCommonMergeStrategy() {
    }

    @Override
    public CompletableFuture<RemotingCommand> merge(
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
                completeUnRespondedResults(undoneResults);
            } else {
                doneResults.put(childOpaque, nonNullableResponse(childOpaque, childResp));
            }

            completeBatchFuture(batchFuture, doneResults, expectedResponseNum, batchOpaque, childOpaque);
        }));

        return batchFuture;
    }

    private RemotingCommand extractResult(Integer batchOpaque, Integer childOpaque, CompletableFuture<RemotingCommand> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("extractResult failed. batch: {}, child: {}.", batchOpaque, childOpaque, e);
            return RemotingCommand.createResponse(childOpaque, PULL_NOT_FOUND, REMARK_PULL_NOT_FOUND);
        }
    }

    private void completeUnRespondedResults(Map<Integer, CompletableFuture<RemotingCommand>> unRespondedResults) {
        if (unRespondedResults.isEmpty()) {
            return ;
        }
        unRespondedResults.forEach(this::completeUnRespondedResult);
    }

    private void completeUnRespondedResult(Integer opaque, CompletableFuture<RemotingCommand> unRespondedResult) {
        // complete it with a PULL_NOT_FOUND value.
        boolean triggered = unRespondedResult.complete(RemotingCommand.createResponse(opaque, PULL_NOT_FOUND, REMARK_PULL_NOT_FOUND));
        if (!triggered) {
            try {
                assert unRespondedResult.isDone();
                RemotingCommand response = unRespondedResult.get();
                if (response != null && response.getFinallyReleasingCallback() != null) {
                    response.getFinallyReleasingCallback().run();
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("completeUnRespondedResult failed.", e);
                throw new RuntimeException("completeUnRespondedResult failed.", e);
            } finally {
                try {
                    RemotingCommand response = unRespondedResult.get();
                    if (response != null && response.getFinallyReleasingCallback() != null) {
                        response.getFinallyReleasingCallback().run();
                    }
                } catch (ExecutionException | InterruptedException e) {
                    log.error("completeUnRespondedResult failed.", e);
                }
            }
        }
    }

    public static PullMessageCommonMergeStrategy getInstance() {
        return instance;
    }
}
