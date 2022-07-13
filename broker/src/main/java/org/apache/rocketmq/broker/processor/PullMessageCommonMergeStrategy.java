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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.rocketmq.common.protocol.ResponseCode.PULL_NOT_FOUND;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

public class PullMessageCommonMergeStrategy extends MergeBatchResponseStrategy {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final PullMessageCommonMergeStrategy instance = new PullMessageCommonMergeStrategy();

    private PullMessageCommonMergeStrategy() {
    }

    @Override
    public CompletableFuture<RemotingCommand> merge(
            Integer batchOpaque,
            Map<Integer, CompletableFuture<RemotingCommand>> opaqueToFuture) throws Exception {

        Preconditions.checkNotNull(batchOpaque, "batchOpaque shouldn't be null.");
        Preconditions.checkNotNull(opaqueToFuture, "opaqueToFuture shouldn't be null.");
        Preconditions.checkArgument(!opaqueToFuture.isEmpty());

        CompletableFuture<RemotingCommand> batchFuture = new CompletableFuture<>();

        Map<Integer /* opaque */, CompletableFuture<RemotingCommand>> doneResults = new HashMap<>();
        Map<Integer /* opaque */, CompletableFuture<RemotingCommand>> undoneResults = new HashMap<>();

        opaqueToFuture.forEach((childOpaque, childFuture) -> {
            if (childFuture.isDone()) {
                doneResults.put(childOpaque, childFuture);
            } else {
                undoneResults.put(childOpaque, childFuture);
            }
        });

        final int expectedResponseNum = opaqueToFuture.size();

        // case 1: all results are done, just merge results then respond to client.
        if (doneResults.size() == expectedResponseNum && undoneResults.isEmpty()) {
            completeWhileAllDone(batchOpaque, doneResults, undoneResults, batchFuture, expectedResponseNum);
            return batchFuture;
        }

        // case 2: some results are done, but others are not. complete undone futures, merge results then respond to client.
        if (!doneResults.isEmpty()) {
            completeBatchWhilePartialDone(batchOpaque, doneResults, undoneResults, batchFuture, expectedResponseNum);
            return batchFuture;
        }

        // case 3: none of them is done. waiting until at least one future is done, completes undone futures, then respond to client.
        completeBatchWhileNoneIsDone(batchOpaque, undoneResults, batchFuture, expectedResponseNum);

        return batchFuture;
    }

    private void completeBatchWhileNoneIsDone(Integer batchOpaque, Map<Integer, CompletableFuture<RemotingCommand>> undoneResults, CompletableFuture<RemotingCommand> batchFuture, int expectedResponseNum) {
        AtomicBoolean completeBatch = new AtomicBoolean(false);

        undoneResults.forEach((childOpaque, childFuture) -> childFuture.whenComplete((childResp, throwable) -> {
            if (completeBatch.compareAndSet(false, true)) {
                // only the first completed future is able to be here.
                List<RemotingCommand> all = new ArrayList<>();
                // add complete response.
                RemotingCommand doneResponse;
                if (throwable != null) {
                    log.error("Something is wrong with pull-merging. batch: {}, child: {}.", batchOpaque, childOpaque, throwable);
                    doneResponse = RemotingCommand.createResponse(childOpaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR);
                } else {
                    doneResponse = nonNullableResponse(childOpaque, childResp);
                }
                all.add(doneResponse);
                // create new responses with PULL_NOT_FOUND coded for long-polling requests.
                Map<Integer, CompletableFuture<RemotingCommand>> stillUndoneResults = undoneResults
                    .entrySet()
                    .stream()
                    .filter(entry -> !Objects.equals(entry.getKey(), childResp.getOpaque()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                // add incomplete responses.
                all.addAll(stillUndoneResults
                    .keySet()
                    .stream()
                    .map(otherOpaque -> RemotingCommand.createResponse(otherOpaque, PULL_NOT_FOUND, REMARK_PULL_NOT_FOUND))
                    .collect(Collectors.toList()));

                try {
                    batchFuture.complete(mergeChildren(all, expectedResponseNum, batchOpaque));
                    completeUnRespondedResults(stillUndoneResults);
                } catch (Exception e) {
                    log.error("completeBatchFuture failed. batch: {}, child: {}.", batchOpaque, childOpaque, e);
                    batchFuture.complete(RemotingCommand.createResponse(batchOpaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
                }
            }
        }));
    }

    private void completeBatchWhilePartialDone(
            Integer batchOpaque,
            Map<Integer, CompletableFuture<RemotingCommand>> doneResults,
            Map<Integer, CompletableFuture<RemotingCommand>> undoneResults,
            CompletableFuture<RemotingCommand> batchFuture,
            int expectedResponseNum) {
        try {
            // done results: respond to client.
            List<RemotingCommand> responses = collectResponses(doneResults, undoneResults);
            batchFuture.complete(mergeChildren(responses, expectedResponseNum, batchOpaque));
            // undone result: will be discarded.
            completeUnRespondedResults(undoneResults);
        } catch (Exception e) {
            log.error("completeBatchFuture failed. batch: {}.", batchOpaque, e);
            batchFuture.complete(RemotingCommand.createResponse(batchOpaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
        }
    }

    private void completeWhileAllDone(
            Integer batchOpaque,
            Map<Integer, CompletableFuture<RemotingCommand>> doneResults,
            Map<Integer, CompletableFuture<RemotingCommand>> undoneResults,
            CompletableFuture<RemotingCommand> batchFuture,
            int expectedResponseNum) {
        try {
            List<RemotingCommand> responses = collectResponses(doneResults, undoneResults);
            batchFuture.complete(mergeChildren(responses, expectedResponseNum, batchOpaque));
        } catch (Exception e) {
            log.error("completeBatchFuture failed. batch: {}.", batchOpaque, e);
            batchFuture.complete(RemotingCommand.createResponse(batchOpaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
        }
    }

    private void completeUnRespondedResults(Map<Integer, CompletableFuture<RemotingCommand>> unRespondedResults) {
        unRespondedResults.forEach((opaque, future) -> completeUnRespondedResult(future));
    }

    private void completeUnRespondedResult(CompletableFuture<RemotingCommand> unRespondedResult) {
        // complete it with a dummy value.
        boolean triggered = unRespondedResult.complete(null);
        if (!triggered) {
            try {
                RemotingCommand response = unRespondedResult.get();
                if (response != null && response.getFinallyCallback() != null) {
                    response.getFinallyCallback().run();
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("completeUnRespondedResult failed.", e);
            }
        }
    }

    public static PullMessageCommonMergeStrategy getInstance() {
        return instance;
    }

    private List<RemotingCommand> collectResponses(
            Map<Integer, CompletableFuture<RemotingCommand>> doneResults,
            Map<Integer, CompletableFuture<RemotingCommand>> undoneResults) {

        List<RemotingCommand> responseChildren = new ArrayList<>();

        // the DONE part
        for (Map.Entry<Integer, CompletableFuture<RemotingCommand>> entry : doneResults.entrySet()) {
            CompletableFuture<RemotingCommand> future = entry.getValue();
            if (!future.isDone()) {
                log.error("Expecting the future is done.");
                throw new RuntimeException("Expecting the future is done. Opaque:" + entry.getKey());
            }
            try {
                responseChildren.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                // will never happen.
                log.error("collectResponses failed.", e);
                throw new RuntimeException(e.getMessage());
            }
        }

        // the UNDONE part
        for (Map.Entry<Integer, CompletableFuture<RemotingCommand>> entry : undoneResults.entrySet()) {
            Integer opaque = entry.getKey();
            // create a new response with PULL_NOT_FOUND coded for long-polling request.
            responseChildren.add(RemotingCommand.createResponse(opaque, PULL_NOT_FOUND, REMARK_PULL_NOT_FOUND));
        }

        return responseChildren;
    }
}
