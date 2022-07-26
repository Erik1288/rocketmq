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

import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.rocketmq.broker.processor.MergeBatchResponseStrategy.REMARK_SYSTEM_ERROR;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

@RunWith(MockitoJUnitRunner.class)
public class CommonMergeBatchResponseStrategyTest {
    private CommonMergeBatchResponseStrategy strategy;
    private Random random = new Random();

    @Before
    public void init() {
        strategy = CommonMergeBatchResponseStrategy.getInstance();
    }

    @Test
    public void testCommonStrategy() throws ExecutionException, InterruptedException, RemotingCommandException {
        int batchOpaque = 9999;
        int totalResponseNum = 20;
        AtomicInteger increment = new AtomicInteger(0);

        Map<Integer, CompletableFuture<RemotingCommand>> allResults = new HashMap<>();

        Map<Integer, CompletableFuture<RemotingCommand>> doneResults = new HashMap<>();
        for (int i = 0; i < totalResponseNum; i++) {
            Integer opaque = increment.getAndIncrement();
            CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            future.complete(RemotingCommand.createResponse(opaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
            doneResults.put(opaque, future);
        }

        Map<Integer, CompletableFuture<RemotingCommand>> undoneResults = new HashMap<>();
        for (int i = 0; i < totalResponseNum; i++) {
            Integer opaque = increment.getAndIncrement();
            CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            undoneResults.put(opaque, future);
        }

        allResults.putAll(doneResults);
        allResults.putAll(undoneResults);

        CompletableFuture<RemotingCommand> batchFuture = null;
        try {
            batchFuture = strategy.mergeResponses(batchOpaque, allResults);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertFalse(batchFuture.isDone());

        for (Map.Entry<Integer, CompletableFuture<RemotingCommand>> entry : undoneResults.entrySet()) {
            Integer opaque = entry.getKey();
            CompletableFuture<RemotingCommand> future = entry.getValue();
            Assert.assertFalse(future.isDone());
            Assert.assertFalse(batchFuture.isDone());
            future.complete(RemotingCommand.createResponse(opaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
        }

        // once all child futures are complete, batch is complete.
        Assert.assertTrue(batchFuture.isDone());

        RemotingCommand actualBatchResponse = batchFuture.get();
        List<RemotingCommand> actualChildResponses = RemotingCommand.parseChildren(actualBatchResponse);
        Assert.assertEquals(totalResponseNum * 2, actualChildResponses.size());

        Map<Integer, RemotingCommand> actualChildResponseMap = actualChildResponses
                .stream()
                .collect(Collectors.toMap(RemotingCommand::getOpaque, x -> x));
        for (Map.Entry<Integer, CompletableFuture<RemotingCommand>> entry : doneResults.entrySet()) {
            Integer opaque = entry.getKey();
            CompletableFuture<RemotingCommand> expectedResponseFuture = entry.getValue();
            RemotingCommand expectedResponse = expectedResponseFuture.get();
            RemotingCommand actualResponse = actualChildResponseMap.get(opaque);
            Assert.assertEquals(expectedResponse.getCode(), actualResponse.getCode());
            Assert.assertEquals(expectedResponse.getBody(), actualResponse.getBody());
            Assert.assertEquals(expectedResponse.getCode(), actualResponse.getCode());
        }
    }
}
