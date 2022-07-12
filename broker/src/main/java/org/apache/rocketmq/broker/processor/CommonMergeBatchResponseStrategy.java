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
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

public class CommonMergeBatchResponseStrategy extends MergeBatchResponseStrategy {

    private CommonMergeBatchResponseStrategy() {
    }

    private static final CommonMergeBatchResponseStrategy commonMergeBatchResponseStrategy = new CommonMergeBatchResponseStrategy();

    @Override
    public CompletableFuture<RemotingCommand> merge(
            Integer batchOpaque,
            Map<Integer, CompletableFuture<RemotingCommand>> opaqueToFuture) {
        Preconditions.checkNotNull(batchOpaque, "batchOpaque shouldn't be null.");
        Preconditions.checkNotNull(opaqueToFuture, "opaqueToFuture shouldn't be null.");

        CompletableFuture<RemotingCommand> batchFuture = new CompletableFuture<>();
        int expectedResponseNum = opaqueToFuture.size();

        ConcurrentHashMap<Integer, RemotingCommand> responses = new ConcurrentHashMap<>(expectedResponseNum);

        opaqueToFuture.forEach((childOpaque, childFuture) -> childFuture.whenComplete((childResp, throwable) -> {
            if (throwable != null) {
                responses.put(childOpaque, RemotingCommand.createResponse(childOpaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
            } else {
                // TODO childResp may be null.
                responses.put(childOpaque, childResp);
            }
            completeBatchFuture(batchFuture, responses, expectedResponseNum, batchOpaque);
        }));

        return batchFuture;
    }

    private void completeBatchFuture(
            CompletableFuture<RemotingCommand> batchFuture,
            ConcurrentMap<Integer, RemotingCommand> responses,
            int expectedResponseNum,
            int batchOpaque) {
        if (responses.size() != expectedResponseNum) {
            return ;
        }

        batchFuture.complete(mergeChildren(new ArrayList<>(responses.values()), expectedResponseNum, batchOpaque));
    }

    public static CommonMergeBatchResponseStrategy getInstance() {
        return commonMergeBatchResponseStrategy;
    }

}
