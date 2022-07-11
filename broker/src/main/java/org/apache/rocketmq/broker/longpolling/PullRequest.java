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
package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.broker.client.RemoteAddressSupplier;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageFilter;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class PullRequest {
    private final RemotingCommand requestCommand;
    private final long timeoutMillis;
    private final long suspendTimestamp;
    private final long pullFromThisOffset;
    private final SubscriptionData subscriptionData;
    private final MessageFilter messageFilter;
    private final CompletableFuture<RemotingCommand> future;
    private final RemoteAddressSupplier remoteAddressSupplier;
    private final Consumer<RemotingCommand> fastFailCallback;

    public PullRequest(RemotingCommand requestCommand, long timeoutMillis, long suspendTimestamp,
                       long pullFromThisOffset, SubscriptionData subscriptionData,
                       MessageFilter messageFilter, CompletableFuture<RemotingCommand> future,
                       RemoteAddressSupplier remoteAddressSupplier, Consumer<RemotingCommand> fastFailCallback) {
        this.requestCommand = requestCommand;
        this.timeoutMillis = timeoutMillis;
        this.suspendTimestamp = suspendTimestamp;
        this.pullFromThisOffset = pullFromThisOffset;
        this.subscriptionData = subscriptionData;
        this.messageFilter = messageFilter;
        this.future = future;
        this.remoteAddressSupplier = remoteAddressSupplier;
        this.fastFailCallback = fastFailCallback;
    }

    public RemotingCommand getRequestCommand() {
        return requestCommand;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getSuspendTimestamp() {
        return suspendTimestamp;
    }

    public long getPullFromThisOffset() {
        return pullFromThisOffset;
    }

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }

    public CompletableFuture<RemotingCommand> getFuture() {
        return future;
    }

    public RemoteAddressSupplier getRemoteAddressSupplier() {
        return remoteAddressSupplier;
    }

    public Consumer<RemotingCommand> getFastFailCallback() {
        return fastFailCallback;
    }
}
