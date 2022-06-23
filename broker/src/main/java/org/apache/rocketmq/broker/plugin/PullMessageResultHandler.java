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

package org.apache.rocketmq.broker.plugin;

import org.apache.rocketmq.broker.processor.PullMessageResult;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageFilter;

import java.net.SocketAddress;

public interface PullMessageResultHandler {

    /**
     * Handle result of get message from store.
     *
     * @param getMessageResult store result
     * @param request request
     * @param requestHeader request header
     * @param clientHost
     * @param subscriptionData sub data
     * @param subscriptionGroupConfig sub config
     * @param brokerAllowSuspend brokerAllowSuspend
     * @param messageFilter store message filter
     * @param response response
     * @return new pull message result
     */
    PullMessageResult handle(final GetMessageResult getMessageResult,
                             final RemotingCommand request,
                             final PullMessageRequestHeader requestHeader,
                             final SocketAddress clientHost,
                             final SubscriptionData subscriptionData,
                             final SubscriptionGroupConfig subscriptionGroupConfig,
                             final boolean brokerAllowSuspend,
                             final MessageFilter messageFilter,
                             final RemotingCommand response);
}
