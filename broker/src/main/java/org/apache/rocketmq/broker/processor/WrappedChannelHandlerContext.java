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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.TriConsumer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.net.SocketAddress;

public class WrappedChannelHandlerContext {
    // Please DO NOT use ctx in processor directly!
    private final ChannelHandlerContext ctx;

    public WrappedChannelHandlerContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public TriConsumer<Integer, Integer, String> getFastFailCallback() {
        return (opaque, code, remark) -> {
            final RemotingCommand response = RemotingCommand.createResponseCommand(code, remark);
            response.setOpaque(opaque);
            WrappedChannelHandlerContext.this.ctx.channel().writeAndFlush(response);
        };
    }

    public SocketAddress remoteAddress() {
        return this.ctx.channel().remoteAddress();
    }

    public String channelRemoteAddr() {
        return RemotingHelper.parseChannelRemoteAddr(this.ctx.channel());
    }

}
