package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CommonBatchProcessor extends AsyncNettyRequestProcessor {
    private final SendMessageProcessor sendMessageProcessor;
    private final PullMessageProcessor pullMessageProcessor;
    private final ConsumerManageProcessor consumerManageProcessor;

    public CommonBatchProcessor(SendMessageProcessor sendMessageProcessor, PullMessageProcessor pullMessageProcessor, ConsumerManageProcessor consumerManageProcessor) {
        this.sendMessageProcessor = sendMessageProcessor;
        this.pullMessageProcessor = pullMessageProcessor;
        this.consumerManageProcessor = consumerManageProcessor;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return null;
    }

    public List<RemotingCommand> parseSubRequest(RemotingCommand request) {
        List<RemotingCommand> subRequests = new ArrayList<>();

        return subRequests;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    @Override
    public CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        return pullMessageProcessor.asyncBatchPullMessages(request, ctx.channel().remoteAddress(), true);
    }
}
