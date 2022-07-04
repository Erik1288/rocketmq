package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CommonBatchProcessor extends AsyncNettyRequestProcessor {

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    @Override
    public CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        CommonBatchProcessor commonBatchProcessor = dispatchToProcessor();
        List<RemotingCommand> requestChildren = RemotingCommand.parseChildren(request);

        List<CompletableFuture<RemotingCommand>> doneResults = new ArrayList<>();
        List<CompletableFuture<RemotingCommand>> undoneResults = new ArrayList<>();

        for (RemotingCommand childRequest : requestChildren) {
            CompletableFuture<RemotingCommand> childFuture = commonBatchProcessor.asyncProcessRequest(ctx, childRequest);
            if (childFuture.isDone()) {
                doneResults.add(childFuture);
            } else {
                undoneResults.add(childFuture);
            }
        }

        if (doneResults.size() + undoneResults.size() != requestChildren.size()) {

        }

        MergeBatchResponseStrategy strategy = selectStrategy(this);

        return strategy.merge(doneResults, undoneResults);
    }

    private MergeBatchResponseStrategy selectStrategy(CommonBatchProcessor commonBatchProcessor) {
        if (commonBatchProcessor instanceof PullMessageProcessor) {
            return PullMessageMergeStrategy.getInstance();
        } /*else if (commonBatchProcessor instanceof ConsumerManageProcessor) {
            return null;
        } else if (commonBatchProcessor instanceof SendMessageProcessor) {
            return null;
        }*/ else {
            throw new RuntimeException("not support.");
        }
    }

    private CommonBatchProcessor dispatchToProcessor() {
        return new CommonBatchProcessor();
    }
}
