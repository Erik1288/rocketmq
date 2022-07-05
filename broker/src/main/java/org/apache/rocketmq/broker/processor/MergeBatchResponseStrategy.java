package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface MergeBatchResponseStrategy {
    CompletableFuture<RemotingCommand> merge(
            RemotingCommand batchRequest, Map<Integer, CompletableFuture<RemotingCommand>> doneResults,
            Map<Integer, CompletableFuture<RemotingCommand>> undoneResults);
}
