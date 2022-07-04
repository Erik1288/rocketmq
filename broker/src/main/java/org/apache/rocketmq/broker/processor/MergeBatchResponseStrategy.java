package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class MergeBatchResponseStrategy {
    public abstract CompletableFuture<RemotingCommand> merge(List<CompletableFuture<RemotingCommand>> doneResults,
                                                      List<CompletableFuture<RemotingCommand>> undoneResults);
}
