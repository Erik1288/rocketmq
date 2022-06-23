package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CompletableFuture;

public class PullMessageResult {
    private boolean isAsyncResponse;
    private RemotingCommand syncResponse;
    private CompletableFuture<RemotingCommand> asyncResponse;

    public boolean isAsyncResponse() {
        return isAsyncResponse;
    }

    public RemotingCommand getSyncResponse() {
        return syncResponse;
    }

    public void setSyncResponse(RemotingCommand syncResponse) {
        this.syncResponse = syncResponse;
        this.isAsyncResponse = false;
    }

    public CompletableFuture<RemotingCommand> getAsyncResponse() {
        return asyncResponse;
    }

    public void setAsyncResponse(CompletableFuture<RemotingCommand> asyncResponse) {
        this.asyncResponse = asyncResponse;
        this.isAsyncResponse = true;
    }
}
