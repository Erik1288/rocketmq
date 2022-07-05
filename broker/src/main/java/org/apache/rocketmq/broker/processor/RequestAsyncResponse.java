package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CompletableFuture;

public class RequestAsyncResponse {
    private RemotingCommand request;
    private CompletableFuture<RemotingCommand> response;

    public RequestAsyncResponse(RemotingCommand request, CompletableFuture<RemotingCommand> response) {
        this.request = request;
        this.response = response;
    }

    public RemotingCommand getRequest() {
        return request;
    }

    public CompletableFuture<RemotingCommand> getResponse() {
        return response;
    }
}
