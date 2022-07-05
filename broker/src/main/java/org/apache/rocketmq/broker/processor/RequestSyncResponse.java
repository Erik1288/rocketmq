package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class RequestSyncResponse {
    private RemotingCommand request;
    private RemotingCommand response;

    public RequestSyncResponse(RemotingCommand request, RemotingCommand response) {
        this.request = request;
        this.response = response;
    }

    public RemotingCommand getRequest() {
        return request;
    }

    public RemotingCommand getResponse() {
        return response;
    }
}
