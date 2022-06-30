package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageFilter;

import java.net.SocketAddress;

public class StoreReturnedResult {
    private final GetMessageResult getMessageResult;
    private final RemotingCommand request;
    private final PullMessageRequestHeader requestHeader;
    private final SocketAddress clientHost;
    private final SubscriptionGroupConfig subscriptionGroupConfig;
    private final boolean brokerAllowSuspend;
    private final boolean hasCommitOffsetFlag;
    private final MessageFilter messageFilter;
    private final RemotingCommand response;
    private final boolean transferMsgByHeap;

    public StoreReturnedResult(GetMessageResult getMessageResult, RemotingCommand request, PullMessageRequestHeader requestHeader,
                               SocketAddress clientHost, SubscriptionGroupConfig subscriptionGroupConfig, boolean brokerAllowSuspend,
                               boolean hasCommitOffsetFlag, MessageFilter messageFilter, RemotingCommand response, boolean transferMsgByHeap) {
        this.getMessageResult = getMessageResult;
        this.request = request;
        this.requestHeader = requestHeader;
        this.clientHost = clientHost;
        this.subscriptionGroupConfig = subscriptionGroupConfig;
        this.brokerAllowSuspend = brokerAllowSuspend;
        this.hasCommitOffsetFlag = false;
        this.messageFilter = messageFilter;
        this.response = response;
        this.transferMsgByHeap = transferMsgByHeap;
    }

    public GetMessageResult getGetMessageResult() {
        return getMessageResult;
    }

    public RemotingCommand getRequest() {
        return request;
    }

    public PullMessageRequestHeader getRequestHeader() {
        return requestHeader;
    }

    public SocketAddress getClientHost() {
        return clientHost;
    }

    public SubscriptionGroupConfig getSubscriptionGroupConfig() {
        return subscriptionGroupConfig;
    }

    public boolean isBrokerAllowSuspend() {
        return brokerAllowSuspend;
    }

    public boolean isHasCommitOffsetFlag() {
        return hasCommitOffsetFlag;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }

    public RemotingCommand getResponse() {
        return response;
    }

    public boolean isTransferMsgByHeap() {
        return transferMsgByHeap;
    }
}
