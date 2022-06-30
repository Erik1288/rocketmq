package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class Result<T> {
    // the response
    private final RemotingCommand error;
    private final T success;

    private Result(RemotingCommand error, T success) {
        this.error = error;
        this.success = success;
    }

    public static <T> Result<T> error(RemotingCommand error) {
        return new Result<>(error, null);
    }

    public static <T> Result<T> success(T success) {
        return new Result<>(null, success);
    }

    public boolean hasError() {
        return this.error != null;
    }

    public T getSuccess() {
        return success;
    }

    public RemotingCommand getError() {
        return error;
    }
}
