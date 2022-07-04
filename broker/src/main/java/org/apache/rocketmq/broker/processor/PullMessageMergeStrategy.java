package org.apache.rocketmq.broker.processor;

import com.google.common.base.Preconditions;
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PullMessageMergeStrategy extends MergeBatchResponseStrategy {
    private static final MergeBatchResponseStrategy instance = new PullMessageMergeStrategy();

    private PullMessageMergeStrategy() {
    }

    @Override
    public CompletableFuture<RemotingCommand> merge(
            List<CompletableFuture<RemotingCommand>> doneResults,
            List<CompletableFuture<RemotingCommand>> undoneResults) {
        CompletableFuture<RemotingCommand> batchFuture = new CompletableFuture<>();

        // case 1. all results are done
        if (undoneResults.isEmpty()) {
            List<RemotingCommand> doneResponses = getResponseChildren(doneResults);
            batchFuture.complete(mergeCompletedFutures(doneResponses));
            return batchFuture;
        }

        // case 2. some results are done, but others are not.
        if (!doneResults.isEmpty()) {
            // done results: respond to client.
            List<RemotingCommand> doneResponses = getResponseChildren(doneResults);
            batchFuture.complete(mergeCompletedFutures(doneResponses));
            // undone result: will be discarded.
            undoneResults.forEach(future -> future.whenComplete((childResp, throwable) -> {
                completeUndoneResults(childResp);
            }));
            return batchFuture;
        }

        // case 3, no result is done.
        AtomicInteger successNum = new AtomicInteger(0);
        AtomicBoolean completeBatch = new AtomicBoolean(false);
        undoneResults
            .forEach(childFuture -> childFuture.whenComplete((childResp, throwable) -> {
                int hasDataNum = successNum.incrementAndGet();
                if (hasDataNum > 0 && completeBatch.compareAndSet(true, false)) {
                    batchFuture.complete(mergeCompletedFutures(Collections.singletonList(childResp)));
                } else {
                    completeUndoneResults(childResp);
                }
            }));

        return batchFuture;
    }

    private void completeUndoneResults(RemotingCommand childResp) {
        boolean zeroCopy = childResp.getCallback() instanceof FileRegion;
        if (zeroCopy) {
            // need to call finallyCallback
            childResp.getCallback().run();
        }
    }

    public static MergeBatchResponseStrategy getInstance() {
        return instance;
    }

    private List<RemotingCommand> getResponseChildren(List<CompletableFuture<RemotingCommand>> futureChildren) {
        List<RemotingCommand> childrenResponse = new ArrayList<>();

        for (CompletableFuture<RemotingCommand> childFuture : futureChildren) {
            try {
                childrenResponse.add(childFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                // will never happen.
                throw new RuntimeException(e.getMessage());
            }
        }
        return childrenResponse;
    }

    private RemotingCommand mergeCompletedFutures(List<RemotingCommand> doneResponses) {
        Preconditions.checkArgument(!doneResponses.isEmpty());

        RemotingCommand sample = doneResponses.get(0);
        boolean zeroCopy = sample.getAttachment() instanceof FileRegion;
        // zero-copy
        if (zeroCopy) {
            for (RemotingCommand doneResp : doneResponses) {
                ManyMessageTransfer manyMessageTransfer = (ManyMessageTransfer) doneResp.getAttachment();
                GetMessageResult getMessageResult = manyMessageTransfer.getGetMessageResult();
                ByteBuffer byteBufferHeader = manyMessageTransfer.getByteBufferHeader();
            }
            return null;
        } else {
            return RemotingCommand.mergeChildren(doneResponses);
        }
    }
}
