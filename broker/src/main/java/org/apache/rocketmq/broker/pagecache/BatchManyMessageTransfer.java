package org.apache.rocketmq.broker.pagecache;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import org.apache.rocketmq.store.GetMessageResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class BatchManyMessageTransfer  {
    private final List<ByteBuffer> byteBufferHeaders;
    private final List<GetMessageResult> getMessageResults;

    public BatchManyMessageTransfer(List<ByteBuffer> byteBufferHeaders, List<GetMessageResult> getMessageResults) {
        this.byteBufferHeaders = byteBufferHeaders;
        this.getMessageResults = getMessageResults;
    }


}
