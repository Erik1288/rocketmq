/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.pagecache;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class BatchManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer batchHeader;
    private final List<ManyMessageTransfer> manyMessageTransferList;
    private long transferred = 0;

    public BatchManyMessageTransfer(ByteBuffer batchHeader, List<ManyMessageTransfer> manyMessageTransferList) {
        this.batchHeader = batchHeader;
        this.manyMessageTransferList = manyMessageTransferList;
    }

    @Override
    public long position() {
        return batchHeader.position() + manyMessageTransferList
                .stream()
                .mapToLong(ManyMessageTransfer::position)
                .sum();
    }

    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long transferred() {
        return transferred;
    }

    @Override
    public long count() {
        return batchHeader.limit() + manyMessageTransferList
                .stream()
                .mapToLong(ManyMessageTransfer::count)
                .sum();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        // batch-header
        if (this.batchHeader.hasRemaining()) {
            transferred += target.write(this.batchHeader);
        }
        // batch-body
        for (ManyMessageTransfer manyMessageTransfer : manyMessageTransferList) {
            while (!manyMessageTransfer.isComplete()) {
                transferred += manyMessageTransfer.transferTo(target, position);
            }
        }
        return transferred;
    }

    @Override
    protected void deallocate() {
        manyMessageTransferList.forEach(ManyMessageTransfer::deallocate);
    }

    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        return this;
    }

    public void close() {
        manyMessageTransferList.forEach(ManyMessageTransfer::close);
    }
}
