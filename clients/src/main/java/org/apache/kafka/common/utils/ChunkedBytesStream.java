/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * ChunkedBytesStream is a {@link BytesStream} which reads from source stream in chunks of configurable size. The
 * implementation of this reader is optimized to reduce the number of calls to sourceStream#read(). This works best in
 * scenarios where sourceStream#read() call is expensive, e.g. when the call crosses JNI boundary.
 * <p>
 * The functionality of this stream is a combination of DataInput and BufferedInputStream with the following
 * differences:
 * - Unlike {@link java.io.BufferedInputStream#skip(long)} this class could be configured to not push skip() to
 * sourceStream. We may want to avoid pushing this to sourceStream because it's implementation maybe inefficient,
 * e.g. the case of ZstdInputStream which allocates a new buffer from buffer pool, per skip call.
 * - Unlike {@link java.io.BufferedInputStream}, which allocates an intermediate buffer, this uses a buffer supplier to
 * create the intermediate buffer.
 * - Unlike {@link DataInputStream#readByte()}, the readByte method does not push the reading of a byte to sourceStream.
 * <p>
 * Note that:
 * - this class is not thread safe and shouldn't be used in scenarios where multiple threads access this.
 * - the implementation of this class is performance sensitive. Minor changes as usage of ByteBuffer instead of byte[]
 *   can significantly impact performance, hence, proceed with caution.
 */
public class ChunkedBytesStream implements BytesStream {
    /**
     * Supplies the ByteBuffer which is used as intermediate buffer to store the chunk of output data.
     */
    private final BufferSupplier bufferSupplier;
    /**
     * Source stream containing compressed data.
     */
    private InputStream sourceStream;
    /**
     * Intermediate buffer to store the chunk of output data. The ChunkedBytesStream is considered closed if
     * this buffer is null.
     */
    private byte[] intermediateBuf;
    /**
     * Total number of bytes written to {@link #intermediateBuf}
     */
    protected int limit = 0;
    /**
     * Index for the next byte read in {@link #intermediateBuf}
     */
    protected int pos = 0;
    /**
     * Reference for the intermediate buffer. This reference is only kept for releasing the buffer from the
     * buffer supplier.
     */
    private final ByteBuffer intermediateBufRef;
    /**
     * Determines if the skip be pushed down
     */
    private final boolean pushSkipToSourceStream;

    public ChunkedBytesStream(InputStream sourceStream, BufferSupplier bufferSupplier, int intermediateBufSize, boolean pushSkipToSourceStream) {
        this.bufferSupplier = bufferSupplier;
        this.sourceStream = sourceStream;
        intermediateBufRef = bufferSupplier.get(intermediateBufSize);
        if (!intermediateBufRef.hasArray() || (intermediateBufRef.arrayOffset() != 0)) {
            throw new IllegalArgumentException("provided ByteBuffer lacks array or has non-zero arrayOffset");
        }
        intermediateBuf = intermediateBufRef.array();
        this.pushSkipToSourceStream = pushSkipToSourceStream;
    }

    private byte[] getBufIfOpen() throws IOException {
        byte[] buffer = intermediateBuf;
        if (buffer == null)
            throw new IOException("Stream closed");
        return buffer;
    }

    @Override
    public int read() throws IOException {
        if (pos >= limit) {
            readChunk();
            if (pos >= limit)
                return -1;
        }

        return getBufIfOpen()[pos++] & 0xff;
    }

    InputStream getInIfOpen() throws IOException {
        InputStream input = sourceStream;
        if (input == null)
            throw new IOException("Stream closed");
        return input;
    }

    /**
     * Fills the intermediate buffer with more data. The amount of new data read is equal to the remaining empty space
     * in the buffer. For optimal performance, read as much data as possible in this call.
     */
    int readChunk() throws IOException {
        byte[] buffer = getBufIfOpen();

        // switch to writing mode
        pos = 0;
        limit = pos;
        int bytesRead = getInIfOpen().read(buffer, pos, buffer.length - pos);

        if (bytesRead > 0)
            limit = bytesRead + pos;

        return bytesRead;
    }

    @Override
    public void close() throws IOException {
        byte[] mybuf = intermediateBuf;
        intermediateBuf = null;

        InputStream input = sourceStream;
        sourceStream = null;

        if (mybuf != null)
            bufferSupplier.release(intermediateBufRef);
        if (input != null)
            input.close();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int totalRead = 0;
        int bytesRead = 0;
        while (totalRead < len) {
            bytesRead = 0;
            int toRead = len - totalRead;
            if (pos >= limit) {
                if (toRead >= getBufIfOpen().length) {
                    // don't use intermediate buffer if we need to read more than it's capacity
                    bytesRead = getInIfOpen().read(b, off + totalRead, toRead);
                    if (bytesRead < 0)
                        break;
                } else {
                    readChunk();
                    if (pos >= limit)
                        break;
                }
            } else {
                int avail = limit - pos;
                toRead = (avail < toRead) ? avail : toRead;
                System.arraycopy(getBufIfOpen(), pos, b, off + totalRead, toRead);
                pos += toRead;
                bytesRead = toRead;
            }

            totalRead += bytesRead;
        }

        if ((bytesRead <= 0) && (totalRead < len))
            return -1;

        return totalRead;
    }

    /**
     * This implementation of skip reads the data from sourceStream in chunks, copies the data into intermediate buffer
     * and skips it.
     */
    @Override
    public int skipBytes(int toSkip) throws IOException {
        if (toSkip <= 0) {
            return 0;
        }

        int remaining = toSkip;

        // Skip bytes stored in intermediate buffer first
        int avail = limit - pos;
        int chunkSkipped = (avail < remaining) ? avail : remaining;
        pos += chunkSkipped;
        remaining -= chunkSkipped;

        while (remaining > 0) {
            if (pushSkipToSourceStream) {
                // Use sourceStream's skip() to skip the rest.
                // conversion to int is acceptable because toSkip and remaining are int.
                chunkSkipped = (int) getInIfOpen().skip(remaining);
                if (chunkSkipped <= 0)
                    break;
            } else {
                if (pos >= limit) {
                    readChunk();
                    // if we don't have data in intermediate buffer after fill, then stop skipping
                    if (pos >= limit)
                        break;
                }
                avail = limit - pos;
                chunkSkipped = (avail < remaining) ? avail : remaining;
                pos += chunkSkipped;
            }
            remaining -= chunkSkipped;
        }
        return toSkip - remaining;
    }

    @Override
    public byte readByte() throws IOException {
        if (pos >= limit) {
            readChunk();
            if (pos >= limit)
                throw new EOFException();
        }
        return getBufIfOpen()[pos++];
    }

    // visible for testing
    public InputStream sourceStream() {
        return sourceStream;
    }
}
