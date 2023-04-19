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
 * - Unlike {@link java.io.BufferedInputStream#skip(long)}
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
    protected int limit;
    /**
     *
     */
    protected int pos;
    /**
     * Reference for the intermediate buffer. This reference is only kept for releasing the buffer from the
     * buffer supplier.
     */
    private final ByteBuffer intermediateBufRef;


    public ChunkedBytesStream(InputStream sourceStream, BufferSupplier bufferSupplier, int intermediateBufSize) {
        this.bufferSupplier = bufferSupplier;
        this.sourceStream = sourceStream;
        intermediateBufRef = bufferSupplier.get(intermediateBufSize);
        if (!intermediateBufRef.hasArray() || (intermediateBufRef.arrayOffset() != 0)) {
            throw new IllegalArgumentException("provided ByteBuffer lacks array or has non-zero arrayOffset");
        }
        intermediateBuf = intermediateBufRef.array();
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
            fill();
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
    int fill() throws IOException {
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
                    fill();
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

    @Override
    public int skipBytes(int toSkip) throws IOException {
        if (toSkip <= 0) {
            return 0;
        }
        int totalSkipped = 0;

        // Skip what exists in the intermediate buffer first
        int avail = limit - pos;
        int bytesToRead = (avail < (toSkip - totalSkipped)) ? avail : (toSkip - totalSkipped);
        pos += bytesToRead;
        totalSkipped += bytesToRead;

        // Use sourceStream's skip() to skip the rest
        while ((totalSkipped < toSkip) && ((bytesToRead = (int) getInIfOpen().skip(toSkip - totalSkipped)) > 0)) {
            totalSkipped += bytesToRead;
        }

        return totalSkipped;
    }

    @Override
    public byte readByte() throws IOException {
        if (pos >= limit) {
            fill();
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
