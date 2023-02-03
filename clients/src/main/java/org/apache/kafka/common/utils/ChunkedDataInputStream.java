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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * ChunkedDataInputStream is a stream which reads from source stream in chunks of configurable size. The
 * implementation of this stream is optimized to reduce the number of calls to sourceStream#read(). This works best in
 * scenarios where sourceStream()#read() call is expensive, e.g. when the call crosses JNI boundary.
 * <p>
 * The functionality of this stream is a combination of DataInput and BufferedInputStream with the following
 * differences:
 * - Unlike BufferedInputStream.skip(), this does not push skip() to sourceStream. We want to avoid pushing this to
 * sourceStream because it's implementation maybe inefficient, e.g. the case of ZstdInputStream which allocates a new
 * buffer from buffer pool, per skip call.
 * - Unlike BufferedInputStream, which allocates an intermediate buffer, this uses a buffer supplier to create the
 * intermediate buffer
 * - Unlike DataInputStream, the readByte method does not push the reading of a byte to sourceStream.
 *
 * Note that:
 * - this class is not thread safe and shouldn't be used in scenarios where multiple threads access this.
 * - many method are un-supported in this class because they aren't currently used in the caller code.
 */
public class ChunkedDataInputStream extends InputStream implements DataInput {
    /**
     * Supplies the ByteBuffer which is used as intermediate buffer to store the chunk of output data.
     */
    private final BufferSupplier bufferSupplier;
    /**
     * Source stream containing compressed data.
     */
    private InputStream sourceStream;
    /**
     * Intermediate buffer to store the chunk of output data. The ChunkedDataInputStream is considered closed if
     * this buffer is null.
     */
    private ByteBuffer intermediateBuf;

    public ChunkedDataInputStream(InputStream sourceStream, BufferSupplier bufferSupplier, int intermediateBufSize) {
        this.bufferSupplier = bufferSupplier;
        this.sourceStream = sourceStream;
        intermediateBuf = bufferSupplier.get(intermediateBufSize);
        // set for reading.
        intermediateBuf.flip();
    }

    private ByteBuffer getBufIfOpen() throws IOException {
        ByteBuffer buffer = intermediateBuf;
        if (buffer == null)
            throw new IOException("Stream closed");
        return buffer;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        int n = fillIfNotAvailable();
        if (n < 0)
            return -1;

        return Byte.toUnsignedInt(intermediateBuf.get());
    }

    private InputStream getInIfOpen() throws IOException {
        InputStream input = sourceStream;
        if (input == null)
            throw new IOException("Stream closed");
        return input;
    }

    /**
     * Fills the intermediate buffer with more data. The amount of new data read is equal to the remaining empty space
     * in the buffer. For optimal performance, read as much data as possible in this call.
     */
    private int fill() throws IOException {
        ByteBuffer buffer = getBufIfOpen();

        // switch to writing mode
        buffer.compact();

        int toRead = buffer.remaining();
        int bytesRead = getInIfOpen().read(buffer.array(), buffer.position(), toRead);

        if (bytesRead > 0)
            buffer.position(buffer.position() + bytesRead);

        // prepare for reading
        buffer.flip();

        return bytesRead;
    }

    private void ensureOpen() throws IOException {
        if (intermediateBuf == null)
            throw new IOException("Stream closed");
    }

    @Override
    public void close() throws IOException {
        ByteBuffer buf = intermediateBuf;
        intermediateBuf = null;

        InputStream input = sourceStream;
        sourceStream = null;

        if (buf != null)
            bufferSupplier.release(buf);
        if (input != null)
            input.close();
    }

    /**
     *
     *
     * @param toSkipBytes the number of bytes to be skipped.
     * @return
     * @throws IOException
     */
    @Override
    public long skip(long toSkipBytes) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int available() throws IOException {
        if (intermediateBuf == null) {
            return 0;
        }

        int n = fillIfNotAvailable();
        if (n < 0)
            return 0;

        return intermediateBuf.remaining();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readlimit) {
        throw new RuntimeException("mark not supported");
    }

    @Override
    public void reset() {
        throw new RuntimeException("reset not supported");
    }

    /**
     *
     * @throws IOException
     * @throws EOFException
     */
    private int fillIfNotAvailable() throws IOException {
        if (!intermediateBuf.hasRemaining()) {
            int bytesRead = fill();
            if (bytesRead < 0)
                return -1;
        }
        return 0;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        ensureOpen();

        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int bytesRead = 0;
        int totalRead = 0;
        while (totalRead < len) {
            int bytesToRead = Math.min(intermediateBuf.remaining(), len - totalRead);

            if (intermediateBuf.hasRemaining()) {
                System.arraycopy(intermediateBuf.array(), intermediateBuf.position(), b, off + totalRead, bytesToRead);
                intermediateBuf.position(intermediateBuf.position() + bytesToRead);
                bytesRead += bytesToRead;
            } else {
                if (bytesToRead >= intermediateBuf.capacity()) {
                    // don't use intermediate buffer if we need to read more than it's capacity
                    bytesRead = getInIfOpen().read(b, off + totalRead, bytesToRead);
                } else {
                    bytesRead = fill();
                }
            }

            if (bytesRead < 0)
                break;
            else
                totalRead += bytesRead;
        }

        if ((bytesRead < 0) && (totalRead == 0))
            throw new EOFException();
    }

    /**
     * This implementation of skip reads the data from sourceStream in chunks, copies the data into intermediate buffer
     * and skips it. Note that this method doesn't push the skip() to sourceStream's implementation.
     */
    @Override
    public int skipBytes(int toSkip) throws IOException {
        ensureOpen();

        if (toSkip <= 0) {
            return 0;
        }

        int totalSkipped = 0;
        int bytesToRead = 0;
        while (totalSkipped < toSkip) {
            if (!intermediateBuf.hasRemaining()) {
                bytesToRead = fill();
            }

            if (bytesToRead < 0) {
                break;
            } else {
                bytesToRead = Math.min(intermediateBuf.remaining(), toSkip - totalSkipped);
                intermediateBuf.position(intermediateBuf.position() + bytesToRead);
                totalSkipped += bytesToRead;
            }
        }

        return totalSkipped;
    }

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() throws IOException {
        ensureOpen();
        int n = fillIfNotAvailable();
        if (n < 0)
            throw new EOFException();

        return intermediateBuf.get();
    }

    @Override
    public int readUnsignedByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short readShort() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public char readChar() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float readFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() {
        throw new UnsupportedOperationException();
    }
}
