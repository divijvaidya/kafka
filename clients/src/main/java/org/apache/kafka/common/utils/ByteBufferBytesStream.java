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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Provides a {@link BytesStream} wrapper over a {@link ByteBuffer}.
 */
public class ByteBufferBytesStream implements BytesStream {
    final private ByteBuffer buf;

    public ByteBufferBytesStream(final ByteBuffer buffer) {
        // we do not modify the markers of source buffer
        buf = buffer.duplicate();
    }

    @Override
    public int read() throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get() & 0xFF;
    }

    @Override
    public int skipBytes(int toSkip) {
        if (toSkip <= 0) {
            return 0;
        }

        int avail = Math.min(toSkip, buf.remaining());
        buf.position(buf.position() + avail);
        return avail;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (!buf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buf.remaining());
        buf.get(b, off, len);
        return len;
    }

    @Override
    public byte readByte() throws IOException {
        int ret = read();
        if (ret == -1)
            throw new EOFException();
        return (byte) ret;
    }

    @Override
    public void close() throws IOException {
        // no-op for a buffer source
    }
}
