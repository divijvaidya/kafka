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

import java.io.IOException;
import java.io.InputStream;

/**
 * SkippableChunkedBytesStream is a variant of ChunkedBytesStream which does not push skip() to the sourceStream.
 * <p>
 * Unlike BufferedInputStream.skip() and ChunkedBytesStream.skip(), this does not push skip() to sourceStream.
 * We want to avoid pushing this to sourceStream because it's implementation maybe inefficient, e.g. the case of Z
 * stdInputStream which allocates a new buffer from buffer pool, per skip call.
 *
 * @see ChunkedBytesStream
 */
public class SkippableChunkedBytesStream extends ChunkedBytesStream {
    public SkippableChunkedBytesStream(InputStream sourceStream, BufferSupplier bufferSupplier, int intermediateBufSize) {
        super(sourceStream, bufferSupplier, intermediateBufSize);
    }

    /**
     * This implementation of skip reads the data from sourceStream in chunks, copies the data into intermediate buffer
     * and skips it. Note that this method doesn't push the skip() to sourceStream's implementation.
     */
    @Override
    public int skipBytes(int toSkip) throws IOException {
        if (toSkip <= 0) {
            return 0;
        }

        int totalSkipped = 0;
        while (totalSkipped < toSkip) {
            if (pos >= limit) {
                fill();
                if (pos >= limit)
                    break;
            }

            int avail = limit - pos;
            int bytesToRead = (avail < (toSkip - totalSkipped)) ? avail : (toSkip - totalSkipped);
            pos += bytesToRead;
            totalSkipped += bytesToRead;
        }

        return totalSkipped;
    }
}
