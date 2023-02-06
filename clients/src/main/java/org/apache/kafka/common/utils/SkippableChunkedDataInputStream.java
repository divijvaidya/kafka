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
 * <p>
 * Note that:
 * - this class is not thread safe and shouldn't be used in scenarios where multiple threads access this.
 * - many method are un-supported in this class because they aren't currently used in the caller code.
 */
public class SkippableChunkedDataInputStream extends ChunkedDataInputStream {


    public SkippableChunkedDataInputStream(InputStream sourceStream, BufferSupplier bufferSupplier, int intermediateBufSize) {
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
