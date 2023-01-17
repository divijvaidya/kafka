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

package org.apache.kafka.common.compress;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ZstdFactory {
    /**
     * Default compression level
     */
    private static final int DEFAULT_COMPRESSION_LEVEL = 3;

    private ZstdFactory() { }

    public static OutputStream wrapForOutput(ByteBufferOutputStream buffer) {
        try {
            // Set input buffer (uncompressed) to 16 KB (none by default) to ensure reasonable performance
            // in cases where the caller passes a small number of bytes to write (potentially a single byte).
            return new BufferedOutputStream(new ZstdOutputStreamNoFinalizer(buffer, RecyclingBufferPool.INSTANCE).setLevel(DEFAULT_COMPRESSION_LEVEL), 16 * 1024);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public static InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            // We use our own BufferSupplier instead of com.github.luben.zstd.RecyclingBufferPool since our
            // implementation doesn't require locking or soft references. This buffer pool is used by zstd-jni for
            // reading compressed data. Buffer to store decompressed data is provided by
            BufferPool bufferPool = new BufferPool() {
                @Override
                public ByteBuffer get(int capacity) {
                    return decompressionBufferSupplier.get(capacity);
                }

                @Override
                public void release(ByteBuffer buffer) {
                    decompressionBufferSupplier.release(buffer);
                }
            };

            // Set output buffer (uncompressed) to 16 KB (none by default) to ensure reasonable performance
            // in cases where the caller reads a small number of bytes (potentially a single byte).
            return new DataInputStream(new ZstdInputStreamNoFinalizer(new ByteArrayInputStream(buffer.array()), bufferPool));
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public static int getRecommendedDOutBufferSize() {
        final long recommendedSize = ZstdInputStreamNoFinalizer.recommendedDOutSize();
        return recommendedSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) recommendedSize;
    }
}
