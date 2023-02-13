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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SkippableChunkedBytesStreamTest {
    private static final Random RANDOM = new Random(1337);
    private final BufferSupplier supplier = BufferSupplier.NO_CACHING;

    @ParameterizedTest
    @MethodSource("provideSourceSkipValuesForTest")
    public void skip_testCorrectness(int bytesToPreRead, ByteBuffer inputBuf, int numBytesToSkip) throws IOException {
        int expectedInpLeftAfterSkip = inputBuf.remaining() - bytesToPreRead - numBytesToSkip;
        int expectedSkippedBytes = Math.min(inputBuf.remaining() - bytesToPreRead, numBytesToSkip);

        try (BytesStream is = new ChunkedBytesStream(new ByteBufferInputStream(inputBuf.duplicate()), supplier, 10)) {
            int cnt = 0;
            while (cnt++ < bytesToPreRead) {
                is.readByte();
            }

            int res = is.skipBytes(numBytesToSkip);
            assertEquals(expectedSkippedBytes, res);

            // verify that we are able to read rest of the input
            cnt = 0;
            while (cnt++ < expectedInpLeftAfterSkip) {
                is.readByte();
            }
        }
    }

    @Test
    public void skip_testEndOfSource() throws IOException {
        ByteBuffer inputBuf = ByteBuffer.allocate(16);
        RANDOM.nextBytes(inputBuf.array());
        inputBuf.rewind();

        try (BytesStream is = new ChunkedBytesStream(new ByteBufferInputStream(inputBuf), supplier, 10)) {
            int res = is.skipBytes(inputBuf.capacity() + 1);
            assertEquals(inputBuf.capacity(), res);
        }
    }

    private static Stream<Arguments> provideSourceSkipValuesForTest() {
        ByteBuffer bufGreaterThanIntermediateBuf = ByteBuffer.allocate(16);
        RANDOM.nextBytes(bufGreaterThanIntermediateBuf.array());
        bufGreaterThanIntermediateBuf.position(bufGreaterThanIntermediateBuf.capacity());
        bufGreaterThanIntermediateBuf.flip();

        ByteBuffer bufMuchGreaterThanIntermediateBuf = ByteBuffer.allocate(100);
        RANDOM.nextBytes(bufMuchGreaterThanIntermediateBuf.array());
        bufMuchGreaterThanIntermediateBuf.position(bufMuchGreaterThanIntermediateBuf.capacity());
        bufMuchGreaterThanIntermediateBuf.flip();

        ByteBuffer emptyBuffer = ByteBuffer.allocate(2);

        ByteBuffer oneByteBuf = ByteBuffer.allocate(1).put((byte) 1);
        oneByteBuf.flip();

        return Stream.of(
            // empty source byte array
            Arguments.of(0, emptyBuffer, 0),
            Arguments.of(0, emptyBuffer, 1),
            Arguments.of(1, emptyBuffer, 1),
            Arguments.of(1, emptyBuffer, 0),
            // byte source array with 1 byte
            Arguments.of(0, oneByteBuf, 0),
            Arguments.of(0, oneByteBuf, 1),
            Arguments.of(1, oneByteBuf, 0),
            Arguments.of(1, oneByteBuf, 1),
            // byte source array with full read from intermediate buf
            Arguments.of(0, bufGreaterThanIntermediateBuf.duplicate(), bufGreaterThanIntermediateBuf.capacity()),
            Arguments.of(bufGreaterThanIntermediateBuf.capacity(), bufGreaterThanIntermediateBuf.duplicate(), 0),
            Arguments.of(2, bufGreaterThanIntermediateBuf.duplicate(), 10),
            Arguments.of(2, bufGreaterThanIntermediateBuf.duplicate(), 8)
        );
    }
}
