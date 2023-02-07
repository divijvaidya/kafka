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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Stream;

public class ChunkedDataInputStreamTest {
    private static final Random RANDOM = new Random(1337);
    private final BufferSupplier supplier = BufferSupplier.NO_CACHING;

    @Test
    public void readFully_testEofError() throws IOException {
        ByteBuffer input = ByteBuffer.allocate(8);
        int lengthGreaterThanInput = input.capacity() + 1;
        byte[] got = new byte[lengthGreaterThanInput];
        try (InputStream is = new ChunkedDataInputStream(new ByteBufferInputStream(input), supplier, 10)) {
            assertThrows(EOFException.class, () -> ((DataInput) is).readFully(got, 0, got.length));
        }
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    public void readFully_testCorrectness(ByteBuffer input) throws IOException {
        byte[] got = new byte[input.array().length];
        try (InputStream is = new ChunkedDataInputStream(new ByteBufferInputStream(input), supplier, 10)) {
            // perform a 2 pass read. this tests the scenarios where one pass may lead to partially consumed
            // intermediate buffer
            int toRead = RANDOM.nextInt(got.length);
            ((DataInput) is).readFully(got, 0, toRead);
            ((DataInput) is).readFully(got, toRead, got.length - toRead);
        }
        assertArrayEquals(input.array(), got);
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    public void readByte_testCorrectness(ByteBuffer input) throws IOException {
        byte[] got = new byte[input.array().length];
        try (InputStream is = new ChunkedDataInputStream(new ByteBufferInputStream(input), supplier, 10)) {
            int i = 0;
            while ((is.available() != 0) && (i < got.length)) {
                got[i++] = ((DataInput) is).readByte();
            }
        }
        assertArrayEquals(input.array(), got);
    }

    @Test
    public void readByte_testEofError() throws IOException {
        ByteBuffer input = ByteBuffer.allocate(8);
        int lengthGreaterThanInput = input.capacity() + 1;
        try (InputStream is = new ChunkedDataInputStream(new ByteBufferInputStream(input), supplier, 10)) {
            assertThrows(EOFException.class, () -> {
                int i = 0;
                while (i++ < lengthGreaterThanInput) {
                    ((DataInput) is).readByte();
                }
            });
        }
    }

    @ParameterizedTest
    @MethodSource("provideSourceBytebuffersForTest")
    public void read_testCorrectness(ByteBuffer inputBuf) throws IOException {
        int[] inputArr = new int[inputBuf.capacity()];
        for (int i = 0; i < inputArr.length; i++) {
            inputArr[i] = Byte.toUnsignedInt(inputBuf.get());
        }
        int[] got = new int[inputArr.length];
        inputBuf.rewind();
        try (InputStream is = new ChunkedDataInputStream(new ByteBufferInputStream(inputBuf), supplier, 10)) {
            int i = 0;
            while ((is.available() != 0) && (i < got.length)) {
                got[i++] = is.read();
            }
        }
        assertArrayEquals(inputArr, got);
    }

    @Test
    public void read_testEndOfFile() throws IOException {
        ByteBuffer inputBuf = ByteBuffer.allocate(2);
        int lengthGreaterThanInput = inputBuf.capacity() + 1;

        try (InputStream is = new ChunkedDataInputStream(new ByteBufferInputStream(inputBuf), supplier, 10)) {
            int cnt = 0;
            while (cnt++ < lengthGreaterThanInput) {
                int res = is.read();
                if (cnt > inputBuf.capacity())
                    assertEquals(-1, res, "enf of file for read should be -1");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideSourceSkipValuesForTest")
    public void skip_testCorrectness(int numBytesToSkip, int expectedSkipCalls) throws IOException {
        ByteBuffer inputBuf = ByteBuffer.allocate(16);
        RANDOM.nextBytes(inputBuf.array());
        inputBuf.rewind();

        final InputStream sourcestream = spy(new ByteBufferInputStream(inputBuf));
        try (InputStream is = new ChunkedDataInputStream(sourcestream, supplier, 10)) {
            // fill up intermediate buffer
            is.read(); // now we should have 10 - 1 = 9 bytes in intermediate buffer

            // skip bytes and observe the calls to source stream's skip() method
            int res = ((DataInput) is).skipBytes(numBytesToSkip);
            assertEquals(numBytesToSkip, res);
            verify(sourcestream, times(expectedSkipCalls)).skip(anyLong());
        }
    }

    @Test
    public void skip_testEndOfSource() throws IOException {
        ByteBuffer inputBuf = ByteBuffer.allocate(16);
        RANDOM.nextBytes(inputBuf.array());
        inputBuf.rewind();

        final InputStream sourcestream = spy(new ByteBufferInputStream(inputBuf));
        try (InputStream is = new ChunkedDataInputStream(sourcestream, supplier, 10)) {
            int res = ((DataInput) is).skipBytes(inputBuf.capacity() + 1);
            assertEquals(inputBuf.capacity(), res);
        }
    }

    private static Stream<Arguments> provideSourceSkipValuesForTest() {
        return Stream.of(
            // skip available in intermediate buffer
            Arguments.of(9, 0),
            // skip not available in intermediate buffer
            Arguments.of(10, 1)
        );
    }

    private static Stream<Arguments> provideSourceBytebuffersForTest() {
        ByteBuffer bufGreaterThanIntermediateBuf = ByteBuffer.allocate(16);
        RANDOM.nextBytes(bufGreaterThanIntermediateBuf.array());
        bufGreaterThanIntermediateBuf.position(bufGreaterThanIntermediateBuf.capacity());

        ByteBuffer bufMuchGreaterThanIntermediateBuf = ByteBuffer.allocate(100);
        RANDOM.nextBytes(bufMuchGreaterThanIntermediateBuf.array());
        bufMuchGreaterThanIntermediateBuf.position(bufMuchGreaterThanIntermediateBuf.capacity());

        return Stream.of(
            // empty byte array
            Arguments.of(ByteBuffer.allocate(2)),
            // byte array with 1 byte
            Arguments.of(ByteBuffer.allocate(1).put((byte) 1).flip()),
            // byte array with size < intermediate buffer
            Arguments.of(ByteBuffer.allocate(8).put("12345678".getBytes()).flip()),
            // byte array with size > intermediate buffer
            Arguments.of(bufGreaterThanIntermediateBuf.flip()),
            // byte array with size >> intermediate buffer
            Arguments.of(bufMuchGreaterThanIntermediateBuf.flip())
        );
    }
}
