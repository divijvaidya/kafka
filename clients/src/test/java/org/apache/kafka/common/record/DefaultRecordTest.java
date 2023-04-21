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
package org.apache.kafka.common.record;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefaultRecordTest {

    private byte[] skipArray;

    @BeforeEach
    public void setUp() {
        skipArray = new byte[64];
    }

    @Test
    public void testBasicSerde() throws IOException {
        Header[] headers = new Header[] {
            new RecordHeader("foo", "value".getBytes()),
            new RecordHeader("bar", (byte[]) null),
            new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
        };

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("hi".getBytes(), "there".getBytes()),
            new SimpleRecord(null, "there".getBytes()),
            new SimpleRecord("hi".getBytes(), null),
            new SimpleRecord(null, null),
            new SimpleRecord(15L, "hi".getBytes(), "there".getBytes(), headers)
        };

        for (SimpleRecord record : records) {
            int baseSequence = 723;
            long baseOffset = 37;
            int offsetDelta = 10;
            long baseTimestamp = System.currentTimeMillis();
            long timestampDelta = 323;

            ByteBufferOutputStream out = new ByteBufferOutputStream(1024);
            DefaultRecord.writeTo(new DataOutputStream(out), offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers());
            ByteBuffer buffer = out.buffer();
            buffer.flip();

            DefaultRecord logRecord = DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, null);
            assertNotNull(logRecord);
            assertEquals(baseOffset + offsetDelta, logRecord.offset());
            assertEquals(baseSequence + offsetDelta, logRecord.sequence());
            assertEquals(baseTimestamp + timestampDelta, logRecord.timestamp());
            assertEquals(record.key(), logRecord.key());
            assertEquals(record.value(), logRecord.value());
            assertArrayEquals(record.headers(), logRecord.headers());
            assertEquals(DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers()), logRecord.sizeInBytes());
        }
    }

    @Test
    public void testBasicSerdeInvalidHeaderCountTooHigh() throws IOException {
        Header[] headers = new Header[] {
            new RecordHeader("foo", "value".getBytes()),
            new RecordHeader("bar", null),
            new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
        };

        SimpleRecord record = new SimpleRecord(15L, "hi".getBytes(), "there".getBytes(), headers);

        int baseSequence = 723;
        long baseOffset = 37;
        int offsetDelta = 10;
        long baseTimestamp = System.currentTimeMillis();
        long timestampDelta = 323;

        ByteBufferOutputStream out = new ByteBufferOutputStream(1024);
        DefaultRecord.writeTo(new DataOutputStream(out), offsetDelta, timestampDelta, record.key(), record.value(),
                record.headers());
        ByteBuffer buffer = out.buffer();
        buffer.flip();
        buffer.put(14, (byte) 8);
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, null));
    }

    @Test
    public void testBasicSerdeInvalidHeaderCountTooLow() throws IOException {
        Header[] headers = new Header[] {
            new RecordHeader("foo", "value".getBytes()),
            new RecordHeader("bar", null),
            new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
        };

        SimpleRecord record = new SimpleRecord(15L, "hi".getBytes(), "there".getBytes(), headers);

        int baseSequence = 723;
        long baseOffset = 37;
        int offsetDelta = 10;
        long baseTimestamp = System.currentTimeMillis();
        long timestampDelta = 323;

        ByteBufferOutputStream out = new ByteBufferOutputStream(1024);
        DefaultRecord.writeTo(new DataOutputStream(out), offsetDelta, timestampDelta, record.key(), record.value(),
                record.headers());
        ByteBuffer buffer = out.buffer();
        buffer.flip();
        buffer.put(14, (byte) 4);

        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, baseSequence, null));
    }

    @Test
    public void testInvalidKeySize() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;
        int keySize = 105; // use a key size larger than the full message

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(keySize, buf);
        buf.position(buf.limit());

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidKeySizePartial() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;
        int keySize = 105; // use a key size larger than the full message

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(keySize, buf);
        buf.position(buf.limit());

        buf.flip();
        InputStream inputStream = new ByteBufferInputStream(buf);
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readPartiallyFrom(inputStream, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidValueSize() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;
        int valueSize = 105; // use a value size larger than the full message

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(valueSize, buf);
        buf.position(buf.limit());

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidValueSizePartial() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;
        int valueSize = 105; // use a value size larger than the full message

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(valueSize, buf);
        buf.position(buf.limit());

        buf.flip();
        InputStream inputStream = new ByteBufferInputStream(buf);
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readPartiallyFrom(inputStream, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidNumHeaders() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(-1, buf); // -1 num.headers, not allowed
        buf.position(buf.limit());

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));

        ByteBuffer buf2 = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf2);
        buf2.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf2);
        ByteUtils.writeVarint(offsetDelta, buf2);
        ByteUtils.writeVarint(-1, buf2); // null key
        ByteUtils.writeVarint(-1, buf2); // null value
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf2); // more headers than remaining buffer size, not allowed
        buf2.position(buf2.limit());

        buf2.flip();
        assertThrows(InvalidRecordException.class,
                () -> DefaultRecord.readFrom(buf2, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidNumHeadersPartial() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(-1, buf); // -1 num.headers, not allowed
        buf.position(buf.limit());

        buf.flip();
        InputStream inputStream = new ByteBufferInputStream(buf);
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readPartiallyFrom(inputStream, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidHeaderKey() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(105, buf); // header key too long
        buf.position(buf.limit());

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () ->  DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidHeaderKeyPartial() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(105, buf); // header key too long
        buf.position(buf.limit());

        buf.flip();
        InputStream inputStream = new ByteBufferInputStream(buf);
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readPartiallyFrom(inputStream, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testNullHeaderKey() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(-1, buf); // null header key not allowed
        buf.position(buf.limit());

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testNullHeaderKeyPartial() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(-1, buf); // null header key not allowed
        buf.position(buf.limit());

        buf.flip();
        InputStream inputStream = new ByteBufferInputStream(buf);
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readPartiallyFrom(inputStream, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidHeaderValue() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(1, buf);
        buf.put((byte) 1);
        ByteUtils.writeVarint(105, buf); // header value too long
        buf.position(buf.limit());

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidHeaderValuePartial() {
        byte attributes = 0;
        long timestampDelta = 2;
        int offsetDelta = 1;
        int sizeOfBodyInBytes = 100;

        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(timestampDelta, buf);
        ByteUtils.writeVarint(offsetDelta, buf);
        ByteUtils.writeVarint(-1, buf); // null key
        ByteUtils.writeVarint(-1, buf); // null value
        ByteUtils.writeVarint(1, buf);
        ByteUtils.writeVarint(1, buf);
        buf.put((byte) 1);
        ByteUtils.writeVarint(105, buf); // header value too long
        buf.position(buf.limit());

        buf.flip();
        InputStream inputStream = new ByteBufferInputStream(buf);
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readPartiallyFrom(inputStream, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testUnderflowReadingTimestamp() {
        byte attributes = 0;
        int sizeOfBodyInBytes = 1;
        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes));
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testUnderflowReadingVarlong() {
        byte attributes = 0;
        int sizeOfBodyInBytes = 2; // one byte for attributes, one byte for partial timestamp
        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + 1);
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        buf.put(attributes);
        ByteUtils.writeVarlong(156, buf); // needs 2 bytes to represent
        buf.position(buf.limit() - 1);

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testInvalidVarlong() {
        byte attributes = 0;
        int sizeOfBodyInBytes = 11; // one byte for attributes, 10 bytes for max timestamp
        ByteBuffer buf = ByteBuffer.allocate(sizeOfBodyInBytes + ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + 1);
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);
        int recordStartPosition = buf.position();

        buf.put(attributes);
        ByteUtils.writeVarlong(Long.MAX_VALUE, buf); // takes 10 bytes
        buf.put(recordStartPosition + 10, Byte.MIN_VALUE); // use an invalid final byte

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }

    @Test
    public void testSerdeNoSequence() throws IOException {
        ByteBuffer key = ByteBuffer.wrap("hi".getBytes());
        ByteBuffer value = ByteBuffer.wrap("there".getBytes());
        long baseOffset = 37;
        int offsetDelta = 10;
        long baseTimestamp = System.currentTimeMillis();
        long timestampDelta = 323;

        ByteBufferOutputStream out = new ByteBufferOutputStream(1024);
        DefaultRecord.writeTo(new DataOutputStream(out), offsetDelta, timestampDelta, key, value, new Header[0]);
        ByteBuffer buffer = out.buffer();
        buffer.flip();

        DefaultRecord record = DefaultRecord.readFrom(buffer, baseOffset, baseTimestamp, RecordBatch.NO_SEQUENCE, null);
        assertNotNull(record);
        assertEquals(RecordBatch.NO_SEQUENCE, record.sequence());
    }

    @Test
    public void testInvalidSizeOfBodyInBytes() {
        int sizeOfBodyInBytes = 10;
        ByteBuffer buf = ByteBuffer.allocate(5);
        ByteUtils.writeVarint(sizeOfBodyInBytes, buf);

        buf.flip();
        assertThrows(InvalidRecordException.class,
            () -> DefaultRecord.readFrom(buf, 0L, 0L, RecordBatch.NO_SEQUENCE, null));
    }
}
