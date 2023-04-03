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
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This classes exposes low-level methods for reading/writing from byte streams or buffers.
 */
public final class ByteUtils {

    public static final ByteBuffer EMPTY_BUF = ByteBuffer.wrap(new byte[0]);

    private ByteUtils() {}

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Read an unsigned integer stored in little-endian format from the {@link InputStream}.
     *
     * @param in The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return in.read()
                | (in.read() << 8)
                | (in.read() << 16)
                | (in.read() << 24);
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset] << 0 & 0xff)
                | ((buffer[offset + 1] & 0xff) << 8)
                | ((buffer[offset + 2] & 0xff) << 16)
                | ((buffer[offset + 3] & 0xff) << 24);
    }

    /**
     * Read a big-endian integer from a byte array
     */
    public static int readIntBE(byte[] buffer, int offset) {
        return ((buffer[offset] & 0xFF) << 24)
            | ((buffer[offset + 1] & 0xFF) << 16)
            | ((buffer[offset + 2] & 0xFF) << 8)
            | (buffer[offset + 3] & 0xFF);
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write an unsigned integer in little-endian format to the {@link OutputStream}.
     *
     * @param out The stream to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value);
        out.write(value >>> 8);
        out.write(value >>> 16);
        out.write(value >>> 24);
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) value;
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 3]   = (byte) (value >>> 24);
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * The implementation is based on Netty's decoding of varint.
     * @see <a href="https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73">Netty's varint decoding</a>
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readUnsignedVarint(ByteBuffer buffer) {
        byte tmp = buffer.get();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = buffer.get()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = buffer.get()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = buffer.get()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = buffer.get()) << 28;
                        if (tmp < 0) {
                            throw illegalVarintException(result);
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * The implementation is based on Netty's decoding of varint.
     * @see <a href="https://github.com/netty/netty/blob/59aa6e635b9996cf21cd946e64353270679adc73/codec/src/main/java/io/netty/handler/codec/protobuf/ProtobufVarint32FrameDecoder.java#L73">Netty's varint decoding</a>
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     * @throws EOFException             if {@link DataInput} throws {@link EOFException}
     */
    static int readUnsignedVarint(DataInput in) throws IOException {
        byte tmp = in.readByte();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = in.readByte()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = in.readByte()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = in.readByte()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = in.readByte()) << 28;
                        if (tmp < 0) {
                            throw illegalVarintException(result);
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readVarint(ByteBuffer buffer) {
        int value = readUnsignedVarint(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static int readVarint(DataInput in) throws IOException {
        int value = readUnsignedVarint(in);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static long readVarlong(DataInput in) throws IOException {
        long raw = readUnsignedVarlong(in);
        return (raw >>> 1) ^ -(raw & 1);
    }

    private static long readUnsignedVarlong(DataInput in) throws IOException {
        byte tmp = in.readByte();
        if (tmp >= 0) {
            return tmp;
        } else {
            long result = tmp & 0x7f;
            if ((tmp = in.readByte()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 0x7f) << 7;
                if ((tmp = in.readByte()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 0x7f) << 14;
                    if ((tmp = in.readByte()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 0x7f) << 21;
                        result = innerReadUnsignedVarLong(in, result);
                    }
                }
            }
            return result;
        }
    }

    private static long innerReadUnsignedVarLong(DataInput in, long result) throws IOException {
        byte tmp;
        if ((tmp = in.readByte()) >= 0) {
            result |= (long) tmp << 28;
        } else {
            result |= (long) (tmp & 0x7f) << 28;
            if ((tmp = in.readByte()) >= 0) {
                result |= (long) tmp << 35;
            } else {
                result |= (long) (tmp & 0x7f) << 35;
                if ((tmp = in.readByte()) >= 0) {
                    result |= (long) tmp << 42;
                } else {
                    result |= (long) (tmp & 0x7f) << 42;
                    if ((tmp = in.readByte()) >= 0) {
                        result |= (long) tmp << 49;
                    } else {
                        result |= (long) (tmp & 0x7f) << 49;
                        if ((tmp = in.readByte()) >= 0) {
                            result |= (long) tmp << 56;
                        } else {
                            result |= (long) (tmp & 0x7f) << 56;
                            result |= (long) (tmp = in.readByte()) << 63;
                            if (tmp < 0) {
                                throw illegalVarlongException(result);
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     */
    public static long readVarlong(ByteBuffer buffer)  {
        long raw  = readUnsignedVarlong(buffer);
        return (raw >>> 1) ^ -(raw & 1);
    }

    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public static long readUnsignedVarlong(ByteBuffer buffer)  {
        byte tmp = buffer.get();
        if (tmp >= 0) {
            return tmp;
        } else {
            long result = tmp & 0x7f;
            if ((tmp = buffer.get()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 0x7f) << 7;
                if ((tmp = buffer.get()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 0x7f) << 14;
                    if ((tmp = buffer.get()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 0x7f) << 21;
                        if ((tmp = buffer.get()) >= 0) {
                            result |= (long) tmp << 28;
                        } else {
                            result |= (long) (tmp & 0x7f) << 28;
                            if ((tmp = buffer.get()) >= 0) {
                                result |= (long) tmp << 35;
                            } else {
                                result |= (long) (tmp & 0x7f) << 35;
                                if ((tmp = buffer.get()) >= 0) {
                                    result |= (long) tmp << 42;
                                } else {
                                    result |= (long) (tmp & 0x7f) << 42;
                                    if ((tmp = buffer.get()) >= 0) {
                                        result |= (long) tmp << 49;
                                    } else {
                                        result |= (long) (tmp & 0x7f) << 49;
                                        if ((tmp = buffer.get()) >= 0) {
                                            result |= (long) tmp << 56;
                                        } else {
                                            result |= (long) (tmp & 0x7f) << 56;
                                            result |= (long) (tmp = buffer.get()) << 63;
                                            if (tmp < 0) {
                                                throw illegalVarlongException(result);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param in The input to read from
     * @return The double value read
     */
    public static double readDouble(DataInput in) throws IOException {
        return in.readDouble();
    }

    /**
     * Read a double-precision 64-bit format IEEE 754 value.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     */
    public static double readDouble(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeUnsignedVarint(int value, ByteBuffer buffer) {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            buffer.put((byte) value);
        } else {
            buffer.put((byte) (value & 0x7F | 0x80));
            if ((value & (0xFFFFFFFF << 14)) == 0) {
                buffer.put((byte) (value >>> 7));
            } else {
                buffer.put((byte) ((value >>> 7) & 0x7F | 0x80));
                if ((value & (0xFFFFFFFF << 21)) == 0) {
                    buffer.put((byte) (value >>> 14));
                } else {
                    buffer.put((byte) ((value >>> 14) & 0x7F | 0x80));
                    if ((value & (0xFFFFFFFF << 28)) == 0) {
                        buffer.put((byte) (value >>> 21));
                    } else {
                        buffer.put((byte) ((value >>> 21) & 0x7F | 0x80));
                        buffer.put((byte) (value >>> 28));
                    }
                }
            }
        }
    }

    /**
     * Write the given integer following the variable-length unsigned encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     * 
     * For implementation notes, see {@link #writeUnsignedVarint(int, ByteBuffer)}
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeUnsignedVarint(int value, DataOutput out) throws IOException {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            out.writeByte(value);
        } else {
            out.writeByte(value & 0x7F | 0x80);
            if ((value & (0xFFFFFFFF << 14)) == 0) {
                out.writeByte(value >>> 7);
            } else {
                out.writeByte((value >>> 7) & 0x7F | 0x80);
                if ((value & (0xFFFFFFFF << 21)) == 0) {
                    out.writeByte(value >>> 14);
                } else {
                    out.writeByte((byte) ((value >>> 14) & 0x7F | 0x80));
                    if ((value & (0xFFFFFFFF << 28)) == 0) {
                        out.writeByte(value >>> 21);
                    } else {
                        out.writeByte((value >>> 21) & 0x7F | 0x80);
                        out.writeByte(value >>> 28);
                    }
                }
            }
        }
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeVarint(int value, DataOutput out) throws IOException {
        writeUnsignedVarint((value << 1) ^ (value >> 31), out);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeVarint(int value, ByteBuffer buffer) {
        writeUnsignedVarint((value << 1) ^ (value >> 31), buffer);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the output.
     *
     * @param v The value to write
     * @param out The output to write to
     */
    public static void writeVarlong(long v, DataOutput out) throws IOException {
        long value = (v << 1) ^ (v >> 63);
        if(value > 0x7FFFFFFFFFFFFFFFL) out.writeByte((byte)(0x80 | (value >>> 63)));
        if(value > 0xFFFFFFFFFFFFFFL)   out.writeByte((byte)(0x80 | ((value >>> 56) & 0x7FL)));
        if(value > 0x1FFFFFFFFFFFFL)    out.writeByte((byte)(0x80 | ((value >>> 49) & 0x7FL)));
        if(value > 0x3FFFFFFFFFFL)      out.writeByte((byte)(0x80 | ((value >>> 42) & 0x7FL)));
        if(value > 0x7FFFFFFFFL)        out.writeByte((byte)(0x80 | ((value >>> 35) & 0x7FL)));
        if(value > 0xFFFFFFFL)          out.writeByte((byte)(0x80 | ((value >>> 28) & 0x7FL)));
        if(value > 0x1FFFFFL)           out.writeByte((byte)(0x80 | ((value >>> 21) & 0x7FL)));
        if(value > 0x3FFFL)             out.writeByte((byte)(0x80 | ((value >>> 14) & 0x7FL)));
        if(value > 0x7FL)               out.writeByte((byte)(0x80 | ((value >>>  7) & 0x7FL)));

        out.writeByte((byte)(value & 0x7FL));
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        writeUnsignedVarlong(v, buffer);
    }

    /**
     * Based on an implementation in Netflix's Hollow repository. The only difference is to
     * extend the implementation of Unsigned Long instead of signed Long present in Hollow.
     * @see https://github.com/Netflix/hollow/blame/877dd522431ac11808d81d95197d5fd0916bc7b5/hollow/src/main/java/com/netflix/hollow/core/memory/encoding/VarInt.java#L51-L134
     */
    public static void writeUnsignedVarlong(long value, ByteBuffer buffer) {
        if(value > 0x7FFFFFFFFFFFFFFFL) buffer.put((byte)(0x80 | (value >>> 63)));
        if(value > 0xFFFFFFFFFFFFFFL)   buffer.put((byte)(0x80 | ((value >>> 56) & 0x7FL)));
        if(value > 0x1FFFFFFFFFFFFL)    buffer.put((byte)(0x80 | ((value >>> 49) & 0x7FL)));
        if(value > 0x3FFFFFFFFFFL)      buffer.put((byte)(0x80 | ((value >>> 42) & 0x7FL)));
        if(value > 0x7FFFFFFFFL)        buffer.put((byte)(0x80 | ((value >>> 35) & 0x7FL)));
        if(value > 0xFFFFFFFL)          buffer.put((byte)(0x80 | ((value >>> 28) & 0x7FL)));
        if(value > 0x1FFFFFL)           buffer.put((byte)(0x80 | ((value >>> 21) & 0x7FL)));
        if(value > 0x3FFFL)             buffer.put((byte)(0x80 | ((value >>> 14) & 0x7FL)));
        if(value > 0x7FL)               buffer.put((byte)(0x80 | ((value >>>  7) & 0x7FL)));

        buffer.put((byte)(value & 0x7FL));
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeDouble(double value, DataOutput out) throws IOException {
        out.writeDouble(value);
    }

    /**
     * Write the given double following the double-precision 64-bit format IEEE 754 value into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeDouble(double value, ByteBuffer buffer) {
        buffer.putDouble(value);
    }

    /**
     * Number of bytes needed to encode an integer in unsigned variable-length format.
     *
     * @param value The signed value
     *
     * @see #writeUnsignedVarint(int, DataOutput)
     */
    public static int sizeOfUnsignedVarint(int value) {
        // Protocol buffers varint encoding is variable length, with a minimum of 1 byte
        // (for zero). The values themselves are not important. What's important here is
        // any leading zero bits are dropped from output. We can use this leading zero
        // count w/ fast intrinsic to calc the output length directly.

        // Test cases verify this matches the output for loop logic exactly.

        // return (38 - leadingZeros) / 7 + leadingZeros / 32;

        // The above formula provides the implementation, but the Java encoding is suboptimal
        // when we have a narrow range of integers, so we can do better manually

        int leadingZeros = Integer.numberOfLeadingZeros(value);
        int leadingZerosBelow38DividedBy7 = ((38 - leadingZeros) * 0b10010010010010011) >>> 19;
        return leadingZerosBelow38DividedBy7 + (leadingZeros >>> 5);
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarint(int value) {
        return sizeOfUnsignedVarint((value << 1) ^ (value >> 31));
    }

    /**
     * Number of bytes needed to encode a long in variable-length format.
     *
     * @param value The signed value
     * @see #sizeOfUnsignedVarint(int)
     */
    public static int sizeOfVarlong(long value) {
        return sizeOfUnsignedVarlong((value << 1) ^ (value >> 63));
    }

    // visible for benchmarking
    public static int sizeOfUnsignedVarlong(long v) {
        // For implementation notes @see #sizeOfUnsignedVarint(int)
        // Similar logic is applied to allow for 64bit input -> 1-9byte output.
        // return (70 - leadingZeros) / 7 + leadingZeros / 64;

        int leadingZeros = Long.numberOfLeadingZeros(v);
        int leadingZerosBelow70DividedBy7 = ((70 - leadingZeros) * 0b10010010010010011) >>> 19;
        return leadingZerosBelow70DividedBy7 + (leadingZeros >>> 6);
    }

    private static IllegalArgumentException illegalVarintException(int value) {
        throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
                "converted value: " + Integer.toHexString(value));
    }

    private static IllegalArgumentException illegalVarlongException(long value) {
        throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                "converted value: " + Long.toHexString(value));
    }
}
