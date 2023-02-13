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

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

/**
 * The ChunkedDataInput interface provides for reading bytes from an underlying source. The source could be a buffer
 * or a stream. It extends the {@link Closeable} interface to ensure that the source is appropriately closed (if required).
 */
public interface ChunkedDataInput extends Closeable {
    /**
     * The interface is based on {@link InputStream#read()} and follows it's contract.
     */
    int read() throws IOException;

    /**
     * The interface is based on {@link DataInput#skipBytes(int)} and follows it's contract.
     */
    int skipBytes(int toSkip) throws IOException;

    /**
     * The interface is based on {@link InputStream#read(byte[], int, int)} and follows it's contract.
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * The interface is based on {@link DataInput#readByte()} and follows it's contract.
     */
    byte readByte() throws IOException;
}