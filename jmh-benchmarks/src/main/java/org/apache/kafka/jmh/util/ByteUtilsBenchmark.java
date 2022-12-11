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

package org.apache.kafka.jmh.util;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.utils.ByteUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class ByteUtilsBenchmark {
    private int inputInt;
    private long inputLong;
    private int[] numbers;
    @Setup
    public void setUp() {
        Random random = new Random(77083993792645L);
        this.numbers = new int[2048];
        for (int i = 0; i < 2048; i++) {
            this.numbers[i] = generateRandomBitNumber(random, random.nextInt(30) + 1);
        }
    }

    private static int generateRandomBitNumber(Random random, int i) {
        int lowerBound = (1 << (i - 1));
        int upperBound = (1 << i) - 1;
        if (lowerBound == upperBound) {
            return lowerBound;
        }
        return lowerBound + random.nextInt(upperBound - lowerBound);
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void writeUnsignedVarint() {
        ByteBuffer buf = ByteBuffer.allocate(70000);
        ByteBufferAccessor acc = new ByteBufferAccessor(buf);
        for (int number : numbers) {
            ByteUtils.writeUnsignedVarint(number, acc);
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testwriteUnsignedVarintNew() {
        ByteBuffer buf = ByteBuffer.allocate(70000);
        ByteBufferAccessor acc = new ByteBufferAccessor(buf);
        for (int number : numbers) {
            ByteUtils.writeUnsignedVarintNew(number, acc);
        }
    }

//    @Benchmark
//    public int testSizeOfUnsignedVarint() {
//        return ByteUtils.sizeOfUnsignedVarint(inputInt);
//    }
//
//    @Benchmark
//    public int testSizeOfUnsignedVarintSimple() {
//        int value = inputInt;
//        int bytes = 1;
//        while ((value & 0xffffff80) != 0L) {
//            bytes += 1;
//            value >>>= 7;
//        }
//        return bytes;
//    }
//
//    @Benchmark
//    public int testSizeOfVarlong() {
//        return ByteUtils.sizeOfVarlong(inputLong);
//    }
//
//    @Benchmark
//    public int testSizeOfVarlongSimple() {
//        long v = (inputLong << 1) ^ (inputLong >> 63);
//        int bytes = 1;
//        while ((v & 0xffffffffffffff80L) != 0L) {
//            bytes += 1;
//            v >>>= 7;
//        }
//        return bytes;
//    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteUtilsBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }
}
