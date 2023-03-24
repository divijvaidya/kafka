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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.ByteUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class ByteUtilsBenchmark {
    private static final int DATA_SET_SAMPLE_SIZE = 4096;
    private int[] randomInts;
    private long[] randomLongs;
    private Random random;
    private ByteBuffer testBuffer;

    @Param({"1", "3", "5", "7"})
    int lastNonZeroByteLong;

    @Param({"1", "2", "3"})
    int lastNonZeroByteInt;

    @Setup(Level.Trial)
    public void setUpBenchmarkLevel() {
        // Initialize the random number generator with a seed so that for each benchmark it produces the same sequence
        // of random numbers. Note that it is important to initialize it again with the seed before every benchmark.
        random = new Random(1337);
    }

    @Setup(Level.Invocation)
    public void setUpInvocationBuffer() {
        testBuffer = ByteBuffer.allocate(10);
    }

    private static int generateRandomBitNumber(Random rng, int i) {
        int lowerBound = 1 << (i - 1);
        int upperBound = (1 << i) - 1;
        if (lowerBound == upperBound) {
            return lowerBound;
        }
        return lowerBound + rng.nextInt(upperBound - lowerBound);
    }

    private static int getNextRantInt(Random random) {
        return generateRandomBitNumber(random, random.nextInt(30) + 1);
    }

    private static long getNextRantLong(Random random) {
        return generateRandomBitNumberLong(random, random.nextInt(62) + 1);
    }

    private static long generateRandomBitNumberLong(Random rng, int i) {
        long lowerBound = 1L << (i - 1);
        long upperBound = (1L << i) - 1;
        if (lowerBound == upperBound) {
            return lowerBound;
        }
        return lowerBound +
            rng.longs(lowerBound, upperBound).findFirst()
                .orElseThrow(() -> new IllegalStateException("Unable to create a random long in the range=[" + lowerBound + ", " + upperBound + "]"));
    }

    @Setup(Level.Iteration)
    public void setUp() {
        randomInts = new int[DATA_SET_SAMPLE_SIZE];
        for (int i = 0; i < DATA_SET_SAMPLE_SIZE; i++) {
            this.randomInts[i] = getNextRantInt(random) & ((1 << (4 * lastNonZeroByteInt)) - 1);
        }

        randomLongs = new long[DATA_SET_SAMPLE_SIZE];
        for (int i = 0; i < DATA_SET_SAMPLE_SIZE; i++) {
            this.randomLongs[i] = getNextRantLong(random) & ((1L << (4 * lastNonZeroByteLong)) - 1);
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarintNetty(Blackhole bk) {
        for (int randomValue : this.randomInts) {
            ByteUtils.writeUnsignedVarint(randomValue, testBuffer);
            // prepare for reading
            testBuffer.flip();
            bk.consume(ByteUtils.readUnsignedVarint(testBuffer));
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarintTrunk(Blackhole bk) {
        for (int randomValue : this.randomInts) {
            ByteUtils.writeUnsignedVarint(randomValue, testBuffer);
            // prepare for reading
            testBuffer.flip();
            bk.consume(ByteUtils.readUnsignedVarintTrunk(testBuffer));
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarintProtobuf(Blackhole bk) {
        for (int randomValue : this.randomInts) {
            ByteUtils.writeUnsignedVarint(randomValue, testBuffer);
            // prepare for reading
            testBuffer.flip();
            bk.consume(ByteUtils.readUnsignedVarintProtoBuf(testBuffer));
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarlongUnrolled(Blackhole bk) {
        for (long randomValue : this.randomLongs) {
            ByteUtils.writeUnsignedVarlong(randomValue, testBuffer);
            // prepare for reading
            testBuffer.flip();
            bk.consume(ByteUtils.readUnsignedVarlong(testBuffer));
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarlongTrunk(Blackhole bk) {
        for (long randomValue : this.randomLongs) {
            ByteUtils.writeUnsignedVarlong(randomValue, testBuffer);
            // prepare for reading
            testBuffer.flip();
            bk.consume(ByteUtils.readUnsignedVarlongTrunk(testBuffer));
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarintUnrolled() {
        for (int randomValue : this.randomInts) {
            ByteUtils.writeUnsignedVarintUnrolled(randomValue, testBuffer);
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarintTrunk() {
        for (int randomValue : this.randomInts) {
            ByteUtils.writeUnsignedVarintTrunk(randomValue, testBuffer);
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarintHollow() {
        for (int randomValue : this.randomInts) {
            ByteUtils.writeUnsignedVarintHollow(randomValue, testBuffer);
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarlongUnrolled() {
        for (long randomValue : this.randomLongs) {
            ByteUtils.writeUnsignedVarlongUnrolled(randomValue, testBuffer);
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarlongTrunk() {
        for (long randomValue : this.randomLongs) {
            ByteUtils.writeUnsignedVarlongTrunk(randomValue, testBuffer);
            testBuffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedWriteVarlongHollow() {
        for (long randomValue : this.randomLongs) {
            ByteUtils.writeUnsignedVarlongHollow(randomValue, testBuffer);
            testBuffer.clear();
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testSizeOfUnsignedVarint(Blackhole bk) {
        for (int randomValue : this.randomInts) {
            bk.consume(ByteUtils.sizeOfUnsignedVarint(randomValue));
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testSizeOfUnsignedVarintSimple(Blackhole bk) {
        for (int randomValue : this.randomInts) {
            int value = randomValue;
            int bytes = 1;
            while ((value & 0xffffff80) != 0L) {
                bytes += 1;
                value >>>= 7;
            }
            bk.consume(bytes);
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testSizeOfUnsignedVarlong(Blackhole bk) {
        for (long randomValue : this.randomLongs) {
            bk.consume(ByteUtils.sizeOfUnsignedVarlong(randomValue));
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteUtilsBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }
}
