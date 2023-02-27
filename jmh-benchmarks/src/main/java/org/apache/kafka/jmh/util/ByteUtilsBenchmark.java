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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class ByteUtilsBenchmark {
    static final int DATA_SET_SAMPLE_SIZE = 20480;
    int[] random_ints;
    long[] random_longs;
    Random random;

    @Setup(Level.Trial)
    public void setUpBenchmarkLevel() {
        // Initialize the random number generator with a seed so that for each benchmark it produces the same sequence
        // of random numbers. Note that it is important to initialize it again with the seed before every benchmark.
        random = new Random(1337);
    }

    private static int generateRandomBitNumber(Random random, int i) {
        int lowerBound = (1 << (i - 1));
        int upperBound = (1 << i) - 1;
        if (lowerBound == upperBound) {
            return lowerBound;
        }
        return lowerBound + random.nextInt(upperBound - lowerBound);
    }

    @Setup(Level.Iteration)
    public void setUp() {
        random_ints = new int[DATA_SET_SAMPLE_SIZE];
        for (int i = 0; i < DATA_SET_SAMPLE_SIZE; i++) {
            this.random_ints[i] = generateRandomBitNumber(random, random.nextInt(30) + 1);
        }
        random_longs = random.longs(DATA_SET_SAMPLE_SIZE).toArray();
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarint() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
        for (int random_value : this.random_ints) {
            ByteUtils.writeUnsignedVarint(random_value, buffer);
            ByteUtils.readUnsignedVarint(buffer);
            buffer.clear();
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void testUnsignedReadVarintNew() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
        for (int random_value : this.random_ints) {
            ByteUtils.writeUnsignedVarint(random_value, buffer);
            ByteUtils.readUnsignedVarintNew(buffer);
            buffer.clear();
        }
    }

    public void testSizeOfUnsignedVarint() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
        for (int random_value : this.random_ints) {
            ByteUtils.writeUnsignedVarint(random_value, buffer);
            buffer.clear();
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
