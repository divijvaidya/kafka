package org.apache.kafka.jmh.scala

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.collection.mutable
object CollectionsBenchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 10)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def AppendListBuffer(blackHole: Blackhole): Unit = {
    val list = new mutable.ListBuffer[String]
    val size = 1000
    for (n <- 1 to size) {
      list+= s"blah$n"
    }

    blackHole.consume(list)
  }

  @Benchmark
  def main(args: Array[String]): Unit = {
    org.openjdk.jmh.Main.main(args)
  }
}
