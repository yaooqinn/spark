/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.sketch.BloomFilter

/**
 * Benchmark for Checksum Algorithms used by shuffle.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/BloomFilterHashStrategyBenchmark-results.txt".
 * }}}
 */
object BloomFilterHashStrategyBenchmark extends BenchmarkBase {

  val N = 256 * 1024 * 1024

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("BloomFilter Hash Algorithms") {
      val benchmark = new Benchmark("BloomFilter Hash Algorithms", N, 3, output = output)
      Seq(1, 2, 4, 8, 16).reverse.foreach { v =>
        val numberOfBits = 1024 * 1024 * v
        Seq(1, 2).foreach { version =>
          benchmark.addCase(s"NDV: $N, size: $numberOfBits bits, version: $version") { _ =>
            val bf = BloomFilter.create(N, numberOfBits, version)
            (1 to N).foreach { i =>
              bf.putLong(i)
            }
          }
        }
      }
      benchmark.run()
    }
  }
}
