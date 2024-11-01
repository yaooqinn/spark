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

import scala.util.Random

import org.apache.spark.benchmark.Benchmark

/**
 * Benchmark for encode
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/SHABenchmark-results.txt".
 * }}}
 */
object SHABenchmark extends SqlBasedBenchmark {
  import spark.implicits._
  private val N = 1024 * 8

  private def generateExpr(
      foldable: Boolean,
      nullable: Boolean = true,
      nested: Boolean = false): Seq[String] = {
    var bitLengths = Seq(0, 224, 256, 384, 512)
    if (nullable) {
      bitLengths ++= Seq(513)
    }
    if (nested) {
      (1 to 5).map { i =>
        bitLengths.foldLeft[String](s"_$i") { (expr, j) =>
          s"sha2($expr, ${if (foldable) "0" else "_6"} + $j)"
        }
      }
    } else {
      (1 to 5).flatMap { i =>
        bitLengths.map { j =>
          s"sha2(_$i, ${if (foldable) "0" else "_6"} + $j)"
        }
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath { path =>
      spark.range(N).map { i =>
        val bytes = Random.nextBytes(i.toInt)
        (bytes, bytes, bytes, bytes, bytes,
          0 // a dummy column to mock un-foldable expressions
        )
      }.write.parquet(path.getCanonicalPath)

      def addBenchmarkCase(
          foldable: Boolean,
          nullable: Boolean,
          nested: Boolean,
          benchmark: Benchmark): Unit = {
        val exprs = generateExpr(foldable, nullable, nested)
        benchmark.addCase(s"Sha2 w/ foldable=$foldable, nullable=$nullable, nested=$nested") {
          _ => spark.read.parquet(path.getCanonicalPath).selectExpr(exprs: _*).noop()
        }
      }
      val benchmark = new Benchmark("sha2 case 1", N, output = output)
      addBenchmarkCase(foldable = false, nullable = true, nested = false, benchmark)
      addBenchmarkCase(foldable = false, nullable = false, nested = false, benchmark)
      addBenchmarkCase(foldable = true, nullable = true, nested = false, benchmark)
      addBenchmarkCase(foldable = true, nullable = false, nested = false, benchmark)
      benchmark.run()

      val benchmark2 = new Benchmark("sha2 case 2", N, output = output)
      addBenchmarkCase(foldable = false, nullable = false, nested = true, benchmark2)
      addBenchmarkCase(foldable = false, nullable = true, nested = true, benchmark2)
      addBenchmarkCase(foldable = true, nullable = false, nested = true, benchmark2)
      addBenchmarkCase(foldable = true, nullable = true, nested = true, benchmark2)
      benchmark2.run()
    }
  }
}
