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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types._

/**
 * Selects the optimal compression codec for a shuffle exchange based on output schema
 * characteristics. String-dominant schemas benefit from higher-ratio codecs (ZSTD),
 * while numeric-dominant schemas benefit from lower-latency codecs (LZ4).
 */
private[sql] object AdaptiveCompressionSelector {

  // Threshold: if string/binary bytes ratio exceeds this, use ZSTD
  private val STRING_DOMINANT_THRESHOLD = 0.6

  /**
   * Selects the best compression codec name based on the output schema.
   *
   * Strategy:
   *  - Estimate the average byte width of each column based on its data type
   *  - If string/binary columns account for >= 60% of estimated row bytes, use ZSTD
   *    (higher compression ratio benefits large compressible data)
   *  - Otherwise, use LZ4 (lower latency benefits numeric/fixed-width data)
   *
   * @param outputAttributes the output schema of the shuffle exchange
   * @return the codec name ("zstd" or "lz4")
   */
  def selectCodec(outputAttributes: Seq[Attribute]): String = {
    if (outputAttributes.isEmpty) {
      return CompressionCodec.LZ4
    }

    var stringBytes = 0L
    var totalBytes = 0L

    outputAttributes.foreach { attr =>
      val estimatedWidth = estimateAvgWidth(attr.dataType)
      totalBytes += estimatedWidth
      if (isStringLike(attr.dataType)) {
        stringBytes += estimatedWidth
      }
    }

    if (totalBytes > 0 && stringBytes.toDouble / totalBytes >= STRING_DOMINANT_THRESHOLD) {
      CompressionCodec.ZSTD
    } else {
      CompressionCodec.LZ4
    }
  }

  private def isStringLike(dataType: DataType): Boolean = dataType match {
    case _: StringType => true
    case BinaryType => true
    case _ => false
  }

  /**
   * Estimates the average byte width of a column based on its data type.
   * For variable-length types, uses a conservative heuristic estimate.
   */
  private[exchange] def estimateAvgWidth(dataType: DataType): Long = dataType match {
    case BooleanType => 1
    case ByteType => 1
    case ShortType => 2
    case IntegerType | FloatType | DateType => 4
    case LongType | DoubleType | TimestampType | TimestampNTZType => 8
    case _: DecimalType => 16
    case _: StringType => 64  // heuristic average for string columns
    case BinaryType => 64     // heuristic average for binary columns
    case _: ArrayType => 64
    case _: MapType => 128
    case s: StructType => s.fields.map(f => estimateAvgWidth(f.dataType)).sum
    case _ => 8 // conservative default
  }
}
