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

import org.apache.spark.SparkFunSuite
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types._

class AdaptiveCompressionSelectorSuite extends SparkFunSuite {

  private def attr(name: String, dt: DataType): AttributeReference =
    AttributeReference(name, dt)()

  test("empty schema defaults to LZ4") {
    assert(AdaptiveCompressionSelector.selectCodec(Seq.empty) === CompressionCodec.LZ4)
  }

  test("all-numeric schema selects LZ4") {
    val attrs = Seq(
      attr("id", LongType),
      attr("value", DoubleType),
      attr("count", IntegerType),
      attr("flag", BooleanType)
    )
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.LZ4)
  }

  test("string-dominant schema selects ZSTD") {
    // String columns: name (64) + description (64) + address (64) = 192
    // Numeric columns: id (8) = 8
    // String ratio: 192/200 = 96% > 60%
    val attrs = Seq(
      attr("id", LongType),
      attr("name", StringType),
      attr("description", StringType),
      attr("address", StringType)
    )
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.ZSTD)
  }

  test("mixed schema with string minority selects LZ4") {
    // String: name (64) = 64
    // Numeric: id(8) + v1(8) + v2(8) + v3(8) + v4(4) + v5(4) + v6(4) + v7(4) = 48
    // plus d1(16) + d2(16) = 32  => total numeric = 80
    // String ratio: 64/144 = 44% < 60%
    val attrs = Seq(
      attr("id", LongType),
      attr("name", StringType),
      attr("v1", DoubleType),
      attr("v2", DoubleType),
      attr("v3", LongType),
      attr("v4", IntegerType),
      attr("v5", FloatType),
      attr("v6", DateType),
      attr("v7", IntegerType),
      attr("d1", DecimalType(18, 2)),
      attr("d2", DecimalType(10, 0))
    )
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.LZ4)
  }

  test("binary columns count as string-like") {
    val attrs = Seq(
      attr("id", IntegerType),   // 4
      attr("data", BinaryType),  // 64
      attr("blob", BinaryType)   // 64
    )
    // Binary ratio: 128/132 = 97% > 60%
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.ZSTD)
  }

  test("single string column selects ZSTD") {
    val attrs = Seq(attr("text", StringType))
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.ZSTD)
  }

  test("single numeric column selects LZ4") {
    val attrs = Seq(attr("id", LongType))
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.LZ4)
  }

  test("struct type estimates recursively") {
    val structType = StructType(Seq(
      StructField("x", DoubleType),
      StructField("y", DoubleType),
      StructField("z", DoubleType)
    ))
    // Struct = 24 bytes (3 * 8), which is numeric
    assert(AdaptiveCompressionSelector.estimateAvgWidth(structType) === 24)
  }

  test("nested struct with strings increases string ratio") {
    val structType = StructType(Seq(
      StructField("name", StringType),   // 64
      StructField("addr", StringType)    // 64
    ))
    val attrs = Seq(
      attr("id", IntegerType),  // 4
      AttributeReference("info", structType)()  // 128 (all string)
    )
    // info is a struct but not string-like itself, so only top-level types matter
    // id=4, info=128 (struct width), no string-like top-level
    // Actually info's dataType is StructType, not StringType, so it's not string-like
    // String ratio: 0/132 = 0% → LZ4
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.LZ4)
  }

  test("exact threshold boundary - at 60% selects ZSTD") {
    // We need string ratio >= 0.6
    // 3 strings (192) + 1 binary(64) would be too much.
    // Let's build: string=64*3=192, numeric need to be <=128
    // 192 / (192+128) = 192/320 = 0.6 exactly → ZSTD
    val attrs = Seq(
      attr("s1", StringType),   // 64
      attr("s2", StringType),   // 64
      attr("s3", StringType),   // 64
      attr("n1", LongType),     // 8
      attr("n2", LongType),     // 8
      attr("n3", LongType),     // 8
      attr("n4", LongType),     // 8
      attr("n5", LongType),     // 8
      attr("n6", LongType),     // 8
      attr("n7", LongType),     // 8
      attr("n8", LongType),     // 8
      attr("n9", LongType),     // 8
      attr("n10", LongType),    // 8
      attr("n11", LongType),    // 8
      attr("n12", LongType),    // 8
      attr("n13", LongType),    // 8
      attr("n14", LongType),    // 8
      attr("n15", LongType),    // 8
      attr("n16", LongType)     // 8
    )
    // String: 192, Numeric: 128, Total: 320, Ratio: 0.6
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.ZSTD)
  }

  test("just below threshold selects LZ4") {
    val attrs = Seq(
      attr("s1", StringType),   // 64
      attr("s2", StringType),   // 64
      attr("s3", StringType),   // 64
      attr("n1", LongType),     // 8
      attr("n2", LongType),     // 8
      attr("n3", LongType),     // 8
      attr("n4", LongType),     // 8
      attr("n5", LongType),     // 8
      attr("n6", LongType),     // 8
      attr("n7", LongType),     // 8
      attr("n8", LongType),     // 8
      attr("n9", LongType),     // 8
      attr("n10", LongType),    // 8
      attr("n11", LongType),    // 8
      attr("n12", LongType),    // 8
      attr("n13", LongType),    // 8
      attr("n14", LongType),    // 8
      attr("n15", LongType),    // 8
      attr("n16", LongType),    // 8
      attr("n17", LongType)     // 8 -- this tips it below 60%
    )
    // String: 192, Numeric: 136, Total: 328, Ratio: 192/328 = 0.585 < 0.6
    assert(AdaptiveCompressionSelector.selectCodec(attrs) === CompressionCodec.LZ4)
  }

  test("estimateAvgWidth covers all common types") {
    assert(AdaptiveCompressionSelector.estimateAvgWidth(BooleanType) === 1)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(ByteType) === 1)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(ShortType) === 2)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(IntegerType) === 4)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(FloatType) === 4)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(DateType) === 4)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(LongType) === 8)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(DoubleType) === 8)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(TimestampType) === 8)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(DecimalType(18, 2)) === 16)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(StringType) === 64)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(BinaryType) === 64)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(ArrayType(IntegerType)) === 64)
    assert(AdaptiveCompressionSelector.estimateAvgWidth(MapType(StringType, IntegerType)) === 128)
  }
}
