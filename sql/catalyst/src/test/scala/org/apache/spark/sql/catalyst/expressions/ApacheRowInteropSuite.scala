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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * Integration tests for ApacheRow interoperability with other row formats.
 */
class ApacheRowInteropSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("ApacheRow to UnsafeRow conversion") {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", LongType),
      StructField("c", StringType)
    ))

    val apacheRow = ApacheRow(42, 100L, UTF8String.fromString("test"))

    // Convert to UnsafeRow using UnsafeProjection
    val converter = UnsafeProjection.create(schema)
    val unsafeRow = converter.apply(apacheRow)

    // Verify values are preserved
    assert(unsafeRow.getInt(0) === 42)
    assert(unsafeRow.getLong(1) === 100L)
    assert(unsafeRow.getString(2) === "test")
  }

  test("UnsafeRow to ApacheRow conversion") {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", LongType),
      StructField("c", StringType)
    ))

    // Create UnsafeRow
    val genericRow = new GenericInternalRow(
      Array[Any](42, 100L, UTF8String.fromString("test"))
    )
    val converter = UnsafeProjection.create(schema)
    val unsafeRow = converter.apply(genericRow)

    // Convert to ApacheRow
    val apacheRow = ApacheRowConverter.toApacheRow(unsafeRow, schema)

    // Verify values are preserved
    assert(apacheRow.getInt(0) === 42)
    assert(apacheRow.getLong(1) === 100L)
    assert(apacheRow.getUTF8String(2) === UTF8String.fromString("test"))
  }

  test("ApacheRow roundtrip through UnsafeRow") {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", DoubleType),
      StructField("c", StringType),
      StructField("d", BooleanType)
    ))

    val original = ApacheRow(
      123,
      45.67,
      UTF8String.fromString("roundtrip"),
      true
    )

    // Convert to UnsafeRow
    val toUnsafe = UnsafeProjection.create(schema)
    val unsafeRow = toUnsafe.apply(original)

    // Convert back to ApacheRow
    val result = ApacheRowConverter.toApacheRow(unsafeRow, schema)

    // Verify values are preserved
    assert(result.getInt(0) === 123)
    assert(result.getDouble(1) === 45.67)
    assert(result.getUTF8String(2) === UTF8String.fromString("roundtrip"))
    assert(result.getBoolean(3) === true)
  }

  test("ApacheRow with null values to UnsafeRow") {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", StringType),
      StructField("c", DoubleType)
    ))

    val apacheRow = new ApacheRow(3)
    apacheRow.setInt(0, 42)
    apacheRow.setNullAt(1)
    apacheRow.setDouble(2, 3.14)

    val converter = UnsafeProjection.create(schema)
    val unsafeRow = converter.apply(apacheRow)

    assert(unsafeRow.getInt(0) === 42)
    assert(unsafeRow.isNullAt(1))
    assert(unsafeRow.getDouble(2) === 3.14)
  }

  test("ApacheRow equality with GenericInternalRow") {
    val apacheRow = ApacheRow(1, 2L, 3.0)
    val genericRow = new GenericInternalRow(Array[Any](1, 2L, 3.0))

    // They should be equal as both extend BaseGenericInternalRow
    assert(apacheRow === genericRow)
    assert(genericRow === apacheRow)
  }

  test("ApacheRow interop with SpecificInternalRow") {
    val fieldTypes = Array[DataType](IntegerType, LongType, DoubleType)
    val specificRow = new SpecificInternalRow(fieldTypes.toImmutableArraySeq)
    specificRow.setInt(0, 100)
    specificRow.setLong(1, 200L)
    specificRow.setDouble(2, 300.0)

    // Convert to ApacheRow
    val apacheRow = ApacheRow.fromInternalRow(specificRow, fieldTypes.toSeq)

    assert(apacheRow.getInt(0) === 100)
    assert(apacheRow.getLong(1) === 200L)
    assert(apacheRow.getDouble(2) === 300.0)
  }

  test("ApacheRow can be used in joins") {
    val apacheRow1 = ApacheRow(1, UTF8String.fromString("left"))
    val apacheRow2 = ApacheRow(2, UTF8String.fromString("right"))

    // Use JoinedRow to combine them
    val joined = new JoinedRow(apacheRow1, apacheRow2)

    assert(joined.numFields === 4)
    assert(joined.getInt(0) === 1)
    assert(joined.getUTF8String(1) === UTF8String.fromString("left"))
    assert(joined.getInt(2) === 2)
    assert(joined.getUTF8String(3) === UTF8String.fromString("right"))
  }

  test("ApacheRow performance for updates vs UnsafeRow") {
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", LongType),
      StructField("c", DoubleType)
    ))

    // ApacheRow allows easy updates
    val apacheRow = new ApacheRow(3)
    apacheRow.setInt(0, 1)
    apacheRow.setLong(1, 2L)
    apacheRow.setDouble(2, 3.0)

    // Update multiple times
    for (i <- 1 to 100) {
      apacheRow.setInt(0, i)
      apacheRow.setLong(1, i * 2L)
      apacheRow.setDouble(2, i * 3.0)
    }

    assert(apacheRow.getInt(0) === 100)
    assert(apacheRow.getLong(1) === 200L)
    assert(apacheRow.getDouble(2) === 300.0)

    // UnsafeRow has limited update support
    val genericRow = new GenericInternalRow(Array[Any](1, 2L, 3.0))
    val converter = UnsafeProjection.create(schema)
    val unsafeRow = converter.apply(genericRow)

    // Can update primitive fields
    unsafeRow.setInt(0, 100)
    unsafeRow.setLong(1, 200L)
    unsafeRow.setDouble(2, 300.0)

    assert(unsafeRow.getInt(0) === 100)
    assert(unsafeRow.getLong(1) === 200L)
    assert(unsafeRow.getDouble(2) === 300.0)
  }

  test("ApacheRow from InternalRow identity") {
    val apacheRow = ApacheRow(1, 2L, 3.0)
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", LongType),
      StructField("c", DoubleType)
    ))

    // Converting ApacheRow to ApacheRow should return same instance
    val result = ApacheRow.fromInternalRow(apacheRow, schema)
    assert(result eq apacheRow) // Same object reference
  }
}
