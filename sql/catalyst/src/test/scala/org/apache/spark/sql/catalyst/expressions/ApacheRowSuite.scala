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

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.ArrayImplicits._

class ApacheRowSuite extends SparkFunSuite {

  test("create ApacheRow with primitive types") {
    val row = new ApacheRow(3)
    row.setInt(0, 1)
    row.setLong(1, 2L)
    row.setDouble(2, 3.0)

    assert(row.numFields === 3)
    assert(row.getInt(0) === 1)
    assert(row.getLong(1) === 2L)
    assert(row.getDouble(2) === 3.0)
  }

  test("create ApacheRow from values") {
    val row = ApacheRow(1, 2L, 3.0f, 4.0, true, 5.toByte, 6.toShort)

    assert(row.numFields === 7)
    assert(row.getInt(0) === 1)
    assert(row.getLong(1) === 2L)
    assert(row.getFloat(2) === 3.0f)
    assert(row.getDouble(3) === 4.0)
    assert(row.getBoolean(4) === true)
    assert(row.getByte(5) === 5.toByte)
    assert(row.getShort(6) === 6.toShort)
  }

  test("create ApacheRow from sequence") {
    val values = Seq(1, 2L, "hello")
    val row = ApacheRow.fromSeq(values)

    assert(row.numFields === 3)
    assert(row.getInt(0) === 1)
    assert(row.getLong(1) === 2L)
  }

  test("empty ApacheRow") {
    val row = ApacheRow.empty
    assert(row.numFields === 0)
  }

  test("null handling") {
    val row = new ApacheRow(3)
    row.setNullAt(0)
    row.setInt(1, 42)
    row.setNullAt(2)

    assert(row.isNullAt(0) === true)
    assert(row.isNullAt(1) === false)
    assert(row.isNullAt(2) === true)
    assert(row.getInt(1) === 42)
  }

  test("string and binary handling") {
    val row = new ApacheRow(2)
    val str = UTF8String.fromString("Hello World")
    val bytes = "binary data".getBytes(StandardCharsets.UTF_8)

    row.update(0, str)
    row.update(1, bytes)

    assert(row.getUTF8String(0) === str)
    assert(row.getBinary(1) === bytes)
  }

  test("complex types - array") {
    val row = new ApacheRow(1)
    val arrayData = new GenericArrayData(Array(1, 2, 3))
    row.update(0, arrayData)

    val retrieved = row.getArray(0)
    assert(retrieved.numElements() === 3)
    assert(retrieved.getInt(0) === 1)
    assert(retrieved.getInt(1) === 2)
    assert(retrieved.getInt(2) === 3)
  }

  test("complex types - struct") {
    val innerRow = new ApacheRow(2)
    innerRow.setInt(0, 100)
    innerRow.update(1, UTF8String.fromString("nested"))

    val row = new ApacheRow(1)
    row.update(0, innerRow)

    val retrieved = row.getStruct(0, 2)
    assert(retrieved.getInt(0) === 100)
    assert(retrieved.getUTF8String(1) === UTF8String.fromString("nested"))
  }

  test("copy creates independent copy") {
    val original = new ApacheRow(3)
    original.setInt(0, 1)
    original.setLong(1, 2L)
    original.setDouble(2, 3.0)

    val copy = original.copy()

    // Verify copy has same values
    assert(copy.getInt(0) === 1)
    assert(copy.getLong(1) === 2L)
    assert(copy.getDouble(2) === 3.0)

    // Mutate original
    original.setInt(0, 100)

    // Verify copy is independent
    assert(copy.getInt(0) === 1)
    assert(original.getInt(0) === 100)
  }

  test("copy with string creates independent copy") {
    val original = new ApacheRow(1)
    original.update(0, UTF8String.fromString("original"))

    val copy = original.copy()

    // Mutate original string
    original.update(0, UTF8String.fromString("modified"))

    // Verify copy retained original value
    assert(copy.getUTF8String(0) === UTF8String.fromString("original"))
    assert(original.getUTF8String(0) === UTF8String.fromString("modified"))
  }

  test("equals and hashCode") {
    val row1 = ApacheRow(1, 2L, 3.0)
    val row2 = ApacheRow(1, 2L, 3.0)
    val row3 = ApacheRow(1, 2L, 4.0)

    assert(row1 === row2)
    assert(row1.hashCode === row2.hashCode)
    assert(row1 !== row3)
  }

  test("equals with null values") {
    val row1 = new ApacheRow(3)
    row1.setNullAt(0)
    row1.setInt(1, 1)
    row1.setNullAt(2)

    val row2 = new ApacheRow(3)
    row2.setNullAt(0)
    row2.setInt(1, 1)
    row2.setNullAt(2)

    assert(row1 === row2)
    assert(row1.hashCode === row2.hashCode)
  }

  test("toString representation") {
    val row = ApacheRow(1, 2L, 3.0)
    val str = row.toString

    assert(str.contains("ApacheRow"))
    assert(str.contains("1"))
    assert(str.contains("2"))
    assert(str.contains("3.0"))
  }

  test("empty row toString") {
    val row = ApacheRow.empty
    val str = row.toString

    assert(str.contains("ApacheRow"))
    assert(str.contains("empty"))
  }

  test("update method") {
    val row = new ApacheRow(2)
    row.update(0, 42)
    row.update(1, UTF8String.fromString("test"))

    assert(row.getInt(0) === 42)
    assert(row.getUTF8String(1) === UTF8String.fromString("test"))
  }

  test("toSeq conversion") {
    val row = ApacheRow(1, 2L, 3.0)
    val fieldTypes = Seq(IntegerType, LongType, DoubleType)
    val seq = row.toSeq(fieldTypes)

    assert(seq.length === 3)
    assert(seq(0) === 1)
    assert(seq(1) === 2L)
    assert(seq(2) === 3.0)
  }

  test("fromInternalRow with GenericInternalRow") {
    val genericRow = new GenericInternalRow(Array[Any](1, 2L, UTF8String.fromString("test")))
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", LongType),
      StructField("c", StringType)
    ))

    val apacheRow = ApacheRow.fromInternalRow(genericRow, schema)

    assert(apacheRow.numFields === 3)
    assert(apacheRow.getInt(0) === 1)
    assert(apacheRow.getLong(1) === 2L)
    assert(apacheRow.getUTF8String(2) === UTF8String.fromString("test"))
  }

  test("fromInternalRow with ApacheRow returns same instance") {
    val original = ApacheRow(1, 2L, 3.0)
    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", LongType),
      StructField("c", DoubleType)
    ))

    val result = ApacheRow.fromInternalRow(original, schema)

    assert(result eq original) // Same instance
  }

  test("fromInternalRow with field types") {
    val genericRow = new GenericInternalRow(Array[Any](1, 2L, 3.0))
    val fieldTypes = Seq(IntegerType, LongType, DoubleType)

    val apacheRow = ApacheRow.fromInternalRow(genericRow, fieldTypes)

    assert(apacheRow.numFields === 3)
    assert(apacheRow.getInt(0) === 1)
    assert(apacheRow.getLong(1) === 2L)
    assert(apacheRow.getDouble(2) === 3.0)
  }

  test("fromInternalRow handles null values") {
    val genericRow = new GenericInternalRow(Array[Any](null, 2L, null))
    val fieldTypes = Seq(IntegerType, LongType, DoubleType)

    val apacheRow = ApacheRow.fromInternalRow(genericRow, fieldTypes)

    assert(apacheRow.isNullAt(0) === true)
    assert(apacheRow.getLong(1) === 2L)
    assert(apacheRow.isNullAt(2) === true)
  }

  test("decimal type support") {
    val row = new ApacheRow(1)
    val decimal = Decimal(123.456)
    row.setDecimal(0, decimal, 10)

    assert(row.getDecimal(0, 10, 3) === decimal)
  }

  test("calendar interval support") {
    val row = new ApacheRow(1)
    val interval = new CalendarInterval(12, 30, 1000000L)
    row.setInterval(0, interval)

    assert(row.getInterval(0) === interval)
  }

  test("anyNull with no nulls") {
    val row = ApacheRow(1, 2L, 3.0)
    assert(row.anyNull === false)
  }

  test("anyNull with nulls") {
    val row = new ApacheRow(3)
    row.setInt(0, 1)
    row.setNullAt(1)
    row.setDouble(2, 3.0)

    assert(row.anyNull === true)
  }

  test("specialized setters") {
    val row = new ApacheRow(7)

    row.setBoolean(0, true)
    row.setByte(1, 1.toByte)
    row.setShort(2, 2.toShort)
    row.setInt(3, 3)
    row.setLong(4, 4L)
    row.setFloat(5, 5.0f)
    row.setDouble(6, 6.0)

    assert(row.getBoolean(0) === true)
    assert(row.getByte(1) === 1.toByte)
    assert(row.getShort(2) === 2.toShort)
    assert(row.getInt(3) === 3)
    assert(row.getLong(4) === 4L)
    assert(row.getFloat(5) === 5.0f)
    assert(row.getDouble(6) === 6.0)
  }
}
