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

package org.apache.spark.sql

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ArrowRow, GenericInternalRow}
import org.apache.spark.sql.catalyst.types.PhysicalDecimalType
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.unsafe.types.UTF8String

class ArrowRowSuite extends SparkFunSuite {

  val allocator = new RootAllocator(Long.MaxValue)

  override def afterAll(): Unit = {
    allocator.close()
    super.afterAll()
  }

  test("ArrowRow with primitive types") {
    val schema = new StructType()
      .add("bool", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)

    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(3)
    
    // Allocate vectors
    val boolVector = root.getVector(0).asInstanceOf[BitVector]
    val byteVector = root.getVector(1).asInstanceOf[TinyIntVector]
    val shortVector = root.getVector(2).asInstanceOf[SmallIntVector]
    val intVector = root.getVector(3).asInstanceOf[IntVector]
    val longVector = root.getVector(4).asInstanceOf[BigIntVector]
    val floatVector = root.getVector(5).asInstanceOf[Float4Vector]
    val doubleVector = root.getVector(6).asInstanceOf[Float8Vector]
    
    boolVector.allocateNew()
    byteVector.allocateNew()
    shortVector.allocateNew()
    intVector.allocateNew()
    longVector.allocateNew()
    floatVector.allocateNew()
    doubleVector.allocateNew()
    
    // Set values for row 0
    boolVector.set(0, 1)
    byteVector.set(0, 1.toByte)
    shortVector.set(0, 2.toShort)
    intVector.set(0, 3)
    longVector.set(0, 4L)
    floatVector.set(0, 5.5f)
    doubleVector.set(0, 6.6)
    
    // Set values for row 1 (with nulls)
    boolVector.setNull(1)
    byteVector.set(1, 10.toByte)
    shortVector.setNull(1)
    intVector.set(1, 30)
    longVector.setNull(1)
    floatVector.set(1, 50.5f)
    doubleVector.setNull(1)
    
    // Set values for row 2
    boolVector.set(2, 0)
    byteVector.set(2, 100.toByte)
    shortVector.set(2, 200.toShort)
    intVector.set(2, 300)
    longVector.set(2, 400L)
    floatVector.set(2, 500.5f)
    doubleVector.set(2, 600.6)
    
    boolVector.setValueCount(3)
    byteVector.setValueCount(3)
    shortVector.setValueCount(3)
    intVector.setValueCount(3)
    longVector.setValueCount(3)
    floatVector.setValueCount(3)
    doubleVector.setValueCount(3)
    
    val columns = (0 until root.getFieldVectors.size()).map { i =>
      new ArrowColumnVector(root.getVector(i))
    }.toArray
    
    val arrowRow = new ArrowRow(columns)
    
    // Test row 0
    arrowRow.rowId = 0
    assert(arrowRow.numFields() === 7)
    assert(arrowRow.getBoolean(0) === true)
    assert(arrowRow.getByte(1) === 1.toByte)
    assert(arrowRow.getShort(2) === 2.toShort)
    assert(arrowRow.getInt(3) === 3)
    assert(arrowRow.getLong(4) === 4L)
    assert(arrowRow.getFloat(5) === 5.5f)
    assert(arrowRow.getDouble(6) === 6.6)
    
    // Test row 1 (with nulls)
    arrowRow.rowId = 1
    assert(arrowRow.isNullAt(0) === true)
    assert(arrowRow.getByte(1) === 10.toByte)
    assert(arrowRow.isNullAt(2) === true)
    assert(arrowRow.getInt(3) === 30)
    assert(arrowRow.isNullAt(4) === true)
    assert(arrowRow.getFloat(5) === 50.5f)
    assert(arrowRow.isNullAt(6) === true)
    
    // Test row 2
    arrowRow.rowId = 2
    assert(arrowRow.getBoolean(0) === false)
    assert(arrowRow.getByte(1) === 100.toByte)
    assert(arrowRow.getShort(2) === 200.toShort)
    assert(arrowRow.getInt(3) === 300)
    assert(arrowRow.getLong(4) === 400L)
    assert(arrowRow.getFloat(5) === 500.5f)
    assert(arrowRow.getDouble(6) === 600.6)
    
    root.close()
  }

  test("ArrowRow with string type") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)

    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(2)
    
    val idVector = root.getVector(0).asInstanceOf[IntVector]
    val nameVector = root.getVector(1).asInstanceOf[VarCharVector]
    
    idVector.allocateNew()
    nameVector.allocateNew()
    
    idVector.set(0, 1)
    nameVector.setSafe(0, "Alice".getBytes("UTF-8"))
    
    idVector.set(1, 2)
    nameVector.setNull(1)
    
    idVector.setValueCount(2)
    nameVector.setValueCount(2)
    
    val columns = (0 until root.getFieldVectors.size()).map { i =>
      new ArrowColumnVector(root.getVector(i))
    }.toArray
    
    val arrowRow = new ArrowRow(columns)
    
    arrowRow.rowId = 0
    assert(arrowRow.getInt(0) === 1)
    assert(arrowRow.getUTF8String(1) === UTF8String.fromString("Alice"))
    
    arrowRow.rowId = 1
    assert(arrowRow.getInt(0) === 2)
    assert(arrowRow.isNullAt(1) === true)
    
    root.close()
  }

  test("ArrowRow copy() creates GenericInternalRow") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("value", DoubleType)

    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(1)
    
    val idVector = root.getVector(0).asInstanceOf[IntVector]
    val nameVector = root.getVector(1).asInstanceOf[VarCharVector]
    val valueVector = root.getVector(2).asInstanceOf[Float8Vector]
    
    idVector.allocateNew()
    nameVector.allocateNew()
    valueVector.allocateNew()
    
    idVector.set(0, 42)
    nameVector.setSafe(0, "test".getBytes("UTF-8"))
    valueVector.set(0, 3.14)
    
    idVector.setValueCount(1)
    nameVector.setValueCount(1)
    valueVector.setValueCount(1)
    
    val columns = (0 until root.getFieldVectors.size()).map { i =>
      new ArrowColumnVector(root.getVector(i))
    }.toArray
    
    val arrowRow = new ArrowRow(columns)
    arrowRow.rowId = 0
    
    val copiedRow = arrowRow.copy()
    
    assert(copiedRow.isInstanceOf[GenericInternalRow])
    assert(copiedRow.getInt(0) === 42)
    assert(copiedRow.getUTF8String(1) === UTF8String.fromString("test"))
    assert(copiedRow.getDouble(2) === 3.14)
    
    root.close()
  }

  test("ArrowRow setRowId and getRowId") {
    val schema = new StructType().add("value", IntegerType)
    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(3)
    
    val valueVector = root.getVector(0).asInstanceOf[IntVector]
    valueVector.allocateNew()
    valueVector.set(0, 100)
    valueVector.set(1, 200)
    valueVector.set(2, 300)
    valueVector.setValueCount(3)
    
    val columns = Array(new ArrowColumnVector(root.getVector(0)))
    val arrowRow = new ArrowRow(columns)
    
    arrowRow.setRowId(0)
    assert(arrowRow.getRowId() === 0)
    assert(arrowRow.getInt(0) === 100)
    
    arrowRow.setRowId(1)
    assert(arrowRow.getRowId() === 1)
    assert(arrowRow.getInt(0) === 200)
    
    arrowRow.setRowId(2)
    assert(arrowRow.getRowId() === 2)
    assert(arrowRow.getInt(0) === 300)
    
    root.close()
  }

  test("ArrowRow update throws UnsupportedOperationException") {
    val schema = new StructType().add("value", IntegerType)
    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(1)
    val valueVector = root.getVector(0).asInstanceOf[IntVector]
    valueVector.allocateNew()
    valueVector.set(0, 100)
    valueVector.setValueCount(1)
    
    val columns = Array(new ArrowColumnVector(root.getVector(0)))
    val arrowRow = new ArrowRow(columns)
    arrowRow.rowId = 0
    
    intercept[Exception] {
      arrowRow.update(0, 200)
    }
    
    root.close()
  }

  test("ArrowRow setNullAt throws UnsupportedOperationException") {
    val schema = new StructType().add("value", IntegerType)
    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(1)
    val valueVector = root.getVector(0).asInstanceOf[IntVector]
    valueVector.allocateNew()
    valueVector.set(0, 100)
    valueVector.setValueCount(1)
    
    val columns = Array(new ArrowColumnVector(root.getVector(0)))
    val arrowRow = new ArrowRow(columns)
    arrowRow.rowId = 0
    
    intercept[Exception] {
      arrowRow.setNullAt(0)
    }
    
    root.close()
  }

  test("ArrowRow with binary type") {
    val schema = new StructType().add("data", BinaryType)
    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(2)
    val dataVector = root.getVector(0).asInstanceOf[VarBinaryVector]
    dataVector.allocateNew()
    
    val bytes1 = Array[Byte](1, 2, 3, 4)
    dataVector.setSafe(0, bytes1)
    dataVector.setNull(1)
    dataVector.setValueCount(2)
    
    val columns = Array(new ArrowColumnVector(root.getVector(0)))
    val arrowRow = new ArrowRow(columns)
    
    arrowRow.rowId = 0
    assert(arrowRow.getBinary(0).sameElements(bytes1))
    
    arrowRow.rowId = 1
    assert(arrowRow.isNullAt(0) === true)
    
    root.close()
  }

  test("ArrowRow with decimal type") {
    val schema = new StructType().add("amount", DecimalType(10, 2))
    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(2)
    val decimalVector = root.getVector(0).asInstanceOf[DecimalVector]
    decimalVector.allocateNew()
    
    decimalVector.set(0, new java.math.BigDecimal("123.45"))
    decimalVector.setNull(1)
    decimalVector.setValueCount(2)
    
    val columns = Array(new ArrowColumnVector(root.getVector(0)))
    val arrowRow = new ArrowRow(columns)
    
    arrowRow.rowId = 0
    val decimal = arrowRow.getDecimal(0, 10, 2)
    assert(decimal.toBigDecimal === BigDecimal("123.45"))
    
    arrowRow.rowId = 1
    assert(arrowRow.isNullAt(0) === true)
    
    root.close()
  }

  test("ArrowRow get(ordinal, dataType) with various types") {
    val schema = new StructType()
      .add("int_col", IntegerType)
      .add("str_col", StringType)
      .add("null_col", IntegerType)

    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(1)
    
    val intVector = root.getVector(0).asInstanceOf[IntVector]
    val strVector = root.getVector(1).asInstanceOf[VarCharVector]
    val nullVector = root.getVector(2).asInstanceOf[IntVector]
    
    intVector.allocateNew()
    strVector.allocateNew()
    nullVector.allocateNew()
    
    intVector.set(0, 42)
    strVector.setSafe(0, "hello".getBytes("UTF-8"))
    nullVector.setNull(0)
    
    intVector.setValueCount(1)
    strVector.setValueCount(1)
    nullVector.setValueCount(1)
    
    val columns = (0 until root.getFieldVectors.size()).map { i =>
      new ArrowColumnVector(root.getVector(i))
    }.toArray
    
    val arrowRow = new ArrowRow(columns)
    arrowRow.rowId = 0
    
    assert(arrowRow.get(0, IntegerType) === 42)
    assert(arrowRow.get(1, StringType) === UTF8String.fromString("hello"))
    assert(arrowRow.get(2, IntegerType) === null)
    
    root.close()
  }

  test("ArrowRow getColumns returns underlying columns") {
    val schema = new StructType().add("value", IntegerType)
    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(1)
    val valueVector = root.getVector(0).asInstanceOf[IntVector]
    valueVector.allocateNew()
    valueVector.set(0, 100)
    valueVector.setValueCount(1)
    
    val columns = Array(new ArrowColumnVector(root.getVector(0)))
    val arrowRow = new ArrowRow(columns)
    
    assert(arrowRow.getColumns() === columns)
    
    root.close()
  }

  test("ArrowRow constructor from VectorSchemaRoot") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)

    val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    
    root.setRowCount(1)
    
    val idVector = root.getVector(0).asInstanceOf[IntVector]
    val nameVector = root.getVector(1).asInstanceOf[VarCharVector]
    
    idVector.allocateNew()
    nameVector.allocateNew()
    
    idVector.set(0, 123)
    nameVector.setSafe(0, "test".getBytes("UTF-8"))
    
    idVector.setValueCount(1)
    nameVector.setValueCount(1)
    
    val arrowRow = new ArrowRow(root)
    arrowRow.rowId = 0
    
    assert(arrowRow.numFields() === 2)
    assert(arrowRow.getInt(0) === 123)
    assert(arrowRow.getUTF8String(1) === UTF8String.fromString("test"))
    
    root.close()
  }
}
