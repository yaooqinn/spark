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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types._

/**
 * An Apache-standard row format implementation that serves as an alternative to UnsafeRow.
 *
 * This implementation uses a simple object array-based storage similar to GenericInternalRow,
 * but provides a standardized, portable format suitable for cross-component data exchange.
 * Unlike UnsafeRow which uses raw memory layout optimized for performance, ApacheRow
 * prioritizes compatibility and ease of use.
 *
 * ApacheRow is ideal for:
 * - Data exchange between different Spark components
 * - Scenarios where schema flexibility is important
 * - Testing and development where readability is valued
 * - Situations where off-heap memory usage needs to be avoided
 *
 * The row stores values as Java objects in an internal array, similar to GenericInternalRow,
 * but with explicit support for all Spark SQL data types.
 */
class ApacheRow(val values: Array[Any]) extends BaseGenericInternalRow {

  /** No-arg constructor for serialization. */
  def this() = this(null)

  /**
   * Construct an ApacheRow with a specified number of fields.
   * All fields are initialized to null.
   */
  def this(size: Int) = this(new Array[Any](size))

  /**
   * Construct an ApacheRow from a sequence of values.
   */
  def this(values: Seq[Any]) = this(values.toArray)

  override protected def genericGet(ordinal: Int): Any = values(ordinal)

  override def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = {
    values.clone().toSeq
  }

  override def numFields: Int = if (values == null) 0 else values.length

  override def setNullAt(i: Int): Unit = {
    values(i) = null
  }

  override def update(i: Int, value: Any): Unit = {
    values(i) = value
  }

  override def copy(): ApacheRow = {
    val len = numFields
    val newValues = new Array[Any](len)
    var i = 0
    while (i < len) {
      newValues(i) = InternalRow.copyValue(genericGet(i))
      i += 1
    }
    new ApacheRow(newValues)
  }

  /**
   * Creates a compact string representation of this row.
   */
  override def toString: String = {
    if (numFields == 0) {
      "[ApacheRow: empty]"
    } else {
      val sb = new StringBuilder
      sb.append("[ApacheRow: ")
      sb.append(genericGet(0))
      val len = numFields
      var i = 1
      while (i < len) {
        sb.append(", ")
        sb.append(genericGet(i))
        i += 1
      }
      sb.append("]")
      sb.toString()
    }
  }
}

object ApacheRow {
  /**
   * Create an ApacheRow from values.
   */
  def apply(values: Any*): ApacheRow = new ApacheRow(values.toArray)

  /**
   * Create an ApacheRow from a sequence of values.
   */
  def fromSeq(values: Seq[Any]): ApacheRow = new ApacheRow(values.toArray)

  /**
   * Create an empty ApacheRow.
   */
  val empty: ApacheRow = new ApacheRow(Array.empty[Any])

  /**
   * Convert an InternalRow to an ApacheRow.
   * If the input is already an ApacheRow, returns it directly.
   * Otherwise, creates a new ApacheRow with copied values.
   */
  def fromInternalRow(row: InternalRow, schema: StructType): ApacheRow = {
    row match {
      case apacheRow: ApacheRow => apacheRow
      case _ =>
        val len = row.numFields
        val values = new Array[Any](len)
        var i = 0
        while (i < len) {
          if (row.isNullAt(i)) {
            values(i) = null
          } else {
            values(i) = InternalRow.copyValue(row.get(i, schema(i).dataType))
          }
          i += 1
        }
        new ApacheRow(values)
    }
  }

  /**
   * Convert an InternalRow to an ApacheRow using field types.
   */
  def fromInternalRow(row: InternalRow, fieldTypes: Seq[DataType]): ApacheRow = {
    row match {
      case apacheRow: ApacheRow => apacheRow
      case _ =>
        val len = row.numFields
        val values = new Array[Any](len)
        var i = 0
        while (i < len) {
          if (row.isNullAt(i)) {
            values(i) = null
          } else {
            values(i) = InternalRow.copyValue(row.get(i, fieldTypes(i)))
          }
          i += 1
        }
        new ApacheRow(values)
    }
  }
}
