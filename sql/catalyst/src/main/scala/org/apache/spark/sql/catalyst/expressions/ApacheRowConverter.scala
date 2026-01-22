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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converter utilities for converting between ApacheRow and other row formats.
 *
 * This object provides utility methods to convert between ApacheRow and other InternalRow
 * implementations, particularly UnsafeRow and GenericInternalRow.
 */
object ApacheRowConverter {

  /**
   * Convert an UnsafeRow to an ApacheRow.
   * This is useful when you need to work with a more flexible row format.
   */
  def toApacheRow(unsafeRow: UnsafeRow, schema: StructType): ApacheRow = {
    val len = unsafeRow.numFields
    val values = new Array[Any](len)
    var i = 0
    while (i < len) {
      if (unsafeRow.isNullAt(i)) {
        values(i) = null
      } else {
        values(i) = InternalRow.copyValue(unsafeRow.get(i, schema(i).dataType))
      }
      i += 1
    }
    new ApacheRow(values)
  }

  /**
   * Convert an UnsafeRow to an ApacheRow using field types.
   */
  def toApacheRow(unsafeRow: UnsafeRow, fieldTypes: Seq[DataType]): ApacheRow = {
    val len = unsafeRow.numFields
    val values = new Array[Any](len)
    var i = 0
    while (i < len) {
      if (unsafeRow.isNullAt(i)) {
        values(i) = null
      } else {
        values(i) = InternalRow.copyValue(unsafeRow.get(i, fieldTypes(i)))
      }
      i += 1
    }
    new ApacheRow(values)
  }

  /**
   * Convert a GenericInternalRow to an ApacheRow.
   */
  def toApacheRow(genericRow: GenericInternalRow, schema: StructType): ApacheRow = {
    ApacheRow.fromInternalRow(genericRow, schema)
  }

  /**
   * Convert any InternalRow to an ApacheRow.
   */
  def toApacheRow(row: InternalRow, schema: StructType): ApacheRow = {
    ApacheRow.fromInternalRow(row, schema)
  }

  /**
   * Convert any InternalRow to an ApacheRow using field types.
   */
  def toApacheRow(row: InternalRow, fieldTypes: Seq[DataType]): ApacheRow = {
    ApacheRow.fromInternalRow(row, fieldTypes)
  }
}
