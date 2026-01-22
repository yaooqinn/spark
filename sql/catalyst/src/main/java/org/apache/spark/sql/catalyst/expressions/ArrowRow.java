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

package org.apache.spark.sql.catalyst.expressions;

import java.util.Map;

import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.types.*;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;
import org.apache.spark.unsafe.types.GeographyVal;
import org.apache.spark.unsafe.types.GeometryVal;

/**
 * An Arrow-based implementation of InternalRow which provides row-oriented access to
 * Arrow columnar data as an alternative to UnsafeRow.
 *
 * This class wraps an array of Arrow-backed {@link ColumnVector} and provides a row view,
 * enabling zero-copy interaction with the Arrow ecosystem while maintaining compatibility
 * with Spark's internal row representation.
 *
 * Unlike UnsafeRow which uses a custom binary format, ArrowRow directly uses Apache Arrow's
 * columnar memory layout, making it efficient for interoperability with Arrow-based systems
 * and libraries.
 *
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * // Create from VectorSchemaRoot
 * VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
 * ArrowRow row = new ArrowRow(root);
 * row.setRowId(0);
 * int value = row.getInt(0);
 *
 * // Create from ColumnVector array
 * ColumnVector[] columns = new ColumnVector[] { new ArrowColumnVector(vector) };
 * ArrowRow row = new ArrowRow(columns);
 * row.rowId = 0;
 * String str = row.getUTF8String(0).toString();
 * }</pre>
 *
 * <h3>Design Notes:</h3>
 * <ul>
 * <li>ArrowRow is read-only. Calls to {@code update()} or {@code setNullAt()} will throw
 *     {@link SparkUnsupportedOperationException}.</li>
 * <li>The {@code copy()} method creates a deep copy as a {@link GenericInternalRow}.</li>
 * <li>Multiple rows can share the same column vectors; use {@code rowId} to access different rows.</li>
 * </ul>
 *
 * @since 4.0.0
 */
@DeveloperApi
public final class ArrowRow extends InternalRow {
  
  /**
   * The current row index being accessed in the columnar data.
   */
  public int rowId;
  
  /**
   * Array of Arrow-backed column vectors containing the data.
   */
  private final ColumnVector[] columns;
  
  /**
   * Constructs an ArrowRow wrapping the given column vectors.
   *
   * @param columns Array of Arrow-backed column vectors
   */
  public ArrowRow(ColumnVector[] columns) {
    this.columns = columns;
  }
  
  /**
   * Constructs an ArrowRow from an Arrow VectorSchemaRoot.
   *
   * @param root Arrow VectorSchemaRoot containing the data
   */
  public ArrowRow(VectorSchemaRoot root) {
    this.columns = new ColumnVector[root.getFieldVectors().size()];
    for (int i = 0; i < columns.length; i++) {
      this.columns[i] = new ArrowColumnVector(root.getVector(i));
    }
  }
  
  @Override
  public int numFields() {
    return columns.length;
  }
  
  @Override
  public InternalRow copy() {
    GenericInternalRow row = new GenericInternalRow(columns.length);
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = columns[i].dataType();
        PhysicalDataType pdt = PhysicalDataType.apply(dt);
        if (pdt instanceof PhysicalBooleanType) {
          row.setBoolean(i, getBoolean(i));
        } else if (pdt instanceof PhysicalByteType) {
          row.setByte(i, getByte(i));
        } else if (pdt instanceof PhysicalShortType) {
          row.setShort(i, getShort(i));
        } else if (pdt instanceof PhysicalIntegerType) {
          row.setInt(i, getInt(i));
        } else if (pdt instanceof PhysicalLongType) {
          row.setLong(i, getLong(i));
        } else if (pdt instanceof PhysicalFloatType) {
          row.setFloat(i, getFloat(i));
        } else if (pdt instanceof PhysicalDoubleType) {
          row.setDouble(i, getDouble(i));
        } else if (pdt instanceof PhysicalStringType) {
          row.update(i, getUTF8String(i).copy());
        } else if (pdt instanceof PhysicalBinaryType) {
          row.update(i, getBinary(i));
        } else if (pdt instanceof PhysicalGeographyType) {
          row.update(i, getGeography(i));
        } else if (pdt instanceof PhysicalGeometryType) {
          row.update(i, getGeometry(i));
        } else if (pdt instanceof PhysicalDecimalType t) {
          row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (pdt instanceof PhysicalStructType t) {
          row.update(i, getStruct(i, t.fields().length).copy());
        } else if (pdt instanceof PhysicalArrayType) {
          row.update(i, getArray(i).copy());
        } else if (pdt instanceof PhysicalMapType) {
          row.update(i, getMap(i).copy());
        } else if (pdt instanceof PhysicalCalendarIntervalType) {
          row.setInterval(i, getInterval(i));
        } else if (pdt instanceof PhysicalVariantType) {
          row.update(i, getVariant(i));
        } else {
          throw new RuntimeException("ArrowRow does not support data type: " + dt);
        }
      }
    }
    return row;
  }
  
  @Override
  public boolean isNullAt(int ordinal) {
    return columns[ordinal].isNullAt(rowId);
  }
  
  @Override
  public boolean getBoolean(int ordinal) {
    return columns[ordinal].getBoolean(rowId);
  }
  
  @Override
  public byte getByte(int ordinal) {
    return columns[ordinal].getByte(rowId);
  }
  
  @Override
  public short getShort(int ordinal) {
    return columns[ordinal].getShort(rowId);
  }
  
  @Override
  public int getInt(int ordinal) {
    return columns[ordinal].getInt(rowId);
  }
  
  @Override
  public long getLong(int ordinal) {
    return columns[ordinal].getLong(rowId);
  }
  
  @Override
  public float getFloat(int ordinal) {
    return columns[ordinal].getFloat(rowId);
  }
  
  @Override
  public double getDouble(int ordinal) {
    return columns[ordinal].getDouble(rowId);
  }
  
  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return columns[ordinal].getDecimal(rowId, precision, scale);
  }
  
  @Override
  public UTF8String getUTF8String(int ordinal) {
    return columns[ordinal].getUTF8String(rowId);
  }
  
  @Override
  public byte[] getBinary(int ordinal) {
    return columns[ordinal].getBinary(rowId);
  }
  
  @Override
  public GeographyVal getGeography(int ordinal) {
    return columns[ordinal].getGeography(rowId);
  }
  
  @Override
  public GeometryVal getGeometry(int ordinal) {
    return columns[ordinal].getGeometry(rowId);
  }
  
  @Override
  public CalendarInterval getInterval(int ordinal) {
    return columns[ordinal].getInterval(rowId);
  }
  
  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    return columns[ordinal].getStruct(rowId);
  }
  
  @Override
  public ArrayData getArray(int ordinal) {
    return columns[ordinal].getArray(rowId);
  }
  
  @Override
  public MapData getMap(int ordinal) {
    return columns[ordinal].getMap(rowId);
  }
  
  @Override
  public VariantVal getVariant(int ordinal) {
    return columns[ordinal].getVariant(rowId);
  }
  
  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal)) {
      return null;
    }
    PhysicalDataType pdt = PhysicalDataType.apply(dataType);
    if (pdt instanceof PhysicalBooleanType) {
      return getBoolean(ordinal);
    } else if (pdt instanceof PhysicalByteType) {
      return getByte(ordinal);
    } else if (pdt instanceof PhysicalShortType) {
      return getShort(ordinal);
    } else if (pdt instanceof PhysicalIntegerType) {
      return getInt(ordinal);
    } else if (pdt instanceof PhysicalLongType) {
      return getLong(ordinal);
    } else if (pdt instanceof PhysicalFloatType) {
      return getFloat(ordinal);
    } else if (pdt instanceof PhysicalDoubleType) {
      return getDouble(ordinal);
    } else if (pdt instanceof PhysicalStringType) {
      return getUTF8String(ordinal);
    } else if (pdt instanceof PhysicalBinaryType) {
      return getBinary(ordinal);
    } else if (pdt instanceof PhysicalGeographyType) {
      return getGeography(ordinal);
    } else if (pdt instanceof PhysicalGeometryType) {
      return getGeometry(ordinal);
    } else if (pdt instanceof PhysicalDecimalType t) {
      return getDecimal(ordinal, t.precision(), t.scale());
    } else if (pdt instanceof PhysicalStructType t) {
      return getStruct(ordinal, t.fields().length);
    } else if (pdt instanceof PhysicalArrayType) {
      return getArray(ordinal);
    } else if (pdt instanceof PhysicalMapType) {
      return getMap(ordinal);
    } else if (pdt instanceof PhysicalCalendarIntervalType) {
      return getInterval(ordinal);
    } else if (pdt instanceof PhysicalVariantType) {
      return getVariant(ordinal);
    } else {
      throw new RuntimeException("ArrowRow does not support data type: " + dataType);
    }
  }
  
  @Override
  public void update(int i, Object value) {
    throw SparkUnsupportedOperationException.apply("UNSUPPORTED_FEATURE.ARROW_ROW_UPDATE");
  }
  
  @Override
  public void setNullAt(int i) {
    throw SparkUnsupportedOperationException.apply("UNSUPPORTED_FEATURE.ARROW_ROW_UPDATE");
  }
  
  /**
   * Returns the underlying Arrow-backed column vectors.
   */
  public ColumnVector[] getColumns() {
    return columns;
  }
  
  /**
   * Sets the current row ID to access.
   *
   * @param rowId The row index to access
   */
  public void setRowId(int rowId) {
    this.rowId = rowId;
  }
  
  /**
   * Gets the current row ID being accessed.
   *
   * @return The current row index
   */
  public int getRowId() {
    return rowId;
  }
}
