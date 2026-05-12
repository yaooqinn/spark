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

package org.apache.spark.sql.execution.columnar

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.sketch.BloomFilter

/**
 * Prototype: Bloom Filter cache serializer with typed dispatch + SQLConf.
 *
 * Subclass of DefaultCachedBatchSerializer that adds a per-partition BloomFilter
 * for EVERY BF-eligible column (Byte/Short/Int/Long/Date/Timestamp/String/Binary
 * /Decimal). Eligibility: not Float/Double/struct/array/map/null.
 *
 * Throwaway prototype. SQLConf via runtime conf strings (no SQLConf.scala patch).
 *
 * Limitations vs final design:
 *   - No CBO-driven NDV gating (uses fixed minNDV via conf, not catalog stats)
 *   - No late materialization with row-level skip (per-batch BF only)
 *   - No Comet bypass coordination (Comet enabled forces user to opt out)
 */

// scalastyle:off line.size.limit
sealed trait BfPutter {
  def put(row: InternalRow): Unit
  def mightContain(literal: Any): Boolean
}

object BfPutter {
  // Build a typed putter for the column. Returns None if type is not BF-eligible.
  def apply(dt: DataType, ord: Int, bf: BloomFilter): Option[BfPutter] = dt match {
    case ByteType => Some(new ByteBfPutter(bf, ord))
    case ShortType => Some(new ShortBfPutter(bf, ord))
    case IntegerType | DateType => Some(new IntBfPutter(bf, ord))
    case LongType | _: TimestampType => Some(new LongBfPutter(bf, ord))
    case StringType => Some(new StringBfPutter(bf, ord))
    case BinaryType => Some(new BinaryBfPutter(bf, ord))
    case d: DecimalType => Some(new DecimalBfPutter(bf, ord, d.precision, d.scale))
    case _ => None  // Float/Double/struct/array/map/null/etc.
  }

  // Probe-side: build a literal-extractor. Returns Some(Long-coded value) if the
  // literal type matches the column type for `mightContain`.
  def isEligible(dt: DataType): Boolean = apply(dt, 0, BloomFilter.create(1024, 0.01)).isDefined
}

class ByteBfPutter(bf: BloomFilter, ord: Int) extends BfPutter {
  def put(row: InternalRow): Unit = if (!row.isNullAt(ord)) bf.putLong(row.getByte(ord).toLong)
  def mightContain(literal: Any): Boolean = literal match {
    case b: Byte => bf.mightContainLong(b.toLong)
    case _ => true
  }
}
class ShortBfPutter(bf: BloomFilter, ord: Int) extends BfPutter {
  def put(row: InternalRow): Unit = if (!row.isNullAt(ord)) bf.putLong(row.getShort(ord).toLong)
  def mightContain(literal: Any): Boolean = literal match {
    case s: Short => bf.mightContainLong(s.toLong)
    case _ => true
  }
}
class IntBfPutter(bf: BloomFilter, ord: Int) extends BfPutter {
  def put(row: InternalRow): Unit = if (!row.isNullAt(ord)) bf.putLong(row.getInt(ord).toLong)
  def mightContain(literal: Any): Boolean = literal match {
    case i: Int => bf.mightContainLong(i.toLong)
    case _ => true
  }
}
class LongBfPutter(bf: BloomFilter, ord: Int) extends BfPutter {
  def put(row: InternalRow): Unit = if (!row.isNullAt(ord)) bf.putLong(row.getLong(ord))
  def mightContain(literal: Any): Boolean = literal match {
    case l: Long => bf.mightContainLong(l)
    case _ => true
  }
}
class StringBfPutter(bf: BloomFilter, ord: Int) extends BfPutter {
  def put(row: InternalRow): Unit = if (!row.isNullAt(ord)) bf.putBinary(row.getUTF8String(ord).getBytes)
  def mightContain(literal: Any): Boolean = literal match {
    case s: org.apache.spark.unsafe.types.UTF8String => bf.mightContainBinary(s.getBytes)
    case _ => true
  }
}
class BinaryBfPutter(bf: BloomFilter, ord: Int) extends BfPutter {
  def put(row: InternalRow): Unit = if (!row.isNullAt(ord)) bf.putBinary(row.getBinary(ord))
  def mightContain(literal: Any): Boolean = literal match {
    case b: Array[Byte] => bf.mightContainBinary(b)
    case _ => true
  }
}
class DecimalBfPutter(bf: BloomFilter, ord: Int, precision: Int, scale: Int) extends BfPutter {
  def put(row: InternalRow): Unit = if (!row.isNullAt(ord)) {
    val d = row.getDecimal(ord, precision, scale)
    bf.putBinary(d.toJavaBigDecimal.unscaledValue.toByteArray)
  }
  def mightContain(literal: Any): Boolean = literal match {
    case d: org.apache.spark.sql.types.Decimal =>
      bf.mightContainBinary(d.toJavaBigDecimal.unscaledValue.toByteArray)
    case _ => true
  }
}

object BloomFilterCachedBatchSerializer {
  // Driver-side accumulators for spike diagnostics.
  @volatile private var _skipped: org.apache.spark.util.LongAccumulator = _
  @volatile private var _scanned: org.apache.spark.util.LongAccumulator = _
  @volatile private var _batchSkipped: org.apache.spark.util.LongAccumulator = _
  @volatile private var _batchScanned: org.apache.spark.util.LongAccumulator = _
  def skipped: org.apache.spark.util.LongAccumulator = {
    if (_skipped == null) {
      _skipped = org.apache.spark.SparkContext.getOrCreate().longAccumulator("bf_part_skipped")
    }
    _skipped
  }
  def scanned: org.apache.spark.util.LongAccumulator = {
    if (_scanned == null) {
      _scanned = org.apache.spark.SparkContext.getOrCreate().longAccumulator("bf_part_scanned")
    }
    _scanned
  }
  def batchSkipped: org.apache.spark.util.LongAccumulator = {
    if (_batchSkipped == null) {
      _batchSkipped = org.apache.spark.SparkContext.getOrCreate().longAccumulator("bf_batch_skipped")
    }
    _batchSkipped
  }
  def batchScanned: org.apache.spark.util.LongAccumulator = {
    if (_batchScanned == null) {
      _batchScanned = org.apache.spark.SparkContext.getOrCreate().longAccumulator("bf_batch_scanned")
    }
    _batchScanned
  }

  // Conf keys (prototype: read via SQLConf.getConfString, no SQLConf.scala patch)
  val FPP_KEY = "spark.sql.inMemoryColumnarStorage.bloomFilter.fpp"
  val EXPECTED_ROWS_KEY = "spark.sql.inMemoryColumnarStorage.bloomFilter.expectedRowsPerPartition"
  val MIN_NDV_KEY = "spark.sql.inMemoryColumnarStorage.bloomFilter.minNDV"
  // late mat conf reserved (not yet wired):
  // val LATE_MAT_ENABLED_KEY = "spark.sql.inMemoryColumnarStorage.bloomFilter.lateMat.enabled"
  // val LATE_MAT_MAX_SEL_KEY = "spark.sql.inMemoryColumnarStorage.bloomFilter.lateMat.maxSelectivity"
}

class BloomFilterCachedBatchSerializer extends DefaultCachedBatchSerializer with Logging {

  import BloomFilterCachedBatchSerializer._

  private def confDouble(conf: SQLConf, key: String, default: Double): Double =
    Option(conf.getConfString(key, null)).map(_.toDouble).getOrElse(default)
  private def confLong(conf: SQLConf, key: String, default: Long): Long =
    Option(conf.getConfString(key, null)).map(_.toLong).getOrElse(default)

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    val fpp = confDouble(conf, FPP_KEY, 0.01)
    val expectedRows = confLong(conf, EXPECTED_ROWS_KEY, 1000000L)
    val bfCols: Seq[Int] = schema.zipWithIndex.collect {
      case (a, i) if BfPutter.isEligible(a.dataType) => i
    }
    if (bfCols.isEmpty) {
      logWarning(s"BF-WRITE: no BF-eligible col in schema, falling back to default")
      return super.convertInternalRowToCachedBatch(input, schema, storageLevel, conf)
    }
    logWarning(s"BF-WRITE: bfCols=${bfCols.map(i => s"$i:${schema(i).name}").mkString(",")}" +
      s" fpp=$fpp expectedRows=$expectedRows")
    convertForCacheWithBF(input, schema, batchSize, useCompression, bfCols, fpp, expectedRows)
  }

  private def convertForCacheWithBF(
      input: RDD[InternalRow],
      output: Seq[Attribute],
      batchSize: Int,
      useCompression: Boolean,
      bfCols: Seq[Int],
      fpp: Double,
      expectedRows: Long): RDD[CachedBatch] = {
    val bfColsArr = bfCols.toArray
    input.mapPartitionsInternal { rowIterator =>
      // One BF per BF-eligible column, shared across all batches in this partition.
      val bfs = bfColsArr.map(_ => BloomFilter.create(expectedRows, fpp))
      val putters: Array[BfPutter] = bfColsArr.zip(bfs).map { case (i, bf) =>
        BfPutter(output(i).dataType, i, bf).get
      }
      val batches = ArrayBuffer.empty[DefaultCachedBatch]
      val perBatchBfBytes = ArrayBuffer.empty[Map[Int, Array[Byte]]]
      while (rowIterator.hasNext) {
        val columnBuilders = output.map { attribute =>
          ColumnBuilder(attribute.dataType, batchSize, attribute.name, useCompression)
        }.toArray
        // Per-batch BF: smaller capacity (rows fit one batch ~10K).
        val batchBfs = bfColsArr.map(_ => BloomFilter.create(batchSize.toLong, fpp))
        val batchPutters: Array[BfPutter] = bfColsArr.zip(batchBfs).map { case (i, bf) =>
          BfPutter(output(i).dataType, i, bf).get
        }
        var rowCount = 0
        var totalSize = 0L
        while (rowIterator.hasNext && rowCount < batchSize
            && totalSize < ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE) {
          val row = rowIterator.next()
          assert(row.numFields == columnBuilders.length)
          var i = 0
          totalSize = 0
          while (i < row.numFields) {
            columnBuilders(i).appendFrom(row, i)
            totalSize += columnBuilders(i).columnStats.sizeInBytes
            i += 1
          }
          var p = 0
          while (p < putters.length) {
            putters(p).put(row)
            batchPutters(p).put(row)
            p += 1
          }
          rowCount += 1
        }
        val stats = new GenericInternalRow(
          columnBuilders.flatMap(_.columnStats.collectedStatistics))
        batches += DefaultCachedBatch(rowCount, columnBuilders.map { builder =>
          JavaUtils.bufferToArray(builder.build())
        }, stats)
        // Serialize per-batch BFs.
        perBatchBfBytes += bfColsArr.zip(batchBfs).map { case (i, bf) =>
          val baos = new ByteArrayOutputStream()
          bf.writeTo(baos)
          i -> baos.toByteArray
        }.toMap
      }
      // Serialize per-partition BFs once.
      val bfBytesMap: Map[Int, Array[Byte]] = bfColsArr.zip(bfs).map { case (i, bf) =>
        val baos = new ByteArrayOutputStream()
        bf.writeTo(baos)
        i -> baos.toByteArray
      }.toMap
      batches.iterator.zip(perBatchBfBytes.iterator).map { case (b, batchBf) =>
        BloomFilterCachedBatch(b, bfBytesMap, batchBf)
      }
    }
  }

  // Unwrap BloomFilterCachedBatch -> inner DefaultCachedBatch before delegating to super,
  // because super read paths cast to DefaultCachedBatch directly (InMemoryRelation.scala:203).
  private def unwrap(input: RDD[CachedBatch]): RDD[CachedBatch] = input.map {
    case bf: BloomFilterCachedBatch => bf.inner
    case other => other
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] =
    super.convertCachedBatchToColumnarBatch(
      unwrap(input), cacheAttributes, selectedAttributes, conf)

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] =
    super.convertCachedBatchToInternalRow(unwrap(input), cacheAttributes, selectedAttributes, conf)

  // Extract BF-eligible point predicates per column.
  // Returns Map[colIdx, Seq[literalValue]] where literalValue is type-matched to the col dataType.
  private def extractBfPredicates(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): Map[Int, Seq[Any]] = {
    val byCol = scala.collection.mutable.Map.empty[Int, ArrayBuffer[Any]]
    predicates.foreach {
      case EqualTo(a: Attribute, Literal(v, dt)) =>
        val idx = cachedAttributes.indexWhere(_.semanticEquals(a))
        if (idx >= 0 && BfPutter.isEligible(dt) && cachedAttributes(idx).dataType == dt) {
          byCol.getOrElseUpdate(idx, ArrayBuffer.empty) += v
        }
      case EqualTo(Literal(v, dt), a: Attribute) =>
        val idx = cachedAttributes.indexWhere(_.semanticEquals(a))
        if (idx >= 0 && BfPutter.isEligible(dt) && cachedAttributes(idx).dataType == dt) {
          byCol.getOrElseUpdate(idx, ArrayBuffer.empty) += v
        }
      case In(a: Attribute, list) =>
        val idx = cachedAttributes.indexWhere(_.semanticEquals(a))
        if (idx >= 0) {
          list.foreach {
            case Literal(v, dt) if BfPutter.isEligible(dt) && cachedAttributes(idx).dataType == dt =>
              byCol.getOrElseUpdate(idx, ArrayBuffer.empty) += v
            case _ => // ignore non-literal
          }
        }
      case _ => // ignore non-BF predicates
    }
    byCol.view.mapValues(_.toSeq).toMap
  }

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]):
      (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    val superFilter = super.buildFilter(predicates, cachedAttributes)
    val bfPredsMap = extractBfPredicates(predicates, cachedAttributes)
    logWarning(s"BF-READ: bfPreds=${bfPredsMap.map { case (i, vs) =>
      s"${cachedAttributes(i).name}=${vs.mkString(",")}" }.mkString("|")}" +
      s" predicates=${predicates.mkString("|")}")
    if (bfPredsMap.isEmpty) return superFilter

    // Build dummy BF for type-dispatch on read side; will be replaced per-batch via deserialize.
    val skipAcc = BloomFilterCachedBatchSerializer.skipped
    val scanAcc = BloomFilterCachedBatchSerializer.scanned
    val batchSkipAcc = BloomFilterCachedBatchSerializer.batchSkipped
    val batchScanAcc = BloomFilterCachedBatchSerializer.batchScanned

    // Helper: check a Map[colIdx -> BF bytes] against all bfPredsMap (AND semantic).
    def probeBfMap(m: Map[Int, Array[Byte]]): Boolean = bfPredsMap.forall { case (colIdx, lits) =>
      m.get(colIdx) match {
        case Some(bytes) =>
          val bf = BloomFilter.readFrom(new ByteArrayInputStream(bytes))
          val dt = cachedAttributes(colIdx).dataType
          val probe = BfPutter(dt, colIdx, bf).get
          lits.exists(v => probe.mightContain(v))
        case None => true
      }
    }

    (partId, iter) => {
      var partitionDecided = false
      var partitionPasses = true
      superFilter(partId, iter).filter {
        case bfBatch: BloomFilterCachedBatch =>
          if (!partitionDecided) {
            partitionDecided = true
            partitionPasses = probeBfMap(bfBatch.partitionBloomFilters)
            if (partitionPasses) scanAcc.add(1L) else skipAcc.add(1L)
            logWarning(s"BF-SKIP: part=$partId passes=$partitionPasses")
          }
          if (!partitionPasses) {
            false
          } else {
            // Per-batch probe (late materialization): skip individual batches
            // within a passing partition.
            val batchPasses = probeBfMap(bfBatch.batchBloomFilters)
            if (batchPasses) batchScanAcc.add(1L) else batchSkipAcc.add(1L)
            batchPasses
          }
        case _ => true
      }
    }
  }
}
// scalastyle:on line.size.limit
