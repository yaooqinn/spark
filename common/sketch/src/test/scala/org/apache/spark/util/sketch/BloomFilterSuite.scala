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

package org.apache.spark.util.sketch

import java.io.ByteArrayOutputStream

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class BloomFilterSuite extends AnyFunSuite { // scalastyle:ignore funsuite
  private final val EPSILON = 0.01

  // Serializes and deserializes a given `BloomFilter`, then checks whether the deserialized
  // version is equivalent to the original one.
  private def checkSerDe(filter: BloomFilter): Unit = {
    val out = new ByteArrayOutputStream()
    filter.writeTo(out)
    out.close()

    val deserialized = BloomFilter.readFrom(out.toByteArray)

    assert(filter == deserialized)
  }

  def testAccuracy[T: ClassTag](
      typeName: String, numItems: Int, version: Int)(itemGen: Random => T): Unit = {
    test(s"accuracy - $typeName - version $version") {
      // use a fixed seed to make the test predictable.
      val r = new Random(37)
      val fpp = 0.05
      val numInsertion = numItems / 10

      val allItems = Array.fill(numItems)(itemGen(r))

      val filter = BloomFilter.create(numInsertion, fpp, version)

      // insert first `numInsertion` items.
      val insertedItems = allItems.take(numInsertion)
      insertedItems.foreach(filter.put)

      // false negative is not allowed.
      assert(insertedItems.forall(filter.mightContain))

      // The number of inserted items doesn't exceed `expectedNumItems`, so the `expectedFpp`
      // should not be significantly higher than the one we passed in to create this bloom filter.
      assert(filter.expectedFpp() - fpp < EPSILON)

      val rest = allItems.diff(insertedItems)
      val errorCount = rest.count(filter.mightContain)

      // Also check the actual fpp is not significantly higher than we expected.
      val actualFpp = errorCount.toDouble / rest.length
      assert(actualFpp - fpp < EPSILON)

      checkSerDe(filter)
    }
  }

  def testMergeInPlace[T: ClassTag](
      typeName: String, numItems: Int, version: Int)(itemGen: Random => T): Unit = {
    test(s"mergeInPlace - $typeName - version $version") {
      // use a fixed seed to make the test predictable.
      val r = new Random(37)

      val items1 = Array.fill(numItems / 2)(itemGen(r))
      val items2 = Array.fill(numItems / 2)(itemGen(r))

      val filter1 = BloomFilter.create(numItems, BloomFilter.DEFAULT_FPP, version)
      items1.foreach(filter1.put)

      val filter2 = BloomFilter.create(numItems, BloomFilter.DEFAULT_FPP, version)
      items2.foreach(filter2.put)

      filter1.mergeInPlace(filter2)

      // After merge, `filter1` has `numItems` items which doesn't exceed `expectedNumItems`, so the
      // `expectedFpp` should not be significantly higher than the default one.
      assert(filter1.expectedFpp() - BloomFilter.DEFAULT_FPP < EPSILON)

      items1.foreach(i => assert(filter1.mightContain(i)))
      items2.foreach(i => assert(filter1.mightContain(i)))

      checkSerDe(filter1)
    }
  }

  def testIntersectInPlace[T: ClassTag](
      typeName: String, numItems: Int, version: Int)(itemGen: Random => T): Unit = {
    test(s"intersectInPlace - $typeName - version $version") {
      // use a fixed seed to make the test predictable.
      val r = new Random(37)

      val items1 = Array.fill(numItems / 2)(itemGen(r))
      val items2 = Array.fill(numItems / 2)(itemGen(r))

      val filter1 = BloomFilter.create(numItems / 2, BloomFilter.DEFAULT_FPP, version)
      items1.foreach(filter1.put)

      val filter2 = BloomFilter.create(numItems / 2, BloomFilter.DEFAULT_FPP, version)
      items2.foreach(filter2.put)

      filter1.intersectInPlace(filter2)

      val common_items = items1.intersect(items2)
      common_items.foreach(i => assert(filter1.mightContain(i)))

      // After intersect, `filter1` still has `numItems/2` items
      // which doesn't exceed `expectedNumItems`,
      // so the `expectedFpp` should not be higher than the default one.
      assert(filter1.expectedFpp() - BloomFilter.DEFAULT_FPP < EPSILON)

      checkSerDe(filter1)
    }
  }

  def testItemType[T: ClassTag](typeName: String, numItems: Int)(itemGen: Random => T): Unit = {
    testAccuracy[T](typeName, numItems, 1)(itemGen)
    testAccuracy[T](typeName, numItems, 2)(itemGen)
    testMergeInPlace[T](typeName, numItems, 1)(itemGen)
    testMergeInPlace[T](typeName, numItems, 2)(itemGen)
    testIntersectInPlace[T](typeName, numItems, 1)(itemGen)
    testIntersectInPlace[T](typeName, numItems, 2)(itemGen)

  }

  // 60 is a magic number satisfying the requirement of `actualFpp` assertion.
  testItemType[Byte]("Byte", 60) { _.nextBytes(1).head }

  testItemType[Short]("Short", 1000) { _.nextInt().toShort }

  testItemType[Int]("Int", 100000) { _.nextInt() }

  testItemType[Long]("Long", 100000) { _.nextLong() }

  testItemType[String]("String", 100000) { r => r.nextString(r.nextInt(512)) }

  test("incompatible merge") {
    intercept[IncompatibleMergeException] {
      BloomFilter.create(1000).mergeInPlace(null)
    }

    intercept[IncompatibleMergeException] {
      val filter1 = BloomFilter.create(1000, 6400)
      val filter2 = BloomFilter.create(1000, 3200)
      filter1.mergeInPlace(filter2)
    }

    intercept[IncompatibleMergeException] {
      val filter1 = BloomFilter.create(1000, 6400)
      val filter2 = BloomFilter.create(2000, 6400)
      filter1.mergeInPlace(filter2)
    }

    intercept[IncompatibleMergeException] {
      val filter1 = BloomFilter.create(1000, 6400, 1)
      val filter2 = BloomFilter.create(1000, 6400, 2)
      filter1.mergeInPlace(filter2)
    }
  }
}
