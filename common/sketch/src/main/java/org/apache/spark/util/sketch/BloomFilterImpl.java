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

package org.apache.spark.util.sketch;

import java.io.*;
import java.util.Objects;

class BloomFilterImpl extends BloomFilter implements Serializable {

  private int numHashFunctions;

  private BitArray bits;

  private int version;

  private HashStrategy strategy;

  BloomFilterImpl(int numHashFunctions, long numBits, int version) {
    this(new BitArray(numBits), numHashFunctions, version);
  }

  BloomFilterImpl(int numHashFunctions, long numBits) {
    this(new BitArray(numBits), numHashFunctions, Version.V1.getVersionNumber());
  }

  private BloomFilterImpl(BitArray bits, int numHashFunctions, int version) {
    this.bits = bits;
    this.numHashFunctions = numHashFunctions;
    this.version = version;
    this.strategy = HashStrategies.fromVersion(version);
  }

  private BloomFilterImpl() {}

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof BloomFilterImpl that)) {
      return false;
    }

    return this.version == that.version &&
      this.numHashFunctions == that.numHashFunctions && this.bits.equals(that.bits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bits, numHashFunctions, version);
  }

  @Override
  public double expectedFpp() {
    return Math.pow((double) bits.cardinality() / bits.bitSize(), numHashFunctions);
  }

  @Override
  public long bitSize() {
    return bits.bitSize();
  }

  @Override
  public boolean put(Object item) {
    if (item instanceof String str) {
      return putString(str);
    } else if (item instanceof byte[] bytes) {
      return putBinary(bytes);
    } else {
      return putLong(Utils.integralToLong(item));
    }
  }

  @Override
  public boolean putString(String item) {
    return putBinary(Utils.getBytesFromUTF8String(item));
  }

  @Override
  public boolean putBinary(byte[] item) {
    return strategy.putBinary(item, bits, numHashFunctions);
  }

  @Override
  public boolean mightContainString(String item) {
    return mightContainBinary(Utils.getBytesFromUTF8String(item));
  }

  @Override
  public boolean mightContainBinary(byte[] item) {
    return strategy.mightContainBinary(item, bits, numHashFunctions);
  }

  @Override
  public boolean putLong(long item) {
    return strategy.putLong(item, bits, numHashFunctions);
  }

  @Override
  public boolean mightContainLong(long item) {
    return strategy.mightContainLong(item, bits, numHashFunctions);
  }

  @Override
  public boolean mightContain(Object item) {
    if (item instanceof String str) {
      return mightContainString(str);
    } else if (item instanceof byte[] bytes) {
      return mightContainBinary(bytes);
    } else {
      return mightContainLong(Utils.integralToLong(item));
    }
  }

  @Override
  public boolean isCompatible(BloomFilter other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof BloomFilterImpl that)) {
      return false;
    }

    return this.bitSize() == that.bitSize() &&
      this.numHashFunctions == that.numHashFunctions && this.version == that.version;
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException {
    BloomFilterImpl otherImplInstance = checkCompatibilityForMerge(other);

    this.bits.putAll(otherImplInstance.bits);
    return this;
  }

  @Override
  public BloomFilter intersectInPlace(BloomFilter other) throws IncompatibleMergeException {
    BloomFilterImpl otherImplInstance = checkCompatibilityForMerge(other);

    this.bits.and(otherImplInstance.bits);
    return this;
  }

  @Override
  public long cardinality() {
    return this.bits.cardinality();
  }

  private BloomFilterImpl checkCompatibilityForMerge(BloomFilter other)
          throws IncompatibleMergeException {
    // Duplicates the logic of `isCompatible` here to provide better error message.
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof BloomFilterImpl that)) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filter of class " + other.getClass().getName()
      );
    }

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filters with different number of hash functions"
      );
    }

    if (this.version != that.version) {
      throw new IncompatibleMergeException(
          "Cannot merge bloom filters with different versions"
      );
    }

    return that;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);
    dos.writeInt(version);
    dos.writeInt(numHashFunctions);
    bits.writeTo(dos);
  }

  private void readFrom0(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);
    this.version = dis.readInt();
    this.strategy = HashStrategies.fromVersion(version);
    this.numHashFunctions = dis.readInt();
    this.bits = BitArray.readFrom(dis);
  }

  public static BloomFilterImpl readFrom(InputStream in) throws IOException {
    BloomFilterImpl filter = new BloomFilterImpl();
    filter.readFrom0(in);
    return filter;
  }

  public static BloomFilterImpl readFrom(byte[] bytes) throws IOException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
      return readFrom(bis);
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeTo(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    readFrom0(in);
  }

  interface HashStrategy extends Serializable {
    boolean putBinary(byte[] item, BitArray bits, int numHashFunctions);
    boolean putLong(long item, BitArray bits, int numHashFunctions);
    boolean mightContainBinary(byte[] item, BitArray bits, int numHashFunctions);
    boolean mightContainLong(long item, BitArray bits, int numHashFunctions);

    default boolean mightContainInternal(BitArray bits, int h1, int h2, int numHashFunctions) {
      long bitSize = bits.bitSize();
      for (int i = 1; i <= numHashFunctions; i++) {
        int combinedHash = h1 + i * h2;
        // Flip all the bits if it's negative (guaranteed positive number)
        if (combinedHash < 0) {
          combinedHash = ~combinedHash;
        }
        if (!bits.get(combinedHash % bitSize)) {
          return false;
        }
      }
      return true;
    }

    default boolean putInternal(BitArray bits, int h1, int h2, int numHashFunctions) {
      long bitSize = bits.bitSize();
      boolean bitsChanged = false;
      for (int i = 1; i <= numHashFunctions; i++) {
        int combinedHash = h1 + i * h2;
        // Flip all the bits if it's negative (guaranteed positive number)
        if (combinedHash < 0) {
          combinedHash = ~combinedHash;
        }
        bitsChanged |= bits.set(combinedHash % bitSize);
      }
      return bitsChanged;
    }
  }

  enum HashStrategies implements HashStrategy {
    MURMUR3_x86_32 {
      @Override
      public boolean putBinary(byte[] item, BitArray bits, int numHashFunctions) {
        int h1 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
        int h2 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, h1);
        return putInternal(bits, h1, h2, numHashFunctions);
      }

      @Override
      public boolean putLong(long item, BitArray bits, int numHashFunctions) {
        // Here we first hash the input long element into 2 int hash values, h1 and h2, then
        // produce n hash values by `h1 + i * h2` with 1 <= i <= numHashFunctions.
        // Note that `CountMinSketch` use a different strategy, it hash the input long element
        // with every i to produce n hash values.
        // TODO: the strategy of `CountMinSketch` looks more advanced, should we follow it here?
        int h1 = Murmur3_x86_32.hashLong(item, 0);
        int h2 = Murmur3_x86_32.hashLong(item, h1);
        return putInternal(bits, h1, h2, numHashFunctions);
      }

      @Override
      public boolean mightContainBinary(byte[] item, BitArray bits, int numHashFunctions) {
        int h1 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
        int h2 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, h1);
        return mightContainInternal(bits, h1, h2, numHashFunctions);
      }

      @Override
      public boolean mightContainLong(long item, BitArray bits, int numHashFunctions) {
        int h1 = Murmur3_x86_32.hashLong(item, 0);
        int h2 = Murmur3_x86_32.hashLong(item, h1);
        return mightContainInternal(bits, h1, h2, numHashFunctions);
      }
    },
    XX_HASH_64 {
      @Override
      public boolean putBinary(byte[] item, BitArray bits, int numHashFunctions) {
        long h64 = XXH64.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
        int h1 = (int) h64;
        int h2 = (int) (h64 >>> 32);
        return putInternal(bits, h1, h2, numHashFunctions);
      }

      @Override
      public boolean putLong(long item, BitArray bits, int numHashFunctions) {
        long h64 = XXH64.hashLong(item, 0);
        int h1 = (int) h64;
        int h2 = (int) (h64 >>> 32);
        return putInternal(bits, h1, h2, numHashFunctions);
      }

      @Override
      public boolean mightContainBinary(byte[] item, BitArray bits, int numHashFunctions) {
        long h64 = XXH64.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
        int h1 = (int) h64;
        int h2 = (int) (h64 >>> 32);
        return mightContainInternal(bits, h1, h2, numHashFunctions);
      }

      @Override
      public boolean mightContainLong(long item, BitArray bits, int numHashFunctions) {
        long h64 = XXH64.hashLong(item, 0);
        int h1 = (int) h64;
        int h2 = (int) (h64 >>> 32);
        return mightContainInternal(bits, h1, h2, numHashFunctions);
      }
    };

    public static HashStrategy fromVersion(int version) {
      return switch (version) {
        case 1 -> HashStrategies.MURMUR3_x86_32;
        case 2 -> HashStrategies.XX_HASH_64;
        default ->
          throw new IllegalArgumentException(
            "Unsupported Bloom filter version number (" + version + ")");
      };
    }
  }
}
