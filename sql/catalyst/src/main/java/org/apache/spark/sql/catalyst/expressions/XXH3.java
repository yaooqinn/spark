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

import java.nio.ByteOrder;

import org.apache.spark.unsafe.Platform;

public final class XXH3 {
  private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  /*! Pseudorandom secret taken directly from FARSH. */
  private static final UnsafeArrayData XXH3_kSecret = UnsafeArrayData.fromPrimitiveArray(
      new byte[] {
          (byte)0xb8, (byte)0xfe, (byte)0x6c, (byte)0x39, (byte)0x23, (byte)0xa4, (byte)0x4b, (byte)0xbe,
          (byte)0x7c, (byte)0x01, (byte)0x81, (byte)0x2c, (byte)0xf7, (byte)0x21, (byte)0xad, (byte)0x1c,
          (byte)0xde, (byte)0xd4, (byte)0x6d, (byte)0xe9, (byte)0x83, (byte)0x90, (byte)0x97, (byte)0xdb,
          (byte)0x72, (byte)0x40, (byte)0xa4, (byte)0xa4, (byte)0xb7, (byte)0xb3, (byte)0x67, (byte)0x1f,
          (byte)0xcb, (byte)0x79, (byte)0xe6, (byte)0x4e, (byte)0xcc, (byte)0xc0, (byte)0xe5, (byte)0x78,
          (byte)0x82, (byte)0x5a, (byte)0xd0, (byte)0x7d, (byte)0xcc, (byte)0xff, (byte)0x72, (byte)0x21,
          (byte)0xb8, (byte)0x08, (byte)0x46, (byte)0x74, (byte)0xf7, (byte)0x43, (byte)0x24, (byte)0x8e,
          (byte)0xe0, (byte)0x35, (byte)0x90, (byte)0xe6, (byte)0x81, (byte)0x3a, (byte)0x26, (byte)0x4c,
          (byte)0x3c, (byte)0x28, (byte)0x52, (byte)0xbb, (byte)0x91, (byte)0xc3, (byte)0x00, (byte)0xcb,
          (byte)0x88, (byte)0xd0, (byte)0x65, (byte)0x8b, (byte)0x1b, (byte)0x53, (byte)0x2e, (byte)0xa3,
          (byte)0x71, (byte)0x64, (byte)0x48, (byte)0x97, (byte)0xa2, (byte)0x0d, (byte)0xf9, (byte)0x4e,
          (byte)0x38, (byte)0x19, (byte)0xef, (byte)0x46, (byte)0xa9, (byte)0xde, (byte)0xac, (byte)0xd8,
          (byte)0xa8, (byte)0xfa, (byte)0x76, (byte)0x3f, (byte)0xe3, (byte)0x9c, (byte)0x34, (byte)0x3f,
          (byte)0xf9, (byte)0xdc, (byte)0xbb, (byte)0xc7, (byte)0xc7, (byte)0x0b, (byte)0x4f, (byte)0x1d,
          (byte)0x8a, (byte)0x51, (byte)0xe0, (byte)0x4b, (byte)0xcd, (byte)0xb4, (byte)0x59, (byte)0x31,
          (byte)0xc8, (byte)0x9f, (byte)0x7e, (byte)0xc9, (byte)0xd9, (byte)0x78, (byte)0x73, (byte)0x64,
          (byte)0xea, (byte)0xc5, (byte)0xac, (byte)0x83, (byte)0x34, (byte)0xd3, (byte)0xeb, (byte)0xc3,
          (byte)0xc5, (byte)0x81, (byte)0xa0, (byte)0xff, (byte)0xfa, (byte)0x13, (byte)0x63, (byte)0xeb,
          (byte)0x17, (byte)0x0d, (byte)0xdd, (byte)0x51, (byte)0xb7, (byte)0xf0, (byte)0xda, (byte)0x49,
          (byte)0xd3, (byte)0x16, (byte)0x55, (byte)0x26, (byte)0x29, (byte)0xd4, (byte)0x68, (byte)0x9e,
          (byte)0x2b, (byte)0x16, (byte)0xbe, (byte)0x58, (byte)0x7d, (byte)0x47, (byte)0xa1, (byte)0xfc,
          (byte)0x8f, (byte)0xf8, (byte)0xb8, (byte)0xd1, (byte)0x7a, (byte)0xd0, (byte)0x31, (byte)0xce,
          (byte)0x45, (byte)0xcb, (byte)0x3a, (byte)0x8f, (byte)0x95, (byte)0x16, (byte)0x04, (byte)0x28,
          (byte)0xaf, (byte)0xd7, (byte)0xfb, (byte)0xca, (byte)0xbb, (byte)0x4b, (byte)0x40, (byte)0x7e
      });

  // Primes
  private static final long XXH_PRIME32_1 = 0x9E3779B1L;   /*!< 0b10011110001101110111100110110001 */
  private static final long XXH_PRIME32_2 = 0x85EBCA77L;   /*!< 0b10000101111010111100101001110111 */
  private static final long XXH_PRIME32_3 = 0xC2B2AE3DL;   /*!< 0b11000010101100101010111000111101 */

  private static final long XXH_PRIME64_1 = 0x9E3779B185EBCA87L;   /*!< 0b1001111000110111011110011011000110000101111010111100101010000111 */
  private static final long XXH_PRIME64_2 = 0xC2B2AE3D27D4EB4FL;   /*!< 0b1100001010110010101011100011110100100111110101001110101101001111 */
  private static final long XXH_PRIME64_3 = 0x165667B19E3779F9L;   /*!< 0b0001011001010110011001111011000110011110001101110111100111111001 */
  private static final long XXH_PRIME64_4 = 0x85EBCA77C2B2AE63L;   /*!< 0b1000010111101011110010100111011111000010101100101010111001100011 */
  private static final long XXH_PRIME64_5 = 0x27D4EB2F165667C5L;   /*!< 0b0010011111010100111010110010111100010110010101100110011111000101 */

  // only support fixed size secret
  private static final int nbStripesPerBlock = (192 - 64) / 8;
  private static final int block_len = 64 * nbStripesPerBlock;

  private final long seed;

  public XXH3(long seed) {
    super();
    this.seed = seed;
  }

  @Override
  public String toString() {
    return "xxh3(seed=" + seed + ")";
  }

  public long hashInt(int input) {
    return hashInt(input, seed);
  }

  public static long hashInt(int input, long seed) {
    long s = seed ^ Long.reverseBytes(seed & 0xFFFFFFFFL);
    final long bitflip = (XXH3_kSecret.getLong(1) ^ XXH3_kSecret.getLong(2)) - s;
    final long keyed = ((input & 0xFFFFFFFFL) + (((long)input) << 32)) ^ bitflip;
    return rrmxmx(keyed, 4);
  }

  public long hashLong(long input) {
    return hashLong(input, seed);
  }

  public static long hashLong(long input, long seed) {
    final long s = seed ^ Long.reverseBytes(seed & 0xFFFFFFFFL);
    final long bitflip = (XXH3_kSecret.getLong(1) ^ XXH3_kSecret.getLong(2)) - s;
    final long keyed = Long.rotateLeft(input, 32) ^ bitflip;
    return rrmxmx(keyed, 8);
  }

  private static long avalanche(long h64) {
    h64 ^= h64 >>> 37;
    h64 *= 0x165667919E3779F9L;
    return h64 ^ (h64 >>> 32);
  }

  private static long rrmxmx(long h64, final long length) {
    h64 ^= Long.rotateLeft(h64, 49) ^ Long.rotateLeft(h64, 24);
    h64 *= 0x9FB21C651E98DF25L;
    h64 ^= (h64 >>> 35) + length;
    h64 *= 0x9FB21C651E98DF25L;
    return h64 ^ (h64 >>> 28);
  }

  private static long unsignedLongMulXorFold(final long lhs, final long rhs) {
    // The Grade School method of multiplication is a hair faster in Java, primarily used here
    // because the implementation is simpler.
    final long lhs_l = lhs & 0xFFFFFFFFL;
    final long lhs_h = lhs >>> 32;
    final long rhs_l = rhs & 0xFFFFFFFFL;
    final long rhs_h = rhs >>> 32;
    final long lo_lo = lhs_l * rhs_l;
    final long hi_lo = lhs_h * rhs_l;
    final long lo_hi = lhs_l * rhs_h;
    final long hi_hi = lhs_h * rhs_h;

    // Add the products together. This will never overflow.
    final long cross = (lo_lo >>> 32) + (hi_lo & 0xFFFFFFFFL) + lo_hi;
    final long upper = (hi_lo >>> 32) + (cross >>> 32) + hi_hi;
    final long lower = (cross << 32) | (lo_lo & 0xFFFFFFFFL);
    return lower ^ upper;
  }

  private static <T> long XXH3_mix16B(final long seed, final T input, final long offIn, final long offSec) {
    final long input_lo = Platform.getLong(input, offIn);
    final long input_hi = Platform.getLong(input, offIn + 8);
    return unsignedLongMulXorFold(
        input_lo ^ Platform.getLong(XXH3_kSecret, offSec) + seed,
        input_hi ^ Platform.getLong(XXH3_kSecret, offSec + 8) - seed);
  }

  private static long XXH3_mix2Accs(final long acc_lh, final long acc_rh, final long offSec) {
    return unsignedLongMulXorFold(
        acc_lh ^ Platform.getLong(XXH3_kSecret, offSec),
        acc_rh ^ Platform.getLong(XXH3_kSecret, offSec + 8L));
  }

  private static long hashUnsafeBytes(Object base, long offset, int length, long seed) {
    assert (length >= 0) : "lengthInBytes cannot be negative";
    if (length < 16) {
      // XXH3_len_0to16_64b
      if (length > 8) {
        // XXH3_len_9to16_64b
        final long bitflip1 = (XXH3_kSecret.getLong(3) ^ XXH3_kSecret.getLong(4)) + seed;
        final long bitflip2 = (XXH3_kSecret.getLong(5) ^ XXH3_kSecret.getLong(6)) - seed;
        final long input_lo = Platform.getLong(base, offset) ^ bitflip1;
        final long input_hi = Platform.getLong(base, offset + length - 8) ^ bitflip2;
        final long acc = length + Long.reverseBytes(input_lo) + input_hi + unsignedLongMulXorFold(input_lo, input_hi);
        return avalanche(acc);
      } else  if (length >= 4) {
        // XXH3_len_4to8_64b
        long s = seed ^ Long.reverseBytes(seed & 0xFFFFFFFFL);
        final long input1 = Platform.getInt(base, offset); // high int will be shifted
        final long input2 = Platform.getInt(base, offset + length - 4) & 0xFFFFFFFFL;
        final long bitflip = (XXH3_kSecret.getLong(1) ^ XXH3_kSecret.getLong(2)) - s;
        final long keyed = (input2 + (input1 << 32)) ^ bitflip;
        return rrmxmx(keyed, length);
      } else if (length != 0) {
        // XXH3_len_1to3_64b
        final int c1 = Platform.getByte(base, offset) & 0xFF;
        final int c2 = Platform.getByte(base, offset + (length >> 1)); // high 3 bytes will be shifted
        final int c3 = Platform.getByte(base, offset + length - 1) & 0xFF;
        final long combined = ((c1 << 16) | (c2  << 24) | c3 | ((int)length << 8)) & 0xFFFFFFFFL;
        final long bitflip = (XXH3_kSecret.getInt(0) ^ XXH3_kSecret.getInt(1)) & 0xFFFFFFFFL + seed;
        return XXH64.fmix(combined ^ bitflip);
      } else {
        // XXH3_len_0b_16b
        return avalanche(seed ^ XXH3_kSecret.getLong(7) ^ XXH3_kSecret.getLong(8));
      }
    } else if (length <= 128) {
      // XXH3_len_17to128_64b
      long acc = length * XXH_PRIME64_1;

      if (length > 32) {
        if (length > 64) {
          if (length > 96) {
            acc += XXH3_mix16B(seed, base, offset + 48L, 96L);
            acc += XXH3_mix16B(seed, base, offset + length - 64L, 112L);
          }
          acc += XXH3_mix16B(seed, base, offset + 32L, 64L);
          acc += XXH3_mix16B(seed, base, offset + length - 48L, 80L);
        }
        acc += XXH3_mix16B(seed, base, offset + 16L, 32L);
        acc += XXH3_mix16B(seed, base, offset + length - 32L, 48L);
      }
      acc += XXH3_mix16B(seed, base, offset, 0);
      acc += XXH3_mix16B(seed, base, offset + length - 16L, 16L);
      return avalanche(acc);
    } else if (length <= 240) {
      // XXH3_len_129to240_64b
      long acc = length * XXH_PRIME64_1;
      final int nbRounds = length / 16;
      int i = 0;
      for (; i < 8; ++i) {
        acc += XXH3_mix16B(seed, base, offset + 16L * i,  16L * i);
      }
      acc = avalanche(acc);

      for (; i < nbRounds; ++i) {
        acc += XXH3_mix16B(seed, base, offset + 16L * i, 16L * (i - 8) + 3L);
      }

      /* last bytes */
      acc += XXH3_mix16B(seed, base, offset + length - 16L,  136L - 17L);
      return avalanche(acc);
    } else {
      // XXH3_hashLong_64b_internal
      long acc_0 = XXH_PRIME32_3;
      long acc_1 = XXH_PRIME64_1;
      long acc_2 = XXH_PRIME64_2;
      long acc_3 = XXH_PRIME64_3;
      long acc_4 = XXH_PRIME64_4;
      long acc_5 = XXH_PRIME32_2;
      long acc_6 = XXH_PRIME64_5;
      long acc_7 = XXH_PRIME32_1;

      // XXH3_hashLong_internal_loop
      final int nb_blocks = (length - 1) / block_len;
      for (long n = 0; n < nb_blocks; n++) {
        // XXH3_accumulate
        final long offBlock = offset + n * block_len;
        for (int s = 0; s < nbStripesPerBlock; s++ ) {
          // XXH3_accumulate_512
          final long offStripe = offBlock + s * 64L;
          {
            final long data_val_0 = Platform.getLong(base, offStripe);
            final long data_val_1 = Platform.getLong(base, offStripe + 8L);
            final long data_key_0 = data_val_0 ^ XXH3_kSecret.getLong(s);
            final long data_key_1 = data_val_1 ^ XXH3_kSecret.getLong(s + 1);
            /* swap adjacent lanes */
            acc_0 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
            acc_1 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
          }
          {
            final long data_val_0 = Platform.getLong(base, offStripe + 8L * 2);
            final long data_val_1 = Platform.getLong(base, offStripe + 8L * 3);
            final long data_key_0 = data_val_0 ^ XXH3_kSecret.getLong(s + 2);
            final long data_key_1 = data_val_1 ^ XXH3_kSecret.getLong(s + 3);
            /* swap adjacent lanes */
            acc_2 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
            acc_3 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
          }
          {
            final long data_val_0 = Platform.getLong(base, offStripe + 8L * 4);
            final long data_val_1 = Platform.getLong(base, offStripe + 8L * 5);
            final long data_key_0 = data_val_0 ^ XXH3_kSecret.getLong(s + 4);
            final long data_key_1 = data_val_1 ^ XXH3_kSecret.getLong(s + 5);;
            /* swap adjacent lanes */
            acc_4 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
            acc_5 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
          }
          {
            final long data_val_0 = Platform.getLong(base, offStripe + 8L * 6);
            final long data_val_1 = Platform.getLong(base, offStripe + 8L * 7);
            final long data_key_0 = data_val_0 ^ XXH3_kSecret.getLong(s + 6);
            final long data_key_1 = data_val_1 ^ XXH3_kSecret.getLong(s + 7);;
            /* swap adjacent lanes */
            acc_6 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
            acc_7 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
          }
        }

        // XXH3_scrambleAcc_scalar
        acc_0 = (acc_0 ^ (acc_0 >>> 47) ^ XXH3_kSecret.getLong(16)) * XXH_PRIME32_1;
        acc_1 = (acc_1 ^ (acc_1 >>> 47) ^ XXH3_kSecret.getLong(17)) * XXH_PRIME32_1;
        acc_2 = (acc_2 ^ (acc_2 >>> 47) ^ XXH3_kSecret.getLong(18)) * XXH_PRIME32_1;
        acc_3 = (acc_3 ^ (acc_3 >>> 47) ^ XXH3_kSecret.getLong(19)) * XXH_PRIME32_1;
        acc_4 = (acc_4 ^ (acc_4 >>> 47) ^ XXH3_kSecret.getLong(20)) * XXH_PRIME32_1;
        acc_5 = (acc_5 ^ (acc_5 >>> 47) ^ XXH3_kSecret.getLong(21)) * XXH_PRIME32_1;
        acc_6 = (acc_6 ^ (acc_6 >>> 47) ^ XXH3_kSecret.getLong(22)) * XXH_PRIME32_1;
        acc_7 = (acc_7 ^ (acc_7 >>> 47) ^ XXH3_kSecret.getLong(23)) * XXH_PRIME32_1;
      }

      /* last partial block */
      final int nbStripes = ((length - 1) - (block_len * nb_blocks)) / 64;
      final long offBlock = offset + block_len * nb_blocks;
      for (int s = 0; s < nbStripes; s++) {
        // XXH3_accumulate_512
        final long offStripe = offBlock + s * 64;
        {
          final long data_val_0 = Platform.getLong(base, offStripe);
          final long data_val_1 = Platform.getLong(base, offStripe + 8L);
          final long data_key_0 = data_val_0 ^ XXH3_kSecret.getLong(s);
          final long data_key_1 = data_val_1 ^ XXH3_kSecret.getLong(s + 1);;
          /* swap adjacent lanes */
          acc_0 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
          acc_1 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
        }
        {
          final long data_val_0 = Platform.getLong(base, offStripe + 8L * 2);
          final long data_val_1 = Platform.getLong(base, offStripe + 8L * 3);
          final long data_key_0 = data_val_0 ^ XXH3_kSecret.getLong(s + 2);;
          final long data_key_1 = data_val_1 ^ XXH3_kSecret.getLong(s + 3);
          /* swap adjacent lanes */
          acc_2 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
          acc_3 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
        }
        {
          final long data_val_0 = Platform.getLong(base, offStripe + 8L * 4);
          final long data_val_1 = Platform.getLong(base, offStripe + 8L * 5);
          final long data_key_0 = data_val_0 ^ XXH3_kSecret.getLong(s + 4);
          final long data_key_1 = data_val_1 ^ XXH3_kSecret.getLong(s + 5);
          /* swap adjacent lanes */
          acc_4 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
          acc_5 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
        }
        {
          final long data_val_0 = Platform.getLong(base, offStripe + 8L * 6);
          final long data_val_1 = Platform.getLong(base, offStripe + 8L * 7);
          final long data_key_0 = data_val_0 ^ XXH3_kSecret.getLong(s + 6);
          final long data_key_1 = data_val_1 ^ XXH3_kSecret.getLong(s + 7);
          /* swap adjacent lanes */
          acc_6 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
          acc_7 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
        }
      }

      /* last stripe */
      // XXH3_accumulate_512
      final long offStripe = offset + length - 64;
      final long offSec = 192 - 64 - 7;
      {
        final long data_val_0 = Platform.getLong(base, offStripe);
        final long data_val_1 = Platform.getLong(base, offStripe + 8L);
        final long data_key_0 = data_val_0 ^ Platform.getLong(XXH3_kSecret,offSec);
        final long data_key_1 = data_val_1 ^ Platform.getLong(XXH3_kSecret,offSec + 8L);
        /* swap adjacent lanes */
        acc_0 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
        acc_1 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
      }
      {
        final long data_val_0 = Platform.getLong(base, offStripe + 8L * 2);
        final long data_val_1 = Platform.getLong(base, offStripe + 8L * 3);
        final long data_key_0 = data_val_0 ^ Platform.getLong(XXH3_kSecret,offSec + 8L * 2);
        final long data_key_1 = data_val_1 ^ Platform.getLong(XXH3_kSecret,offSec + 8L * 3);
        /* swap adjacent lanes */
        acc_2 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
        acc_3 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
      }
      {
        final long data_val_0 = Platform.getLong(base, offStripe + 8L * 4);
        final long data_val_1 = Platform.getLong(base, offStripe + 8L * 5);
        final long data_key_0 = data_val_0 ^ Platform.getLong(XXH3_kSecret,offSec + 8L * 4);
        final long data_key_1 = data_val_1 ^ Platform.getLong(XXH3_kSecret,offSec + 8L * 5);
        /* swap adjacent lanes */
        acc_4 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
        acc_5 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
      }
      {
        final long data_val_0 = Platform.getLong(base, offStripe + 8L * 6);
        final long data_val_1 = Platform.getLong(base, offStripe + 8L * 7);
        final long data_key_0 = data_val_0 ^ Platform.getLong(XXH3_kSecret,offSec + 8L * 6);
        final long data_key_1 = data_val_1 ^ Platform.getLong(XXH3_kSecret,offSec + 8L * 7);
        /* swap adjacent lanes */
        acc_6 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
        acc_7 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
      }

      // XXH3_mergeAccs
      final long result64 = length * XXH_PRIME64_1
          + XXH3_mix2Accs(acc_0, acc_1, 11L)
          + XXH3_mix2Accs(acc_2, acc_3, 11L + 16L)
          + XXH3_mix2Accs(acc_4, acc_5, 11L + 16L * 2)
          + XXH3_mix2Accs(acc_6, acc_7, 11L + 16L * 3);

      return avalanche(result64);
    }
  }

  public long hashUnsafeBytes(Object base, long offset, int length) {
    return hashUnsafeBytes(base, offset, length, seed);
  }
}
