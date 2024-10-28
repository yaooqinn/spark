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

  private static final long[] SECRETS = {
      -4734510112055689544L, 8988705074615774462L, 107169723235645804L, -9150895811085458631L,
      3206846044944704547L, -635991603978286172L, 2447473855086509643L, -5971219860401587010L,
      2066345149520216444L, -2441886536549236479L, -3108015162914296703L, 7914194659941938988L,
      -1626409839981944329L, -8941494824140493535L, -8033320652366799699L, -7525369938742813156L,
      -2623469361688619810L, 8276375387167616468L, 4644015609783511405L, -6611157965513653271L,
      -6583065893254229885L, -5213861871876335728L, -5496743794819540073L, 7472518124495991515L,
      2262974939099578482L, -3810212738154322880L, 8776142829118792868L, -1839215637059881052L,
      5685365492914041783L, -3724786431015557197L, -4554178371385614489L, -1891287204249351393L,
      8711581037947681227L, -9045227235349436807L, 6521908138563358438L, -3433288310154277810L,
      9065845566317379788L, -3711581430728825408L, -14498364963784475L, 8286566680123572856L,
      2410270004345854594L, -5178731653526335398L, 628288925819764176L, 5046485836271438973L,
      8378393743697575884L, -615790245780032769L, 4897510963931521394L, 2613204287568263201L,
      -8204357891075471176L, -2265833688187779576L, 3882259165203625030L, -8055285457383852172L,
      -1832905809766104073L, -9086416637098318781L, 4215904233249082916L, 2754656949352390798L,
      5487137525590930912L, 4344889773235015733L, 2899275987193816720L, 5920048007935066598L,
      -4948848801086031231L, -7945666784801315270L, -4354493403153806298L, 55047854181858380L,
      -3818837453329782724L, -8589771024315493848L, -3420260712846345390L, 7336514198459093435L,
      -8402080243849837679L, 1984792007109443779L, 5988533398925789952L, 3338042034334238923L,
      -6688317018830679928L, 8188439481968977360L, 7237745495519234917L, 5216419214072683403L,
      -7545670736427461861L, -6730831521841467821L, 982514005898797870L, -500565212929953373L,
      5690594596133299313L, 4057454151265110116L, 1817289281226577736L, -1217880312389983593L,
      5111331831722610082L, -6249044541332063987L, -2402310933491200263L, -5990164332231968690L,
      -2833645246901970632L, -6280079608045441255L, -384819531158567185L, 8573350489219836230L,
      4573118074737974953L, -2071806484620464930L, -7141794803835414356L, 3791154848057698520L,
      4554437623014685352L, -486612386300594438L, -2523916620961464458L, -4909775443879730369L,
      -4054404076451619613L, -4051062782047603556L, 848866664462761780L, 5695865814404364607L,
      2111919702937427193L, -8494546410135897124L, 5875540889195497403L, -2282891677615274041L,
      5467459601266838471L, -3653580031866876149L, -5418591349844075185L, 6464017090953185821L,
      3556072174620004746L, -4021334359191855023L, -6933237364981675040L, 9124231484359888203L,
      -3927526142850255667L, -2753530472436770380L, 8708212900181324121L, 8320639771003045937L,
      7238261902898274248L, -1556992608276218209L, -4185422456575899266L, -5997129611619018295L,
      -8958567948248450855L, 3784058077962335096L, -3227810254839716749L, -1453760514566526364L,
      -4329134394285701654L, -4196251135427498811L, -9095648454776683604L, -6881001310379625341L,
      -26878911368670412L, -360392965937173549L, 1439744095735366635L, 7139325810128831939L,
      -1485321483350670907L, 1723580219865931905L, 943481457726914464L, -2518330316883232001L,
      5898885483309765626L, -5237161843349560557L, -1101321574019503261L, -2670433016801847317L,
      5321830579834785047L, -3221803331004277491L, 1644739493610607069L, 6131320256870790993L,
      2762139043194663095L, 2965150961192524528L, -3158951516726670886L, 7553707719620219721L,
      -7032137544937171245L, 3143064850383918358L, 1597544665906226773L, -4749560797652047578L,
      6394572897509757993L, 9032178055121889492L, 5151371122220703336L, -6825348890156979298L,
      -242834301215959509L, -8071399103737053674L, -535932061014468418L, -5118182661306221224L,
      -3334642226765412483L, 8850058120466833735L, -3424193974287467359L, 3589503944184336380L,
      -3588858202114426737L, 5030012605302946040L, -3799403997270715976L, 4236556626373409489L,
      -8125959076964085638L, -7669846995664752176L, 1627364323045527089L, 294587268038608334L,
      2883454493032893253L, -5825401622958753077L, -2905059236606800070L, -299578263794707057L,
      -3820222711603128683L, -4914839139546299370L, 5457178556493670404L, 4633003122163691304L,
      9097354517224871855L
  };

  // Primes
  private static final long XXH_PRIME32_1 = 0x9E3779B1L;
  private static final long XXH_PRIME32_2 = 0x85EBCA77L;
  private static final long XXH_PRIME32_3 = 0xC2B2AE3DL;

  private static final long XXH_PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long XXH_PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long XXH_PRIME64_3 = 0x165667B19E3779F9L;
  private static final long XXH_PRIME64_4 = 0x85EBCA77C2B2AE63L;
  private static final long XXH_PRIME64_5 = 0x27D4EB2F165667C5L;

  // only support fixed size secret
  private static final int nbStripesPerBlock = (192 - 64) / 8;
  private static final int block_len = 64 * nbStripesPerBlock;

  private final long seed;

  public XXH3(long seed) {
    super();

    this.seed = seed;
  }
  // 2027464037 to hex


  @Override
  public String toString() {
    return "xxh3(seed=" + seed + ")";
  }

  public long hashInt(int input) {
    return hashInt(input, seed);
  }

  public static long hashInt(int input, long seed) {
    long s = seed ^ Long.reverseBytes(seed & 0xFFFFFFFFL);
    final long bitflip = SECRETS[1] ^ SECRETS[2] - s;
    final long keyed = ((input & 0xFFFFFFFFL) + (((long)input) << 32)) ^ bitflip;
    return rrmxmx(keyed, 4);
  }

  public long hashLong(long input) {
    return hashLong(input, seed);
  }

  public static long hashLong(long input, long seed) {
    final long s = seed ^ Long.reverseBytes(seed & 0xFFFFFFFFL);
    final long bitflip = SECRETS[1] ^ SECRETS[2] - s;
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
    final long upper = multiplyHigh(lhs, rhs) + ((lhs >> 63) & rhs) + ((rhs >> 63) & lhs);
    final long lower = lhs * rhs;
    return lower ^ upper;
  }

  private static long multiplyHigh(long x, long y) {
    if (x < 0 || y < 0) {
      // Use technique from section 8-2 of Henry S. Warren, Jr.,
      // Hacker's Delight (2nd ed.) (Addison Wesley, 2013), 173-174.
      long x1 = x >> 32;
      long x2 = x & 0xFFFFFFFFL;
      long y1 = y >> 32;
      long y2 = y & 0xFFFFFFFFL;
      long z2 = x2 * y2;
      long t = x1 * y2 + (z2 >>> 32);
      long z1 = t & 0xFFFFFFFFL;
      long z0 = t >> 32;
      z1 += x2 * y1;
      return x1 * y1 + z0 + (z1 >> 32);
    }
    else {
      // Use Karatsuba technique with two base 2^32 digits.
      long x1 = x >>> 32;
      long y1 = y >>> 32;
      long x2 = x & 0xFFFFFFFFL;
      long y2 = y & 0xFFFFFFFFL;
      long A = x1 * y1;
      long B = x2 * y2;
      long C = (x1 + x2) * (y1 + y2);
      long K = C - A - B;
      return (((B >>> 32) + K) >>> 32) + A;
    }
  }

  private static long u32(int i) {
    return i & 0xFFFFFFFFL;
  }

  private static long mix16B(
      final long seed, final Object input, final long offIn, final int offSec) {
    final long input_lo = Platform.getLong(input, offIn);
    final long input_hi = Platform.getLong(input, offIn + 8);
    return unsignedLongMulXorFold(input_lo ^ (SECRETS[offSec] + seed),
        input_hi ^ (SECRETS[offSec + 8] - seed));
  }

  private static long mix2Accs(final long acc_lh, final long acc_rh, final int offSec) {
    return unsignedLongMulXorFold(acc_lh ^ SECRETS[offSec], acc_rh ^ SECRETS[offSec + 8]);
  }

  public static long hashUnsafeBytes(Object base, long offset, int length, long seed) {
    assert (length >= 0) : "lengthInBytes cannot be negative";
    if (length <= 16) {
      // XXH3_len_0to16_64b
      if (length > 8) {
        // XXH3_len_9to16_64b
        final long bitflip1 = SECRETS[24] ^ SECRETS[32] + seed;
        final long bitflip2 = SECRETS[40] ^ SECRETS[48] - seed;
        final long input_lo = Platform.getLong(base, offset) ^ bitflip1;
        final long input_hi = Platform.getLong(base, offset + length - 8) ^ bitflip2;
        final long acc = length + Long.reverseBytes(input_lo) + input_hi
            + unsignedLongMulXorFold(input_lo, input_hi);
        return avalanche(acc);
      }
      if (length >= 4) {
        // XXH3_len_4to8_64b
        long s = seed ^ Long.reverseBytes(seed & 0xFFFFFFFFL);
        final long input1 = Platform.getInt(base, offset);
        final long input2 = Platform.getInt(base, offset + length - 4) & 0xFFFFFFFFL;
        final long bitflip = (SECRETS[8] ^ SECRETS[16]) - s;
        final long keyed = (input2 + (input1 << 32)) ^ bitflip;
        return rrmxmx(keyed, length);
      }
      if (length != 0) {
        // XXH3_len_1to3_64b
        final int c1 = Platform.getByte(base, offset) & 0xFF;
        final int c2 = Platform.getByte(base, offset + (length >> 1));
        final int c3 = Platform.getByte(base, offset + length - 1) & 0xFF;
        final long combined = u32(((c1 << 16) | (c2  << 24) | c3 | (length << 8)));
        final long bitflip = 0x78d8a565L + seed;
        return XXH64.fmix(combined ^ bitflip);
      }

      // XXH3_len_0b_16b
      return XXH64.fmix(seed ^ SECRETS[56] ^ SECRETS[64]);
    }

    if (length <= 128) {
      // XXH3_len_17to128_64b
      long acc = length * XXH_PRIME64_1;

      if (length > 32) {
        if (length > 64) {
          if (length > 96) {
            acc += mix16B(seed, base, offset + 48L, 96);
            acc += mix16B(seed, base, offset + length - 64L, 112);
          }
          acc += mix16B(seed, base, offset + 32L, 64);
          acc += mix16B(seed, base, offset + length - 48L, 80);
        }
        acc += mix16B(seed, base, offset + 16L, 32);
        acc += mix16B(seed, base, offset + length - 32L, 48);
      }
      acc += mix16B(seed, base, offset, 0);
      acc += mix16B(seed, base, offset + length - 16L, 16);
      return avalanche(acc);
    }

    if (length <= 240) {
      // XXH3_len_129to240_64b
      long acc = length * XXH_PRIME64_1;
      final int nbRounds = length / 16;
      int i = 0;
      for (; i < 8; ++i) {
        acc += mix16B(seed, base, offset + 16L * i,  16 * i);
      }
      acc = avalanche(acc);

      for (; i < nbRounds; ++i) {
        acc += mix16B(seed, base, offset + 16L * i, 16 * (i - 8) + 3);
      }

      /* last bytes */
      acc += mix16B(seed, base, offset + length - 16L,  136 - 17);
      return avalanche(acc);
    }

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
        final long offStripe = offBlock + s * 64;
        final int offSec = s * 8;
        {
          final long data_val_0 = Platform.getLong(base, offStripe);
          final long data_val_1 = Platform.getLong(base, offStripe + 8L);
          final long data_key_0 = data_val_0 ^ SECRETS[offSec];
          final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8];
          /* swap adjacent lanes */
          acc_0 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
          acc_1 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
        }
        {
          final long data_val_0 = Platform.getLong(base, offStripe + 8L * 2);
          final long data_val_1 = Platform.getLong(base, offStripe + 8L * 3);
          final long data_key_0 = data_val_0 ^ SECRETS[offSec + 8 * 2];
          final long data_key_1 = data_val_0 ^ SECRETS[offSec + 8 * 3];
          /* swap adjacent lanes */
          acc_2 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
          acc_3 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
        }
        {
          final long data_val_0 = Platform.getLong(base, offStripe + 8L * 4);
          final long data_val_1 = Platform.getLong(base, offStripe + 8L * 5);
          final long data_key_0 = data_val_0 ^ SECRETS[offSec + 8 * 4];
          final long data_key_1 = data_val_0 ^ SECRETS[offSec + 8 * 5];
          /* swap adjacent lanes */
          acc_4 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
          acc_5 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
        }
        {
          final long data_val_0 = Platform.getLong(base, offStripe + 8L * 6);
          final long data_val_1 = Platform.getLong(base, offStripe + 8L * 7);
          final long data_key_0 = data_val_1 ^ SECRETS[offSec + 8 * 6];
          final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8 * 7];
          /* swap adjacent lanes */
          acc_6 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
          acc_7 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
        }
      }

      // XXH3_scrambleAcc_scalar
      final int offSec = 192 - 64;
      acc_0 = (acc_0 ^ (acc_0 >>> 47) ^ SECRETS[offSec]) * XXH_PRIME32_1;
      acc_1 = (acc_1 ^ (acc_1 >>> 47) ^ SECRETS[offSec + 8]) * XXH_PRIME32_1;
      acc_2 = (acc_2 ^ (acc_2 >>> 47) ^ SECRETS[offSec + 8 * 2]) * XXH_PRIME32_1;
      acc_3 = (acc_3 ^ (acc_3 >>> 47) ^ SECRETS[offSec + 8 * 3]) * XXH_PRIME32_1;
      acc_4 = (acc_4 ^ (acc_4 >>> 47) ^ SECRETS[offSec + 8 * 4]) * XXH_PRIME32_1;
      acc_5 = (acc_5 ^ (acc_5 >>> 47) ^ SECRETS[offSec + 8 * 5]) * XXH_PRIME32_1;
      acc_6 = (acc_6 ^ (acc_6 >>> 47) ^ SECRETS[offSec + 8 * 6]) * XXH_PRIME32_1;
      acc_7 = (acc_7 ^ (acc_7 >>> 47) ^ SECRETS[offSec + 8 * 7]) * XXH_PRIME32_1;
    }

    /* last partial block */
    final long nbStripes = ((length - 1) - (block_len * nb_blocks)) / 64;
    final long offBlock = offset + block_len * nb_blocks;
    for (int s = 0; s < nbStripes; s++) {
      // XXH3_accumulate_512
      final long offStripe = offBlock + s * 64L;
      final int offSec = s * 8;
      {
        final long data_val_0 = Platform.getLong(base, offStripe);
        final long data_val_1 = Platform.getLong(base, offStripe + 8L);
        final long data_key_0 = data_val_0 ^ SECRETS[offSec];
        final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8];
        /* swap adjacent lanes */
        acc_0 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
        acc_1 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
      }
      {
        final long data_val_0 = Platform.getLong(base, offStripe + 8L * 2);
        final long data_val_1 = Platform.getLong(base, offStripe + 8L * 3);
        final long data_key_0 = data_val_0 ^ SECRETS[offSec + 8 * 2];
        final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8 * 3];
        /* swap adjacent lanes */
        acc_2 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
        acc_3 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
      }
      {
        final long data_val_0 = Platform.getLong(base, offStripe + 8L * 4);
        final long data_val_1 = Platform.getLong(base, offStripe + 8L * 5);
        final long data_key_0 = data_val_0 ^ SECRETS[offSec + 8 * 4];
        final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8 * 5];
        /* swap adjacent lanes */
        acc_4 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
        acc_5 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
      }
      {
        final long data_val_0 = Platform.getLong(base, offStripe + 8L * 6);
        final long data_val_1 = Platform.getLong(base, offStripe + 8L * 7);
        final long data_key_0 = data_val_0 ^ SECRETS[offSec + 8 * 6];
        final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8 * 7];
        /* swap adjacent lanes */
        acc_6 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
        acc_7 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
      }
    }

    /* last stripe */
    // XXH3_accumulate_512
    final long offStripe = offset + length - 64;
    final int offSec = 192 - 64 - 7;
    {
      final long data_val_0 = Platform.getLong(base, offStripe);
      final long data_val_1 = Platform.getLong(base, offStripe + 8L);
      final long data_key_0 = data_val_0 ^ SECRETS[offSec];
      final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8];
      /* swap adjacent lanes */
      acc_0 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
      acc_1 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
    }
    {
      final long data_val_0 = Platform.getLong(base, offStripe + 8L * 2);
      final long data_val_1 = Platform.getLong(base, offStripe + 8L * 3);
      final long data_key_0 = data_val_0 ^ SECRETS[offSec + 8 * 2];
      final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8 * 3];
      /* swap adjacent lanes */
      acc_2 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
      acc_3 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
    }
    {
      final long data_val_0 = Platform.getLong(base, offStripe + 8L * 4);
      final long data_val_1 = Platform.getLong(base, offStripe + 8L * 5);
      final long data_key_0 = data_val_0 ^ SECRETS[offSec + 8 * 4];
      final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8 * 5];
      /* swap adjacent lanes */
      acc_4 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
      acc_5 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
    }
    {
      final long data_val_0 = Platform.getLong(base, offStripe + 8L * 6);
      final long data_val_1 = Platform.getLong(base, offStripe + 8L * 7);
      final long data_key_0 = data_val_0 ^ SECRETS[offSec + 8 * 6];
      final long data_key_1 = data_val_1 ^ SECRETS[offSec + 8 * 7];
      /* swap adjacent lanes */
      acc_6 += data_val_1 + (0xFFFFFFFFL & data_key_0) * (data_key_0 >>> 32);
      acc_7 += data_val_0 + (0xFFFFFFFFL & data_key_1) * (data_key_1 >>> 32);
    }

    // XXH3_mergeAccs
    final long result64 = length * XXH_PRIME64_1
        + mix2Accs(acc_0, acc_1, 11)
        + mix2Accs(acc_2, acc_3, 11 + 16)
        + mix2Accs(acc_4, acc_5, 11 + 16 * 2)
        + mix2Accs(acc_6, acc_7, 11 + 16 * 3);

    return avalanche(result64);
  }
}
