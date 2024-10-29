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

import net.openhft.hashing.Access;
import net.openhft.hashing.LongHashFunction;
import org.apache.spark.unsafe.Platform;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;


public final class XXH3v2 {

  private static final NativeAccess INSTANCE = new NativeAccess();

  private final long seed;

  private final LongHashFunction hashFunction;

  public XXH3v2(long seed) {
    super();
    this.seed = seed;
    this.hashFunction = LongHashFunction.xx3(seed);
  }


  static class NativeAccess extends Access<Object> {
    private NativeAccess() {
    }

    @Override
    public int getByte(Object input, long offset) {
      return Platform.getByte(input, offset);
    }

    @Override
    public int getShort(Object input, long offset) {
      return Platform.getShort(input, offset);
    }

    @Override
    public int getInt(Object input, long offset) {
      return Platform.getInt(input, offset);
    }

    @Override
    public long getLong(Object input, long offset) {
      return Platform.getLong(input, offset);
    }

    @Override
    public int getUnsignedByte(Object input, long offset) {
      return getByte(input, offset) & 0xFF;
    }

    @Override
    public int getUnsignedShort(Object input, long offset) {
      return getShort(input, offset) & 0xFFFF;
    }

    @Override
    public long getUnsignedInt(Object input, long offset) {
      return getInt(input, offset) & 0xFFFFFFFFL;
    }


    @Override
    public ByteOrder byteOrder(Object input) {
      return ByteOrder.nativeOrder();
    }

    @Override
    protected Access<Object> reverseAccess() {
      return new ReverseAccess(INSTANCE);
    }
  }

  static class ReverseAccess<T> extends Access<T> {
    final Access<T> access;
    private ReverseAccess(final Access<T> access) {
      this.access = access;
    }
    @Override
    public long getLong(final T input, final long offset) {
      return Long.reverseBytes(access.getLong(input, offset));
    }
    @Override
    public long getUnsignedInt(final T input, final long offset) {
      return Long.reverseBytes(access.getUnsignedInt(input, offset)) >>> 32;
    }
    @Override
    public int getInt(final T input, final long offset) {
      return Integer.reverseBytes(access.getInt(input, offset));
    }
    @Override
    public int getUnsignedShort(final T input, final long offset) {
      return Integer.reverseBytes(access.getUnsignedShort(input, offset)) >>> 16;
    }
    @Override
    public int getShort(final T input, final long offset) {
      return Integer.reverseBytes(access.getShort(input, offset)) >> 16;
    }
    @Override
    public int getUnsignedByte(final T input, final long offset) {
      return access.getUnsignedByte(input, offset);
    }
    @Override
    public int getByte(final T input, final long offset) {
      return access.getByte(input, offset);
    }
    @Override
    public ByteOrder byteOrder(final T input) {
      return LITTLE_ENDIAN == access.byteOrder(input) ? BIG_ENDIAN : LITTLE_ENDIAN;
    }
    @Override
    protected Access<T> reverseAccess() {
      return access;
    }
  }


  public static long hashUnsafeBytes(Object base, long offset, int length, long seed) {
    final LongHashFunction hasher = LongHashFunction.xx3(seed);
    return hasher.hash(base, INSTANCE, offset, length);
  }
}
