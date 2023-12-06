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

package org.apache.spark.io;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import org.apache.spark.unsafe.Platform;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * A pool of direct buffers which uses a simple reference queue to recycle buffers.
 *
 * @see com.github.luben.zstd.BufferPool
 * @see com.github.luben.zstd.RecyclingBufferPool
 */
public class OffHeapRecyclingBufferPool implements BufferPool {

  public static final BufferPool INSTANCE = new OffHeapRecyclingBufferPool();

  private static final int buffSize = Math.max(Math.max(
          (int) ZstdOutputStreamNoFinalizer.recommendedCOutSize(),
          (int) ZstdInputStreamNoFinalizer.recommendedDInSize()),
      (int) ZstdInputStreamNoFinalizer.recommendedDOutSize());

  private final ConcurrentLinkedQueue<SoftReference<ByteBuffer>> pool;

  private OffHeapRecyclingBufferPool() {
    this.pool = new ConcurrentLinkedQueue<SoftReference<ByteBuffer>>();
  }

  @Override
  public ByteBuffer get(int capacity) {
    if (capacity > buffSize) {
      throw new RuntimeException(
          "Unsupported buffer size: " + capacity +
              ". Supported buffer sizes: " + buffSize + " or smaller."
      );
    }
    while(true) {
      SoftReference<ByteBuffer> sbuf = null;

      // This if statement introduces a possible race condition of allocating a buffer while we're trying to
      // release one. However, the extra allocation should be considered insignificant in terms of cost.
      // Particularly with respect to throughput.
      if (!pool.isEmpty()) {
        sbuf = pool.poll();
      }

      if (sbuf == null) {
        return Platform.allocateDirectBuffer(buffSize);
      }
      ByteBuffer buf = sbuf.get();
      if (buf != null) {
        return buf;
      }
    }
  }

  @Override
  public void release(ByteBuffer buffer) {
    if (buffer.capacity() >= buffSize) {
      buffer.clear();
      pool.add(new SoftReference<>(buffer));
    }
  }
}
