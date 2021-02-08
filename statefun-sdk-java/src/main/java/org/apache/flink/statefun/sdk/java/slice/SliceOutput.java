/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.sdk.java.slice;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class SliceOutput {
  private byte[] buf;
  private int position;

  public static SliceOutput sliceOutput(int initialSize) {
    return new SliceOutput(initialSize);
  }

  private SliceOutput(int initialSize) {
    this.buf = new byte[initialSize];
    this.position = 0;
  }

  public void write(byte[] buffer, int offset, int len) {
    Objects.requireNonNull(buffer);
    if (offset < 0 || offset > buffer.length) {
      throw new IllegalArgumentException("Offset out of range " + offset);
    }
    if (len < 0) {
      throw new IllegalArgumentException("Negative length " + len);
    }
    ensureCapacity(position + len);
    System.arraycopy(buffer, offset, buf, position, len);
    position += len;
  }

  public void write(ByteBuffer buffer) {
    int n = buffer.remaining();
    ensureCapacity(position + n);
    buffer.get(buf, position, n);
  }

  public void write(Slice slice) {
    write(slice.asReadOnlyByteBuffer());
  }

  public Slice copyOf() {
    return Slices.copyOf(buf, 0, position);
  }

  public void reset() {
    position = 0;
  }

  private void ensureCapacity(int requiredNewLength) {
    if (requiredNewLength >= buf.length) {
      this.buf = Arrays.copyOf(buf, 2 * requiredNewLength);
    }
  }
}
