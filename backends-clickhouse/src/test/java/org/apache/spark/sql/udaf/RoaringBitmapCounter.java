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
package org.apache.spark.sql.udaf;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/** A {@link BitmapCounter} based on roaring bitmap. */
public class RoaringBitmapCounter implements BitmapCounter, Serializable {

  private Roaring64NavigableMap bitmap;
  private Long counter;

  public RoaringBitmapCounter() {
    // Roaring64NavigableMap.SERIALIZATION_MODE =
    //     Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE;
    bitmap = new Roaring64NavigableMap();
  }

  RoaringBitmapCounter(Roaring64NavigableMap bitmap) {
    this.bitmap = bitmap;
  }

  RoaringBitmapCounter(long counter) {
    this.bitmap = new Roaring64NavigableMap();
    this.counter = counter;
  }

  @Override
  public void orWith(BitmapCounter another) {
    if (another instanceof RoaringBitmapCounter) {
      RoaringBitmapCounter input = (RoaringBitmapCounter) another;
      bitmap.or(input.bitmap);
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported type: " + another.getClass().getCanonicalName());
  }

  @Override
  public void andWith(BitmapCounter another) {
    if (another instanceof RoaringBitmapCounter) {
      RoaringBitmapCounter input = (RoaringBitmapCounter) another;
      bitmap.and(input.bitmap);
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported type: " + another.getClass().getCanonicalName());
  }

  @Override
  public void add(long value) {
    bitmap.add(value);
  }

  public void clear() {
    bitmap = new Roaring64NavigableMap();
  }

  public long getCount() {
    if (counter != null) {
      return counter;
    }

    return bitmap.getLongCardinality();
  }

  public int getMemBytes() {
    return bitmap.getSizeInBytes();
  }

  public Iterator<Long> iterator() {
    return bitmap.iterator();
  }

  public void write(ByteBuffer out) throws IOException {
    if (bitmap instanceof Roaring64NavigableMap) {
      bitmap.runOptimize();
    }

    if (out.remaining() < bitmap.getSizeInBytes()) {
      throw new BufferOverflowException();
    }
    try (DataOutputStream dos = new DataOutputStream(new ByteBufferOutputStream(out))) {
      bitmap.serialize(dos);
    }
  }

  public void serializeCH0940(ByteBuffer buffer) {
    int size = (int) bitmap.serializedSizeInBytes();
    // TODO use custom data output so that we can handle large byte array
    try (ByteArrayOutputStream bas = new ByteArrayOutputStream(size)) {
      DataOutput out = new DataOutputStream(bas);
      try {
        // https://github.com/RoaringBitmap/RoaringBitmap/blob/0.9.9/RoaringBitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java#L1105
        // bitmap.serializePortable(out);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to serialize given bitmap", e);
      }

      buffer.put((byte) 1);
      // buffer.putLong(Long.reverseBytes(bitmap.serializedSizeInBytes()));
      RoaringBitmapJniTest.writeVarInt(buffer, bitmap.serializedSizeInBytes());
      byte[] bytes = bas.toByteArray();
      buffer.put(bytes, 0, size);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize given bitmap", e);
    }
  }

  public void write(ByteArrayOutputStream baos) throws IOException {
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      bitmap.serialize(dos);
    }
  }

  public void readFields(ByteBuffer in) throws IOException {
    ByteBufferBackedInputStream bbi = new ByteBufferBackedInputStream(in);
    bitmap.deserialize(new DataInputStream(bbi));
  }

  public int peekLength(ByteBuffer in) {
    // The current peeklength method has no meaning
    throw new UnsupportedOperationException();
  }

  public boolean equals(Object obj) {
    return (obj instanceof RoaringBitmapCounter)
        && bitmap.equals(((RoaringBitmapCounter) obj).bitmap);
  }

  public int hashCode() {
    return bitmap.hashCode();
  }

  public String toString() {
    return "RoaringBitmapCounter[" + getCount() + "]";
  }
}
