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
package org.apache.spark.sql.udaf

import org.apache.spark.internal.Logging

import org.roaringbitmap.longlong.Roaring64NavigableMap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

class BitmapSerAndDeSer extends Logging {}

// TODO: remove BitmapSerAndDeSerObj and use BitmapSerAndDeSer eventually
object BitmapSerAndDeSerObj extends BitmapSerAndDeSer

object BitmapSerAndDeSer {
  private val serAndDeSer: ThreadLocal[BitmapSerAndDeSer] = new ThreadLocal[BitmapSerAndDeSer]() {
    override def initialValue(): BitmapSerAndDeSer = new BitmapSerAndDeSer
  }

  def get(): BitmapSerAndDeSer = serAndDeSer.get()

  def getBitmapArray(values: Long*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024000000)
    val count = new RoaringBitmapCounter()
    values.foreach(count.add)
    count.write(buffer)

    buffer.flip()
    if (buffer.limit() == buffer.capacity()) {
      buffer.array()
    } else {
      val result = new Array[Byte](buffer.limit())
      buffer.get(result, 0, buffer.limit())
      result
    }
  }

  def getBitmapArrayCH(values: Long*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024000000)
    val count = new RoaringBitmapCounter()
    values.foreach(count.add)
    count.write(buffer)
    val bitmapBytes = buffer.array()
    val byteSize = buffer.position()
    val result = ByteBuffer.allocate(1024000000)

    // CH BitmapKind::Bitmap flag
    result.put(1.toByte)
    // data size
    RoaringBitmapJniTest.writeVarInt(result, byteSize.toLong + 3L)

    var highToBitmapSize = 0
    highToBitmapSize = (highToBitmapSize << 8) + (bitmapBytes(1) & 0xff)
    highToBitmapSize = (highToBitmapSize << 8) + (bitmapBytes(2) & 0xff)
    highToBitmapSize = (highToBitmapSize << 8) + (bitmapBytes(3) & 0xff)
    highToBitmapSize = (highToBitmapSize << 8) + (bitmapBytes(4) & 0xff)

    result.putLong(java.lang.Long.reverseBytes(highToBitmapSize.toLong))

    var startIdx = 5
    /* for (i <- 0 until highToBitmapSize) {
      var key = 0
      key = (key << 8) + (bitmapBytes(startIdx + 0) & 0xFF)
      key = (key << 8) + (bitmapBytes(startIdx + 1) & 0xFF)
      key = (key << 8) + (bitmapBytes(startIdx + 2) & 0xFF)
      key = (key << 8) + (bitmapBytes(startIdx + 3) & 0xFF)

      println(s"key $key reverseBytes ${java.lang.Integer.reverseBytes(key)}")
      // result.putInt(java.lang.Integer.reverseBytes(key))
      // result.put(bitmapBytes, startIdx + 4, byteSize - startIdx - 4)
    } */
    result.put(bitmapBytes, startIdx, byteSize - startIdx)
    result.array()
  }

  def getBitmapArrayCH0940(values: Long*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024000000)
    val count = new RoaringBitmapCounter()
    values.foreach(count.add)
    count.serializeCH0940(buffer)
    buffer.array()
  }

  def deserialize(bytes: Array[Byte]): Roaring64NavigableMap = {
    val bitMap = new Roaring64NavigableMap()
    if (bytes.nonEmpty) {
      try {
        val dis = new DataInputStream(new ByteArrayInputStream(bytes))
        bitMap.deserialize(dis)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          return new Roaring64NavigableMap()
      }
    }
    bitMap
  }

  def serialize(buffer: Roaring64NavigableMap): Array[Byte] = {
    buffer.runOptimize()
    serialize(buffer, 1024 * 1024)
  }

  final def serialize(buffer: Roaring64NavigableMap, capacity: Int): Array[Byte] = {
    try {
      val bos = new ByteArrayOutputStream(capacity)
      val dos = new DataOutputStream(bos)
      buffer.serialize(dos)
      bos.toByteArray
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return Array.empty[Byte]
    }
  }
}
