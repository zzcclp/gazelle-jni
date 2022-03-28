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

package com.intel.oap.execution

import java.io._

import com.intel.oap.GazelleJniConfig
import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.util._

case class ColumnarUnsafeRow(col1: Double, col2: String, col1Data: Array[Byte],
                             col2Data: Array[Byte], col3Data: Array[Byte])

class NativeWholestageRowRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    columnarReads: Boolean)
    extends RDD[InternalRow](sc, Nil) {
  val numaBindingInfo = GazelleJniConfig.getConf.numaBindingInfo
  val loadNative = GazelleJniConfig.getConf.loadNative

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => new NativeSubstraitPartition(index, inputPartition)
    }.toArray
  }

  private def castPartition(split: Partition): NativeSubstraitPartition = split match {
    case p: NativeSubstraitPartition => p
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  private def castNativePartition(split: Partition): BaseNativeFilePartition = split match {
    case NativeSubstraitPartition(_, p: NativeFilePartition) => p
    case NativeSubstraitPartition(_, m: NativeMergeTreePartition) => m
    case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
  }

  /*override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    ExecutorManager.tryTaskSet(numaBindingInfo)

    val inputPartition = castNativePartition(split)

    var resIter : RowIterator = null
    if (loadNative) {
      val transKernel = new ExpressionEvaluator()
      val inBatchIters = new java.util.ArrayList[ColumnarNativeIterator]()
      var startTime = System.nanoTime()
      resIter = transKernel.createKernelWithRowIterator(inputPartition.substraitPlan, inBatchIters)
      logWarning(s"===========create ${System.nanoTime() - startTime}")
    }

    val iter = new Iterator[InternalRow] with AutoCloseable {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics
      private[this] var currentIterator: Iterator[InternalRow] = null
      private var totalBatch = 0

      override def hasNext: Boolean = {
        if (loadNative) {
          val hasNextRes = (currentIterator != null && currentIterator.hasNext) || nextIterator()
          hasNextRes
        } else {
          false
        }
      }

      private def nextIterator(): Boolean = {
        var startTime = System.nanoTime()
        if (resIter.hasNext) {
          logWarning(s"===========hasNext ${totalBatch} ${System.nanoTime() - startTime}")
          startTime = System.nanoTime()
          val sparkRowInfo = resIter.next()
          totalBatch += 1
          logWarning(s"===========next ${totalBatch} ${System.nanoTime() - startTime}")
          val result = if (sparkRowInfo.offsets != null && sparkRowInfo.offsets.length > 0) {
            val numRows = sparkRowInfo.offsets.length
            val numFields = sparkRowInfo.fieldsNum
            currentIterator = new Iterator[InternalRow] with AutoCloseable {

              var rowId = 0
              val row = new UnsafeRow(numFields.intValue())

              override def hasNext: Boolean = {
                rowId < numRows
              }

              override def next(): InternalRow = {
                if (rowId >= numRows) throw new NoSuchElementException
                val (offset, length) = (sparkRowInfo.offsets(rowId), sparkRowInfo.lengths(rowId))
                row.pointTo(null, sparkRowInfo.memoryAddress + offset, length.toInt)
                rowId += 1
                row
              }

              override def close(): Unit = {}
            }
            true
          } else {
            false
          }
          result
        } else {
          false
        }
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        val cb = currentIterator.next()
        cb
      }

      override def close(): Unit = {
        var startTime = System.nanoTime()
        if (resIter != null) {
          resIter.close()
        }
        logWarning(s"===========close ${System.nanoTime() - startTime}")
      }
    }
    context.addTaskCompletionListener[Unit] { _ =>
      iter.close()
    }
    iter
  }*/

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    ExecutorManager.tryTaskSet(numaBindingInfo)

    val inputPartition = castNativePartition(split)

    val columnarEncoder = Encoders.product[ColumnarUnsafeRow]
    val columnarExprEncoder = columnarEncoder.asInstanceOf[ExpressionEncoder[ColumnarUnsafeRow]]
    val res = Array.range(0, 10).map(i => {
      columnarExprEncoder.createSerializer().apply(ColumnarUnsafeRow(1.0, "F",
        // compressed columnar data
        Array[Byte](192.toByte, 168.toByte, 1, 9),
        Array[Byte](192.toByte, 168.toByte, 1, 10),
        Array[Byte](192.toByte, 168.toByte, 1, 11)))
      match {
        case ur: UnsafeRow => {
          println(ur)
          ur
        }
      }
    })
    var resIter = res.iterator

    val iter = new Iterator[InternalRow] with AutoCloseable {
      private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

      override def hasNext: Boolean = {
        resIter.hasNext
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        resIter.next()
      }

      override def close(): Unit = {
      }
    }
    context.addTaskCompletionListener[Unit] { _ =>
      iter.close()
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.preferredLocations()
  }

}
