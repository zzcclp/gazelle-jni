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
package org.apache.spark.sql.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.metrics.GlutenTimeMetric

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

import scala.concurrent.Future
import scala.concurrent.duration.Duration

case class ColumnarSubqueryBroadcastProxyExec(
    name: String,
    index: Int,
    buildKeys: Seq[Expression],
    child: SparkPlan)
  extends BaseSubqueryExec
  with UnaryExecNode
  with GlutenPlan {

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = exchangeChild.output

  lazy val exchangeChild = child match {
    case queryStageExec: QueryStageExec if queryStageExec.plan.isInstanceOf[ReusedExchangeExec] =>
      queryStageExec.plan.asInstanceOf[ReusedExchangeExec].child
    case queryStageExec: QueryStageExec =>
      queryStageExec.plan
  }

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))

  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    copy(name = "ColumnarSubqueryBroadcastProxyExec", buildKeys = keys, child = child.canonicalized)
  }

  @transient
  private lazy val relationFuture: Future[Array[Byte]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(session, executionId) {
        val batches = GlutenTimeMetric.millis(longMetric("collectTime")) {
          _ =>
            exchangeChild
              .executeBroadcast[BuildSideRelation]
              .value
              .asByteArray
        }
        longMetric("dataSize") += batches.length
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        batches
      }
    }(SubqueryBroadcastExec.executionContext)
  }

  override protected def doPrepare(): Unit = {
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "SubqueryBroadcastExec does not support the execute() code path.")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // convert block byte array to RDD[ColumnarBatch]
    val value = ThreadUtils.awaitResult(relationFuture, Duration.Inf)
    sparkContext
      .parallelize(value.toSeq, 1)
      .mapPartitionsInternal(
        iter => {
          BackendsApiManager.getIteratorApiInstance.genBroadcastExchangeIterator(iter.toArray)
        })
  }

  override def executeCollect(): Array[InternalRow] = {
    throw new UnsupportedOperationException(
      "ColumnarSubqueryBroadcastProxyExec does not support the executeCollect() code path.")
  }

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")

  protected def withNewChildInternal(newChild: SparkPlan): ColumnarSubqueryBroadcastProxyExec =
    copy(child = newChild)
}
