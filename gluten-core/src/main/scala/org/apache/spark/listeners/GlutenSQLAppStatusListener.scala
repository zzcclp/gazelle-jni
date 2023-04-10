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
package org.apache.spark.listeners

import org.apache.spark.{JobExecutionStatus, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{GlutenDeleteResource, GlutenDriverEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.status.LiveEntity

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.Date
import scala.collection.JavaConverters._

class GlutenSQLAppStatusListener(conf: SparkConf) extends SparkListener with Logging {

  // Live tracked data is needed by the SQL status store to calculate metrics for in-flight
  // executions; that means arbitrary threads may be querying these maps, so they need to be
  // thread-safe.
  private val liveExecutions = new ConcurrentHashMap[Long, LiveExecutionData]()

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val executionIdString = event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }

    val executionId = executionIdString.toLong
    val jobId = event.jobId
    val exec = Option(liveExecutions.get(executionId))
      .getOrElse(getOrCreateExecution(executionId))

    logError(s"------------------- job ${event.jobId} start with execution id: ${executionId}")
    exec.jobs = exec.jobs + (jobId -> JobExecutionStatus.RUNNING)
    exec.stages ++= event.stageIds.toSet
    update(exec, force = true)
  }

  private def getOrCreateExecution(executionId: Long): LiveExecutionData = {
    liveExecutions.computeIfAbsent(executionId,
      (_: Long) => {
        logError(s"------------------- creat liveExecutions for new execution id: ${executionId}")
        new LiveExecutionData(executionId)
      })
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    liveExecutions.values().asScala.foreach { exec =>
      if (exec.jobs.contains(event.jobId)) {
        val result = event.jobResult match {
          case JobSucceeded => JobExecutionStatus.SUCCEEDED
          case _ => JobExecutionStatus.FAILED
        }
        logError(
          s"------------------- job ${event.jobId} end with execution id: ${exec.executionId}")
        exec.jobs = exec.jobs + (event.jobId -> result)
        exec.endEvents.incrementAndGet()
        update(exec, force = true)
      }
    }
  }

  private def update(exec: LiveExecutionData, force: Boolean = false): Unit = {
    val now = System.nanoTime()
    if (exec.endEvents.get() >= exec.jobs.size + 1) {
      logError(s"------------------- remove execution id: ${exec.executionId}")
      liveExecutions.remove(exec.executionId)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLAdaptiveExecutionUpdate => onAdaptiveExecutionUpdate(e)
    case e: SparkListenerSQLAdaptiveSQLMetricUpdates => onAdaptiveSQLMetricUpdate(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    // case e: SparkListenerDriverAccumUpdates => onDriverAccumUpdates(e)
    case _ => // Ignore
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val planGraph = SparkPlanGraph(event.sparkPlanInfo)
    val sqlPlanMetrics = planGraph.allNodes.flatMap { node =>
      node.metrics.map { metric => (metric.accumulatorId, metric) }
    }.toMap.values.toList

    val exec = getOrCreateExecution(event.executionId)
    exec.description = event.description
    exec.details = event.details
    exec.physicalPlanDescription = event.physicalPlanDescription
    exec.metrics = sqlPlanMetrics
    exec.submissionTime = event.time
    logError(s"------------------- execution id: ${event.executionId} start ")
    update(exec)
  }

  private def onAdaptiveExecutionUpdate(event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    val SparkListenerSQLAdaptiveExecutionUpdate(
    executionId, physicalPlanDescription, sparkPlanInfo) = event

    val planGraph = SparkPlanGraph(sparkPlanInfo)
    val sqlPlanMetrics = planGraph.allNodes.flatMap { node =>
      node.metrics.map { metric => (metric.accumulatorId, metric) }
    }.toMap.values.toList

    val exec = getOrCreateExecution(executionId)
    exec.physicalPlanDescription = physicalPlanDescription
    exec.metrics ++= sqlPlanMetrics
    update(exec)
  }

  private def onAdaptiveSQLMetricUpdate(event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {
    val SparkListenerSQLAdaptiveSQLMetricUpdates(executionId, sqlPlanMetrics) = event

    val exec = getOrCreateExecution(executionId)
    exec.metrics ++= sqlPlanMetrics
    update(exec)
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val SparkListenerSQLExecutionEnd(executionId, time) = event
    Option(liveExecutions.get(executionId)).foreach { exec =>
      exec.completionTime = Some(new Date(time))
      update(exec)
    }
    logError(s"------------------- execution id: ${event.executionId} end ")
    logError(s"------------------- remove broadcast value: ${GlutenSQLAppStatusListener
      .executionIdToBroadcastValue.get(event.executionId)} end ")

    val deleteResourceIds = GlutenSQLAppStatusListener
      .executionIdToBroadcastValue.get(event.executionId)
    if (deleteResourceIds != null) {
      GlutenDriverEndpoint.executorDataMap.values().forEach { e =>
        e.executorRef.send(GlutenDeleteResource(deleteResourceIds.asScala.mkString(",")))
        GlutenSQLAppStatusListener
          .executionIdToBroadcastValue.remove(event.executionId)
      }
    }
  }
}

private class LiveExecutionData(val executionId: Long) extends LiveEntity {

  var description: String = null
  var details: String = null
  var physicalPlanDescription: String = null
  var metrics = Seq[SQLPlanMetric]()
  var submissionTime = -1L
  var completionTime: Option[Date] = None

  var jobs = Map[Int, JobExecutionStatus]()
  var stages = Set[Int]()
  var driverAccumUpdates = Seq[(Long, Long)]()

  @volatile var metricsValues: Map[Long, String] = null

  // Just in case job end and execution end arrive out of order, keep track of how many
  // end events arrived so that the listener can stop tracking the execution.
  val endEvents = new AtomicInteger()

  override protected def doUpdate(): Any = {
    null
  }

}

object GlutenSQLAppStatusListener {

  val executionIdToBroadcastValue = new ConcurrentHashMap[Long, util.ArrayList[String]]()

  def addToSparkListenerBus(sc: SparkContext, listener: GlutenSQLAppStatusListener): Unit = {
    sc.listenerBus.addToStatusQueue(listener)
  }
}