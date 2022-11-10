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
package io.glutenproject.plan

import com.google.protobuf.util.JsonFormat
import io.substrait.plan.PlanProtoConverter
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.TPCHBase
import org.apache.spark.sql.execution.SparkPlan

class ScanTest extends TPCHBase with Logging {

  def printSubstraitPlan(s: SparkPlan): String = {
    val stages = s.collect { case p: GlutenWholeStage => p }
    stages
      .map(_.doSubstraitGen())
      .map((new PlanProtoConverter).toProto)
      .map(JsonFormat.printer.print)
      .mkString("\n==================================================================\n")
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.codegen.wholeStage", "false")
      .set("spark.sql.adaptive.enabled", "false")
  }

  protected def PhysicalPlan(sql: String): SparkPlan = spark.sql(sql).queryExecution.executedPlan

  test("testReadRel") {
    val plan = PhysicalPlan("select l_orderkey from lineitem")
    val planner = new GlutenPlanner
    val newPlan = planner.plan(plan)
    logInfo("\n" + newPlan.treeString)
    val sparkPlan = ReplacePlaceHolder.apply(newPlan)
    logInfo("\n" + sparkPlan.treeString)
    logInfo("\n" + printSubstraitPlan(sparkPlan))
  }
}
