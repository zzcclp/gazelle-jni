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
package io.glutenproject.extension

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager

import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}

case class RemoveUnnecessaryHashAggregate() extends Rule[SparkPlan] {

  /** check whether the parent aggregate and the child aggregate are a group. */
  def isTheGroupOfAggregate(
      childGroupingExprs: Seq[NamedExpression],
      childAggregateExprs: Seq[AggregateExpression],
      groupingExprs: Seq[NamedExpression],
      aggregateExprs: Seq[AggregateExpression]): Boolean = {
    val childGroupingAttributes = childGroupingExprs.map(_.toAttribute)
    val childAggregateExpressions = childAggregateExprs.map(_.copy(mode = Complete))
    val currGroupingAttributes = groupingExprs.map(_.asInstanceOf[Attribute])
    val currAggregateExpressions = aggregateExprs.map(_.copy(mode = Complete))
    val aggExprIsSame = currAggregateExpressions.size == childAggregateExpressions.size &&
      currAggregateExpressions.zip(childAggregateExpressions).forall {
        case (currAggExpr, childAggExpr) =>
          currAggExpr.canonicalized == childAggExpr.canonicalized
      }
    val groupingAttrsIsSame = currGroupingAttributes.sameElements(childGroupingAttributes)

    aggExprIsSame && groupingAttrsIsSame
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (
      !GlutenConfig.getConf.enableGluten ||
      !BackendsApiManager.getSettings.removeUnnecessaryHashAggregate()
    ) {
      return plan
    }

    val newPlan = plan.transformUp {
      case hashAgg @ HashAggregateExec(
            _,
            isStreaming,
            _,
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            _,
            _,
            child: HashAggregateExec) if !isStreaming =>
        if (
          isTheGroupOfAggregate(
            child.groupingExpressions,
            child.aggregateExpressions,
            groupingExpressions,
            aggregateExpressions)
        ) {
          // convert to complete mode aggregate expressions
          val currAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
          HashAggregateExec(
            requiredChildDistributionExpressions = None,
            isStreaming = isStreaming,
            numShufflePartitions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = currAggregateExpressions,
            aggregateAttributes = aggregateAttributes,
            initialInputBufferOffset = 0,
            resultExpressions = hashAgg.resultExpressions,
            child = child.child
          )
        } else {
          hashAgg
        }
      case objectHashAgg @ ObjectHashAggregateExec(
            _,
            isStreaming,
            _,
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            _,
            _,
            child: ObjectHashAggregateExec) if !isStreaming =>
        if (
          isTheGroupOfAggregate(
            child.groupingExpressions,
            child.aggregateExpressions,
            groupingExpressions,
            aggregateExpressions)
        ) {
          // convert to complete mode aggregate expressions
          val currAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
          ObjectHashAggregateExec(
            requiredChildDistributionExpressions = None,
            isStreaming = isStreaming,
            numShufflePartitions = None,
            groupingExpressions = child.groupingExpressions,
            aggregateExpressions = currAggregateExpressions,
            aggregateAttributes = aggregateAttributes,
            initialInputBufferOffset = 0,
            resultExpressions = objectHashAgg.resultExpressions,
            child = child.child
          )
        } else {
          objectHashAgg
        }
      case plan: SparkPlan => plan
    }
    newPlan
  }
}
