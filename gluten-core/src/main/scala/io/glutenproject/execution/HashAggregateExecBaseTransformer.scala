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
package io.glutenproject.execution

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.types._

import com.google.protobuf.StringValue

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._

/** Columnar Based HashAggregateExec. */
abstract class HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends BaseAggregateExec
  with UnaryTransformSupport {

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genHashAggregateTransformerMetrics(sparkContext)

  // The direct outputs of Aggregation.
  protected lazy val allAggregateResultAttributes: List[Attribute] = {
    groupingExpressions.map(ConverterUtils.getAttrFromExpr(_)).toList :::
      getAttrForAggregateExprs(aggregateExpressions, aggregateAttributes).toList
  }

  protected def isCapableForStreamingAggregation: Boolean = {
    if (!conf.getConf(GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE)) {
      return false
    }
    if (groupingExpressions.isEmpty) {
      return false
    }

    val childOrdering = child match {
      case agg: HashAggregateExecBaseTransformer
          if agg.groupingExpressions == this.groupingExpressions =>
        // If the child aggregate supports streaming aggregate then the ordering is not changed.
        // So we can propagate ordering if there is no shuffle exchange between aggregates and
        // they have same grouping keys,
        agg.child.outputOrdering
      case _ => child.outputOrdering
    }
    val requiredOrdering = groupingExpressions.map(expr => SortOrder.apply(expr, Ascending))
    SortOrder.orderingSatisfies(childOrdering, requiredOrdering)
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHashAggregateTransformerMetricsUpdater(metrics)

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions
    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"HashAggregateTransformer(keys=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"HashAggregateTransformer(keys=$keyString, functions=$functionString)"
    }
  }

  // override def canEqual(that: Any): Boolean = false

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  protected def checkType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | StringType | TimestampType | DateType | BinaryType =>
        true
      case _: NumericType => true
      case _: ArrayType => true
      case _: NullType => true
      case _ => false
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    val aggParams = new AggregationParams
    val relNode = getAggRel(substraitContext, operatorId, aggParams, null, validation = true)

    val unsupportedAggExprs = aggregateAttributes.filterNot(attr => checkType(attr.dataType))
    if (unsupportedAggExprs.nonEmpty) {
      return ValidationResult.notOk(
        "Found unsupported data type in aggregation expression: " +
          unsupportedAggExprs
            .map(attr => s"${attr.name}#${attr.exprId.id}:${attr.dataType}")
            .mkString(", "))
    }
    val unsupportedGroupExprs = groupingExpressions.filterNot(attr => checkType(attr.dataType))
    if (unsupportedGroupExprs.nonEmpty) {
      return ValidationResult.notOk(
        "Found unsupported data type in grouping expression: " +
          unsupportedGroupExprs
            .map(attr => s"${attr.name}#${attr.exprId.id}:${attr.dataType}")
            .mkString(", "))
    }
    aggregateExpressions.foreach {
      expr =>
        if (!checkAggFuncModeSupport(expr.aggregateFunction, expr.mode)) {
          throw new UnsupportedOperationException(
            s"Unsupported aggregate mode: ${expr.mode} for ${expr.aggregateFunction.prettyName}")
        }
    }
    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)

    val aggParams = new AggregationParams
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getAggRel(context, operatorId, aggParams, childCtx.root)
    TransformContext(childCtx.outputAttributes, output, relNode)
  }

  // Members declared in org.apache.spark.sql.execution.AliasAwareOutputPartitioning
  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  // Check if Pre-Projection is needed before the Aggregation.
  protected def needsPreProjection: Boolean = {
    groupingExpressions.exists {
      case _: Attribute => false
      case _ => true
    } || aggregateExpressions.exists {
      expr =>
        expr.filter match {
          case None | Some(_: Attribute) | Some(_: Literal) =>
          case _ => return true
        }
        expr.mode match {
          case Partial =>
            expr.aggregateFunction.children.exists {
              case _: Attribute | _: Literal => false
              case _ => true
            }
          // No need to consider pre-projection for PartialMerge and Final Agg.
          case _ => false
        }
    }
  }

  // Check if Post-Projection is needed after the Aggregation.
  protected def needsPostProjection(aggOutAttributes: List[Attribute]): Boolean = {
    // If the result expressions has different size with output attribute,
    // post-projection is needed.
    resultExpressions.size != aggOutAttributes.size ||
    // Compare each item in result expressions and output attributes.
    resultExpressions.zip(aggOutAttributes).exists {
      case (exprAttr: Attribute, resAttr) =>
        // If the result attribute and result expression has different name or type,
        // post-projection is needed.
        exprAttr.name != resAttr.name || exprAttr.dataType != resAttr.dataType
      case _ =>
        // If result expression is not instance of Attribute,
        // post-projection is needed.
        true
    }
  }

  protected def getAggRelWithPreProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode = null,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Will add a Projection before Aggregate.
    // Logic was added to prevent selecting the same column for more than once,
    // so the expression in preExpressions will be unique.
    var preExpressions: Seq[Expression] = Seq()
    var selections: Seq[Int] = Seq()
    // Indices of filter used columns.
    var filterSelections: Seq[Int] = Seq()

    def appendIfNotFound(expression: Expression): Unit = {
      val foundExpr = preExpressions.find(e => e.semanticEquals(expression)).orNull
      if (foundExpr != null) {
        // If found, no need to add it to preExpressions again.
        // The selecting index will be found.
        selections = selections :+ preExpressions.indexOf(foundExpr)
      } else {
        // If not found, add this expression into preExpressions.
        // A new selecting index will be created.
        preExpressions = preExpressions :+ expression.clone()
        selections = selections :+ (preExpressions.size - 1)
      }
    }

    // Get the needed expressions from grouping expressions.
    groupingExpressions.foreach(expression => appendIfNotFound(expression))

    val hasDistinct = aggregateExpressions.exists(_.isDistinct)
    // Get the needed expressions from aggregation expressions.
    aggregateExpressions.foreach(
      aggExpr => {
        val aggregateFunc = aggExpr.aggregateFunction
        aggExpr.mode match {
          case Partial =>
            aggregateFunc.children.foreach(appendIfNotFound)
          case PartialMerge =>
            // For aggregate with distinct
            assert(
              hasDistinct,
              s"Not support PartialMerge for non-distinct aggregate: $aggregateFunc")
            aggregateFunc.inputAggBufferAttributes.foreach(appendIfNotFound)
          case Complete =>
            aggregateFunc.children.foreach(appendIfNotFound)
          case other =>
            throw new UnsupportedOperationException(s"$other not supported.")
        }
      })

    // Handle expressions used in Aggregate filter.
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          appendIfNotFound(aggExpr.filter.orNull)
          filterSelections = filterSelections :+ selections.last
        }
      })

    // Create the expression nodes needed by Project node.
    val preExprNodes = preExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, originalInputAttributes)
          .doTransform(args))
      .asJava
    val emitStartIndex = originalInputAttributes.size
    val inputRel = if (!validation) {
      RelBuilder.makeProjectRel(input, preExprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input,
        preExprNodes,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }

    // Handle the pure Aggregate after Projection. Both grouping and Aggregate expressions are
    // selections.
    getAggRelAfterProject(context, selections, filterSelections, inputRel, operatorId)
  }

  protected def getAggRelAfterProject(
      context: SubstraitContext,
      selections: Seq[Int],
      filterSelections: Seq[Int],
      inputRel: RelNode,
      operatorId: Long): RelNode = {
    val groupingList = new JArrayList[ExpressionNode]()
    var colIdx = 0
    while (colIdx < groupingExpressions.size) {
      val groupingExpr: ExpressionNode = ExpressionBuilder.makeSelection(selections(colIdx))
      groupingList.add(groupingExpr)
      colIdx += 1
    }

    // Create Aggregation functions.
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodeList = new JArrayList[ExpressionNode]()
        aggExpr.mode match {
          case Partial | Complete =>
            aggregateFunc.children.foreach {
              _ =>
                val aggExpr = ExpressionBuilder.makeSelection(selections(colIdx))
                colIdx += 1
                childrenNodeList.add(aggExpr)
            }
          case PartialMerge =>
            // For aggregate with distinct
            aggregateFunc.inputAggBufferAttributes.foreach {
              _ =>
                val aggExpr = ExpressionBuilder.makeSelection(selections(colIdx))
                colIdx += 1
                childrenNodeList.add(aggExpr)
            }
          case other =>
            throw new UnsupportedOperationException(s"$other not supported.")
        }
        addFunctionNode(
          context.registeredFunction,
          aggregateFunc,
          childrenNodeList,
          aggExpr.mode,
          aggregateFunctionList)
      })

    val aggFilterList = new JArrayList[ExpressionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          aggFilterList.add(ExpressionBuilder.makeSelection(selections(colIdx)))
          colIdx += 1
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }
      })

    val extensionNode = getAdvancedExtension()
    RelBuilder.makeAggregateRel(
      inputRel,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  protected def addFunctionNode(
      args: java.lang.Object,
      aggregateFunction: AggregateFunction,
      childrenNodeList: JList[ExpressionNode],
      aggregateMode: AggregateMode,
      aggregateNodeList: JList[AggregateFunctionNode]): Unit = {
    aggregateNodeList.add(
      ExpressionBuilder.makeAggregateFunction(
        AggregateFunctionsBuilder.create(args, aggregateFunction),
        childrenNodeList,
        modeToKeyWord(aggregateMode),
        ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
      ))
  }

  protected def applyPostProjection(
      context: SubstraitContext,
      aggRel: RelNode,
      operatorId: Long,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction

    // Will add an projection after Agg.
    val resExprNodes = resultExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, allAggregateResultAttributes)
          .doTransform(args))
      .asJava
    val emitStartIndex = allAggregateResultAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(aggRel, resExprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = allAggregateResultAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        aggRel,
        resExprNodes,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }
  }

  /** This method calculates the output attributes of Aggregation. */
  protected def getAttrForAggregateExprs(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributeList: Seq[Attribute]): Seq[Attribute]

  protected def checkAggFuncModeSupport(
      aggFunc: AggregateFunction,
      mode: AggregateMode): Boolean = {
    aggFunc match {
      case _: CollectList | _: CollectSet =>
        mode match {
          case Partial | Final | Complete => true
          case _ => false
        }
      case bloom if bloom.getClass.getSimpleName.equals("BloomFilterAggregate") =>
        mode match {
          case Partial | Final | Complete => true
          case _ => false
        }
      case _ =>
        mode match {
          case Partial | PartialMerge | Final | Complete => true
          case _ => false
        }
    }
  }

  protected def modeToKeyWord(aggregateMode: AggregateMode): String = {
    aggregateMode match {
      case Partial => "PARTIAL"
      case PartialMerge => "PARTIAL_MERGE"
      case Complete => "COMPLETE"
      case Final => "FINAL"
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }

  protected def getAggRelWithoutPreProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode = null,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Get the grouping nodes.
    // Use 'child.output' as based Seq[Attribute], the originalInputAttributes
    // may be different for each backend.
    val groupingList = groupingExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, child.output)
          .doTransform(args))
      .asJava
    // Get the aggregate function nodes.
    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(aggExpr.filter.get, child.output)
            .doTransform(args)
          aggFilterList.add(exprNode)
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }
        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodes = aggExpr.mode match {
          case Partial | Complete =>
            aggregateFunc.children.toList.map(
              expr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(expr, originalInputAttributes)
                  .doTransform(args)
              })
          case PartialMerge | Final =>
            aggregateFunc.inputAggBufferAttributes.toList.map(
              attr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                  .doTransform(args)
              })
          case other =>
            throw new UnsupportedOperationException(s"$other not supported.")
        }
        addFunctionNode(
          args,
          aggregateFunc,
          childrenNodes.asJava,
          aggExpr.mode,
          aggregateFunctionList)
      })

    val extensionNode = getAdvancedExtension(validation, originalInputAttributes)
    RelBuilder.makeAggregateRel(
      input,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  protected def getAdvancedExtension(
      validation: Boolean = false,
      originalInputAttributes: Seq[Attribute] = Seq.empty): AdvancedExtensionNode = {
    val enhancement = if (validation) {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf)
    } else {
      null
    }

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder
          .setValue(formatExtOptimizationString(isCapableForStreamingAggregation))
          .build)
    ExtensionBuilder.makeAdvancedExtension(optimization, enhancement)
  }

  protected def formatExtOptimizationString(isStreaming: Boolean): String = {
    s"isStreaming=${if (isStreaming) "1" else "0"}\n"
  }

  protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode
}

object HashAggregateExecTransformerUtil {
  // Return whether the outputs partial aggregation should be combined for Velox computing.
  // When the partial outputs are multiple-column, row construct is needed.
  def rowConstructNeeded(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.exists {
      aggExpr =>
        aggExpr.mode match {
          case PartialMerge | Final =>
            aggExpr.aggregateFunction.inputAggBufferAttributes.size > 1
          case _ => false
        }
    }
  }
}
