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

import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.extension.ExpressionExtensionTrait
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode, Final, Partial, TypedImperativeAggregate}
import org.apache.spark.sql.types.{BinaryType, DataType, LongType}

import com.google.common.collect.Lists
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.mutable.ListBuffer

class KeBitmapFunctionTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class CustomerExpressionTransformer() extends ExpressionExtensionTrait {

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  def expressionSigList: Seq[Sig] = Seq(
    Sig[PreciseCardinality]("ke_bitmap_cardinality"),
    Sig[PreciseCountDistinctDecode]("ke_bitmap_cardinality"),
    Sig[ReusePreciseCountDistinct]("ke_bitmap_or_data"),
    Sig[PreciseCountDistinctAndValue]("ke_bitmap_and_value"),
    Sig[PreciseCountDistinctAndArray]("ke_bitmap_and_ids"),
    Sig[PreciseCountDistinct]("ke_bitmap_or_cardinality")
  )

  /** Replace extension expression to transformer. */
  override def replaceWithExtensionExpressionTransformer(
      substraitExprName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = expr match {
    case preciseCardinality: PreciseCardinality =>
      new KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCardinality.child, attributeSeq),
        preciseCardinality
      )
    case preciseCountDistinctDecode: PreciseCountDistinctDecode =>
      new KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCountDistinctDecode.child, attributeSeq),
        preciseCountDistinctDecode
      )
    case other =>
      throw new UnsupportedOperationException(
        s"${expr.getClass} or $expr is not currently supported.")
  }

  /** Get the attribute index of the extension aggregate functions. */
  override def getAttrsIndexForExtensionAggregateExpr(
      aggregateFunc: AggregateFunction,
      mode: AggregateMode,
      exp: AggregateExpression,
      aggregateAttributeList: Seq[Attribute],
      aggregateAttr: ListBuffer[Attribute],
      resIndex: Int): Int = {
    var reIndex = resIndex
    aggregateFunc match {
      case bitmap
          if bitmap.getClass.getSimpleName.equals("ReusePreciseCountDistinct") ||
            bitmap.getClass.getSimpleName.equals("PreciseCountDistinctAndValue") ||
            bitmap.getClass.getSimpleName.equals("PreciseCountDistinctAndArray") ||
            bitmap.getClass.getSimpleName.equals("PreciseCountDistinct") =>
        mode match {
          case Partial =>
            val bitmapFunc = aggregateFunc
              .asInstanceOf[TypedImperativeAggregate[Roaring64NavigableMap]]
            val aggBufferAttr = bitmapFunc.inputAggBufferAttributes
            for (index <- aggBufferAttr.indices) {
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
              aggregateAttr += attr
            }
            reIndex += aggBufferAttr.size
            reIndex
          case Final =>
            aggregateAttr += aggregateAttributeList(resIndex)
            reIndex += 1
            reIndex
          case other =>
            throw new UnsupportedOperationException(s"Unsupported aggregate mode: $other.")
        }
    }
  }

  /** Get the custom agg function substrait name and the input types of the child */
  override def buildCustomAggregateFunction(
      aggregateFunc: AggregateFunction): (Option[String], Seq[DataType]) = {
    val substraitAggFuncName = aggregateFunc match {
      case countDistinct: PreciseCountDistinct =>
        countDistinct.dataType match {
          case LongType =>
            Some("ke_bitmap_or_cardinality")
          case BinaryType =>
            Some("ke_bitmap_or_data")
          case _ =>
            throw new UnsupportedOperationException(
              s"Aggregate function ${aggregateFunc.getClass} does not support the data type " +
                s"${countDistinct.dataType}.")
        }
      case _ =>
        extensionExpressionsMapping.get(aggregateFunc.getClass)
    }
    if (substraitAggFuncName.isEmpty) {
      throw new UnsupportedOperationException(
        s"Aggregate function ${aggregateFunc.getClass} is not supported.")
    }
    (substraitAggFuncName, aggregateFunc.children.map(child => child.dataType))
  }
}
