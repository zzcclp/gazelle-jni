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
package org.apache.spark.sql.catalyst.expressions

import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.extension.ExpressionExtensionTrait
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StringType

import com.google.common.collect.Lists

class FloorDateTimeExpressionTransformer(
    substraitExprName: String,
    format: ExpressionTransformer,
    timestamp: ExpressionTransformer,
    original: FloorDateTime)
  extends ExpressionTransformer
  with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val formatNode = format.doTransform(args)
    val timestampNode = timestamp.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    // wrap in lower: lower(format)
    val lowerFuncId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName("lower", Seq(StringType), FunctionConfig.OPT))
    val lowerExprNodes = Lists.newArrayList[ExpressionNode](formatNode)
    val lowerTypeNode = TypeBuilder.makeString(original.nullable)
    val lowerFormatNode =
      ExpressionBuilder.makeScalarFunction(lowerFuncId, lowerExprNodes, lowerTypeNode)

    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        Seq(original.format.dataType, original.timestamp.dataType),
        FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(lowerFormatNode, timestampNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class TimestampAddExpressionTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    mid: ExpressionTransformer,
    right: ExpressionTransformer,
    original: TimestampAdd)
  extends ExpressionTransformer
  with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode = left.doTransform(args)
    val midNode = mid.doTransform(args)
    val rightNode = right.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(leftNode, midNode, rightNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class TimestampDiffExpressionTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    mid: ExpressionTransformer,
    right: ExpressionTransformer,
    original: TimestampDiff)
  extends ExpressionTransformer
  with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode = left.doTransform(args)
    val midNode = mid.doTransform(args)
    val rightNode = right.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(leftNode, midNode, rightNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class CustomerExpressionTransformer() extends ExpressionExtensionTrait {

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  def expressionSigList: Seq[Sig] = Seq(
    Sig[FloorDateTime]("floor_datetime"),
    Sig[TimestampAdd]("timestamp_add"),
    Sig[TimestampDiff]("timestamp_diff")
  )

  /** Replace extension expression to transformer. */
  override def replaceWithExtensionExpressionTransformer(
      substraitExprName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = expr match {
    case floorDateTime: FloorDateTime =>
      new FloorDateTimeExpressionTransformer(
        substraitExprName,
        ExpressionConverter.replaceWithExpressionTransformer(floorDateTime.format, attributeSeq),
        ExpressionConverter.replaceWithExpressionTransformer(floorDateTime.timestamp, attributeSeq),
        floorDateTime
      )
    case timestampAdd: TimestampAdd =>
      new TimestampAddExpressionTransformer(
        substraitExprName,
        ExpressionConverter.replaceWithExpressionTransformer(timestampAdd.right, attributeSeq),
        ExpressionConverter.replaceWithExpressionTransformer(timestampAdd.mid, attributeSeq),
        ExpressionConverter.replaceWithExpressionTransformer(timestampAdd.left, attributeSeq),
        timestampAdd
      )
    case timestampDiff: TimestampDiff =>
      new TimestampDiffExpressionTransformer(
        substraitExprName,
        ExpressionConverter.replaceWithExpressionTransformer(timestampDiff.left, attributeSeq),
        ExpressionConverter.replaceWithExpressionTransformer(timestampDiff.mid, attributeSeq),
        ExpressionConverter.replaceWithExpressionTransformer(timestampDiff.right, attributeSeq),
        timestampDiff
      )
    case other =>
      throw new UnsupportedOperationException(
        s"${expr.getClass} or $expr is not currently supported.")
  }
}
