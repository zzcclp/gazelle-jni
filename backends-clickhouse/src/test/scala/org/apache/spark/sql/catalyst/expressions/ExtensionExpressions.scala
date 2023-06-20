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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TypeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.time.ZoneId
import java.util.Locale

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sum calculated from values of a group. " +
    "It differs in that when no non null values are applied zero is returned instead of null")
case class Sum0(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case _ => DoubleType
  }

  private lazy val sumDataType = resultType

  private lazy val sum = AttributeReference("sum", sumDataType)()

  private lazy val zero = Cast(Literal(0), sumDataType)

  override lazy val aggBufferAttributes = sum :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    //    /* sum = */ Literal.create(0, sumDataType)
    //    /* sum = */ Literal.create(null, sumDataType)
    Cast(Literal(0), sumDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      Seq(
        /* sum = */
        Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType)), sum))
      )
    } else {
      Seq(
        /* sum = */
        Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType))
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      Coalesce(Seq(Add(Coalesce(Seq(sum.left, zero)), sum.right), sum.left))
    )
  }

  override lazy val evaluateExpression: Expression = sum

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

case class SplitPart(left: Expression, mid: Expression, right: Expression)
  extends TernaryExpression
  with ExpectsInputTypes {

  override def dataType: DataType = left.dataType

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)

  override def first: Expression = left

  override def second: Expression = mid

  override def third: Expression = right

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    SplitPartImpl.evaluate(input1.toString, input2.toString, input3.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ta = SplitPartImpl.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(
      ctx,
      ev,
      (arg1, arg2, arg3) => {
        s"""
          org.apache.spark.unsafe.types.UTF8String result =
            $ta.evaluate($arg1.toString(), $arg2.toString(), $arg3);
          if (result == null) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = result;
          }
        """
      }
    )
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): Expression = {
    val newChildren = Seq(newFirst, newSecond, newThird)
    super.legacyWithNewChildren(newChildren)
  }
}

case class FloorDateTime(
    timestamp: Expression,
    format: Expression,
    timeZoneId: Option[String] = None)
  extends TruncInstant
  with TimeZoneAwareExpression {

  override def left: Expression = timestamp

  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override def dataType: TimestampType = TimestampType

  override def prettyName: String = "floor_datetime"

  override val instant = timestamp

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(timestamp: Expression, format: Expression) = this(timestamp, format, None)

  override def eval(input: InternalRow): Any = {
    evalHelper(input, minLevel = DateTimeUtils.TRUNC_TO_SECOND) {
      (t: Any, level: Int) => DateTimeUtils.truncTimestamp(t.asInstanceOf[Long], level, zoneId)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", zoneId, classOf[ZoneId].getName)
    codeGenHelper(ctx, ev, minLevel = DateTimeUtils.TRUNC_TO_SECOND, true) {
      (date: String, fmt: String) => s"truncTimestamp($date, $fmt, $tz);"
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
}

case class CeilDateTime(
    timestamp: Expression,
    format: Expression,
    timeZoneId: Option[String] = None)
  extends TruncInstant
  with TimeZoneAwareExpression {

  override def left: Expression = timestamp

  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override def dataType: TimestampType = TimestampType

  override def prettyName: String = "ceil_datetime"

  override val instant = timestamp

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(timestamp: Expression, format: Expression) = this(timestamp, format, None)

  // scalastyle:off
  override def eval(input: InternalRow): Any = {
    evalHelper(input, minLevel = DateTimeUtils.TRUNC_TO_SECOND) {
      (t: Any, level: Int) => ExtendDateTimeUtils.ceilTimestamp(t.asInstanceOf[Long], level, zoneId)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val extendedDtu = ExtendDateTimeUtils.getClass.getName.stripSuffix("$")
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val truncLevel: Int =
      DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
    val javaType = CodeGenerator.javaType(dataType)
    if (format.foldable) {
      if (truncLevel < DateTimeUtils.TRUNC_TO_SECOND) {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};""")
      } else {
        val t = instant.genCode(ctx)
        ev.copy(code = code"""
          ${t.code}
          boolean ${ev.isNull} = ${t.isNull};
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $extendedDtu.ceilTimestamp(${t.value}, ${truncLevel.toString}, $zid);
          }""")
      }
    } else {
      nullSafeCodeGen(
        ctx,
        ev,
        (left, right) => {
          val form = ctx.freshName("form")
          val (dateVal, fmt) = (right, left)
          s"""
          int $form = $dtu.parseTruncLevel($fmt);
          if ($form < ${DateTimeUtils.TRUNC_TO_SECOND}) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $extendedDtu.ceilTimestamp($dateVal, $form, $zid);
          }
        """
        }
      )
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
}

case class TimestampAdd(left: Expression, mid: Expression, right: Expression)
  extends TernaryExpression
  with ExpectsInputTypes {

  override def dataType: DataType = getResultDataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, TypeCollection(IntegerType, LongType), TypeCollection(DateType, TimestampType))

  def getResultDataType(): DataType = {
    if (canConvertTimestamp()) {
      TimestampType
    } else {
      right.dataType
    }
  }

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    (mid.dataType, right.dataType) match {
      case (IntegerType, DateType) =>
        if (canConvertTimestamp()) {
          TimestampAddImpl.evaluateTimestamp(
            input1.toString,
            input2.asInstanceOf[Int],
            input3.asInstanceOf[Int])
        } else {
          TimestampAddImpl.evaluateDays(
            input1.toString,
            input2.asInstanceOf[Int],
            input3.asInstanceOf[Int])
        }
      case (LongType, DateType) =>
        if (canConvertTimestamp()) {
          TimestampAddImpl.evaluateTimestamp(
            input1.toString,
            input2.asInstanceOf[Long],
            input3.asInstanceOf[Int])
        } else {
          TimestampAddImpl.evaluateDays(
            input1.toString,
            input2.asInstanceOf[Long],
            input3.asInstanceOf[Int])
        }
      case (IntegerType, TimestampType) =>
        TimestampAddImpl.evaluateTimestamp(
          input1.toString,
          input2.asInstanceOf[Int],
          input3.asInstanceOf[Long])
      case (LongType, TimestampType) =>
        TimestampAddImpl.evaluateTimestamp(
          input1.toString,
          input2.asInstanceOf[Long],
          input3.asInstanceOf[Long])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ta = TimestampAddImpl.getClass.getName.stripSuffix("$")
    (mid.dataType, right.dataType) match {
      case ((IntegerType, DateType) | (LongType, DateType)) =>
        if (canConvertTimestamp()) {
          defineCodeGen(
            ctx,
            ev,
            (arg1, arg2, arg3) => {
              s"""$ta.evaluateTimestamp($arg1.toString(), $arg2, $arg3)"""
            })
        } else {
          defineCodeGen(
            ctx,
            ev,
            (arg1, arg2, arg3) => {
              s"""$ta.evaluateDays($arg1.toString(), $arg2, $arg3)"""
            })
        }
      case (IntegerType, TimestampType) | (LongType, TimestampType) =>
        defineCodeGen(
          ctx,
          ev,
          (arg1, arg2, arg3) => {
            s"""$ta.evaluateTimestamp($arg1.toString(), $arg2, $arg3)"""
          })
    }
  }

  override def first: Expression = left

  override def second: Expression = mid

  override def third: Expression = right

  def canConvertTimestamp(): Boolean = {
    if (left.isInstanceOf[Literal] && left.asInstanceOf[Literal].value != null) {
      val unit = left.asInstanceOf[Literal].value.toString.toUpperCase(Locale.ROOT)
      if (TimestampAddImpl.TIME_UNIT.contains(unit) && right.dataType.isInstanceOf[DateType]) {
        return true
      }
    }
    false
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): Expression = {
    val newChildren = Seq(newFirst, newSecond, newThird)
    super.legacyWithNewChildren(newChildren)
  }
}

case class TimestampDiff(left: Expression, mid: Expression, right: Expression)
  extends TernaryExpression
  with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      StringType,
      TypeCollection(DateType, TimestampType),
      TypeCollection(DateType, TimestampType))

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    (mid.dataType, right.dataType) match {
      case (DateType, DateType) =>
        TimestampDiffImpl.evaluate(
          input1.toString,
          input2.asInstanceOf[Int],
          input3.asInstanceOf[Int])
      case (DateType, TimestampType) =>
        TimestampDiffImpl.evaluate(
          input1.toString,
          input2.asInstanceOf[Int],
          input3.asInstanceOf[Long])
      case (TimestampType, DateType) =>
        TimestampDiffImpl.evaluate(
          input1.toString,
          input2.asInstanceOf[Long],
          input3.asInstanceOf[Int])
      case (TimestampType, TimestampType) =>
        TimestampDiffImpl.evaluate(
          input1.toString,
          input2.asInstanceOf[Long],
          input3.asInstanceOf[Long])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val td = TimestampDiffImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(
      ctx,
      ev,
      (arg1, arg2, arg3) => {
        s"""$td.evaluate($arg1.toString(), $arg2, $arg3)"""
      })
  }

  override def first: Expression = left

  override def second: Expression = mid

  override def third: Expression = right

  override def dataType: DataType = LongType

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): Expression = {
    val newChildren = Seq(newFirst, newSecond, newThird)
    super.legacyWithNewChildren(newChildren)
  }
}
