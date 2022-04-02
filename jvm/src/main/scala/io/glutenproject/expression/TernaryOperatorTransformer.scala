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

package io.glutenproject.expression

import com.google.common.collect.Lists
import io.glutenproject.substrait.expression.ExpressionNode
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class SubStringTransformer(str: Expression, pos: Expression, len: Expression, original: Expression)
    extends Substring(str: Expression, pos: Expression, len: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

object TernaryOperatorTransformer {

  def create(str: Expression, pos: Expression, len: Expression, original: Expression): Expression =
    original match {
    case ss: Substring =>
      new SubStringTransformer(str, pos, len, ss)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
