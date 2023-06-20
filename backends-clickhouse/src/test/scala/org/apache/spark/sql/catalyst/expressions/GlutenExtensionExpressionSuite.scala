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

import io.glutenproject.execution.GlutenClickHouseTPCHAbstractSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class GlutenExtensionExpressionSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "false")
      .set(
        "spark.gluten.sql.columnar.extended.expressions.transformer",
        "org.apache.spark.sql.catalyst.expressions.CustomerExpressionTransformer")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (floorDateTimeExpr, floorDateTimeBuilder) =
      FunctionRegistryBase.build[FloorDateTime]("floor_datetime", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("floor_datetime"),
      floorDateTimeExpr,
      floorDateTimeBuilder
    )

    val (ceilDateTimeExpr, ceilDateTimeBuilder) =
      FunctionRegistryBase.build[CeilDateTime]("ceil_datetime", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("ceil_datetime"),
      ceilDateTimeExpr,
      ceilDateTimeBuilder
    )

    val (timestampAddExpr, timestampAddBuilder) =
      FunctionRegistryBase.build[TimestampAdd]("timestampadd", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("timestampadd"),
      timestampAddExpr,
      timestampAddBuilder
    )

    val (timestampDiffExpr, timestampDiffBuilder) =
      FunctionRegistryBase.build[TimestampDiff]("timestampdiff", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("timestampdiff"),
      timestampDiffExpr,
      timestampDiffBuilder
    )
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createTPCHParquetTables(tablesPath)
  }

  test("test some extension expresion") {
    // Array("floor_", "ceil_").map(f => {
    Array
      .empty[String]
      .map(
        f => {
          val testSqlSeq = Seq(
            s"select ${f}datetime(date'2012-02-29', 'year')",
            s"select ${f}datetime(date'2012-02-29', 'quarter')",
            s"select ${f}datetime(date'2012-02-29', 'month')",
            s"select ${f}datetime(date'2012-02-29', 'week')",
            s"select ${f}datetime(date'2012-02-29', 'DAY')",
            s"select ${f}datetime(date'2012-02-29', 'hour')",
            s"select ${f}datetime(date'2012-02-29', 'minute')",
            s"select ${f}datetime(date'2012-02-29', 'second')",
            s"select ${f}datetime('2012-02-29', 'year')",
            s"select ${f}datetime('2012-02-29', 'quarter')",
            s"select ${f}datetime('2012-02-29', 'month')",
            s"select ${f}datetime('2012-02-29', 'week')",
            s"select ${f}datetime('2012-02-29', 'DAY')",
            s"select ${f}datetime('2012-02-29', 'hour')",
            s"select ${f}datetime('2012-02-29', 'minute')",
            s"select ${f}datetime('2012-02-29', 'second')",
            s"select ${f}datetime(timestamp'2012-02-29 23:59:59.1', 'year')",
            s"select ${f}datetime(timestamp'2012-02-29 23:59:59.1', 'quarter')",
            s"select ${f}datetime(timestamp'2012-02-29 23:59:59.1', 'month')",
            s"select ${f}datetime(timestamp'2012-02-29 23:59:59.1', 'week')",
            s"select ${f}datetime(timestamp'2012-02-29 23:59:59.1', 'DAY')",
            s"select ${f}datetime(timestamp'2012-02-29 23:59:59.1', 'hour')",
            s"select ${f}datetime(timestamp'2012-02-29 23:59:59.1', 'minute')",
            s"select ${f}datetime(timestamp'2012-02-29 23:59:59.1', 'second')",
            s"select ${f}datetime('2012-02-29 23:59:59.1', 'year')",
            s"select ${f}datetime('2012-02-29 23:59:59.1', 'quarter')",
            s"select ${f}datetime('2012-02-29 23:59:59.1', 'month')",
            s"select ${f}datetime('2012-02-29 23:59:59.1', 'week')",
            s"select ${f}datetime('2012-02-29 23:59:59.1', 'DAY')",
            s"select ${f}datetime('2012-02-29 23:59:59.1', 'hour')",
            s"select ${f}datetime('2012-02-29 23:59:59.1', 'minute')",
            s"select ${f}datetime('2012-02-29 23:59:59.1', 'second')"
          )
          testSqlSeq.foreach(
            sql => {
              compareResultsAgainstVanillaSpark(sql, true, df => df.show(1000), false)
            })
        })

    val sql =
      s"""
         |select l_shipdate,
         |       floor_datetime(l_shipdate, 'YEAR') a,
         |       floor_datetime(l_shipdate, 'quarter') b,
         |       floor_datetime(l_shipdate, 'month') c,
         |       floor_datetime(l_shipdate, 'week') d,
         |       floor_datetime(l_shipdate, 'DAY') e,
         |       floor_datetime(l_shipdate, 'hour') f,
         |       floor_datetime(l_shipdate, 'minute') g,
         |       floor_datetime(l_shipdate, 'second') h
         |from lineitem
         |order by l_shipdate
         |limit 10
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          df.show(1000, false)
          df.explain(false)
      })

    val sql1 =
      s"""
         |select l_shipdate,
         |       date_trunc('year', l_shipdate) a,
         |       date_trunc('quarter', l_shipdate) b,
         |       date_trunc('month', l_shipdate) c,
         |       date_trunc('WEEK', l_shipdate) d,
         |       date_trunc('day', l_shipdate) e,
         |       date_trunc('hour', l_shipdate) f,
         |       date_trunc('minute', l_shipdate) g,
         |       date_trunc('second', l_shipdate) h
         |from lineitem
         |order by l_shipdate
         |limit 10
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql1,
      true,
      {
        df =>
          df.show(1000, false)
          df.explain(false)
      })

    /* val sql2 =
      s"""
         |select l_shipdate,
         |       trunc(l_shipdate, 'year') a,
         |       trunc(l_shipdate, 'quarter') b,
         |       trunc(l_shipdate, 'month') c,
         |       trunc(l_shipdate, 'week') d,
         |       trunc(l_shipdate, 'day') e,
         |       trunc(l_shipdate, 'hour') f,
         |       trunc(l_shipdate, 'minute') g,
         |       trunc(l_shipdate, 'second') h
         |from lineitem
         |order by l_shipdate
         |limit 10
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql2,
      true,
      {
        df =>
          df.show(1000, false)
          df.explain(false)
      }) */

    /*
      --------------- type : Date32
    Re-throwing a java exception for native exception...
    13:18:56.453 ERROR org.apache.spark.util.memory.TaskResources: Task 5 failed by error:
    java.lang.RuntimeException: Function addDays supports 2 or 3 arguments. The 1st argument must be of type Date or DateTime. The 2nd argument must be a number. The 3rd argument (optional) must be a constant string with timezone name. The timezone argument is allowed only when the 1st argument has the type DateTime
    0. Poco::Exception::Exception(String const&, int) @ 0x00000000132a5259 in /usr/local/clickhouse/lib/libch.so
    1. DB::Exception::Exception(DB::Exception::MessageMasked&&, int, bool) @ 0x000000000ba652f5 in /usr/local/clickhouse/lib/libch.so
    2. DB::Exception::Exception<String>(int, FormatStringHelperImpl<std::type_identity<String>::type>, String&&) @ 0x00000000069cb623 in /usr/local/clickhouse/lib/libch.so
    3. DB::FunctionDateOrDateTimeAddInterval<DB::AddDaysImpl>::getReturnTypeImpl(std::vector<DB::ColumnWithTypeAndName
     */
    /*
    !== Correct Answer - 10 ==                                      == Spark Answer - 10 ==
     struct<l_shipdate:date,l_receiptdate:date,e:bigint,h:bigint>   struct<l_shipdate:date,l_receiptdate:date,e:bigint,h:bigint>
     [1992-01-03,1992-01-04,1,0]                                    [1992-01-03,1992-01-04,1,0]
     [1992-01-03,1992-01-05,2,0]                                    [1992-01-03,1992-01-05,2,0]
     [1992-01-03,1992-01-05,2,0]                                    [1992-01-03,1992-01-05,2,0]
    ![1992-01-03,1992-01-30,27,3]                                   [1992-01-03,1992-01-30,27,4]
     [1992-01-03,1992-01-31,28,4]                                   [1992-01-03,1992-01-31,28,4]
     [1992-01-04,1992-01-05,1,0]                                    [1992-01-04,1992-01-05,1,0]
     [1992-01-04,1992-01-05,1,0]                                    [1992-01-04,1992-01-05,1,0]
    ![1992-01-04,1992-01-14,10,1]                                   [1992-01-04,1992-01-14,10,2]
     [1992-01-04,1992-01-18,14,2]                                   [1992-01-04,1992-01-18,14,2]
    ![1992-01-04,1992-01-22,18,2]                                   [1992-01-04,1992-01-22,18,3]

     */
    val sql3 =
      s"""
         |select l_shipdate,l_receiptdate,
         |--        timestampdiff('year', l_shipdate, l_shipdate) a,
         |--        timestampadd('quarter', 2, l_shipdate) b,
         |--        timestampadd('month', 3, l_shipdate) c,
         |--        timestampadd('week', 1, l_shipdate) d,
         |       timestampdiff('day', l_shipdate, l_receiptdate) e,
         |--        timestampdiff('week', l_shipdate, l_receiptdate) f,
         |--        timestampdiff('minute', l_shipdate, l_receiptdate) g,
         |       timestampdiff('week', l_shipdate, l_receiptdate) h
         |from lineitem
         |order by l_shipdate,l_receiptdate
         |limit 10
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql3,
      true,
      {
        df =>
          df.show(1000, false)
          df.explain(false)
      })
  }
}
