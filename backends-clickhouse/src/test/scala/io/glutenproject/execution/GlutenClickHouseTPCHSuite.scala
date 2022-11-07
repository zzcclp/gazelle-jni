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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.BuildLeft

class GlutenClickHouseTPCHSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
  }

  test("TPCH Q1") {
    runTPCHQuery(1) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 1)
    }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 8)
    }
  }

  test("TPCH Q3") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(3) {
        df =>
          val shjBuildLeft = df.queryExecution.executedPlan.collect {
            case shj: ShuffledHashJoinExecTransformer if shj.joinBuildSide == BuildLeft => shj
          }
          assert(shjBuildLeft.size == 2)
      }
    }
  }

  test("TPCH Q4") {
    runTPCHQuery(4) { df => }
  }

  test("TPCH Q5") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(5) {
        df =>
          val bhjRes = df.queryExecution.executedPlan.collect {
            case bhj: BroadcastHashJoinExecTransformer => bhj
          }
          assert(bhjRes.isEmpty)
      }
    }
  }

  test("TPCH Q6") {
    runTPCHQuery(6) { df => }
  }

  test("TPCH Q7") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
      runTPCHQuery(7) { df => }
    }
  }

  test("TPCH Q8") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
      runTPCHQuery(8) { df => }
    }
  }

  test("TPCH Q9") {
    runTPCHQuery(9) { df => }
  }

  test("TPCH Q10") {
    runTPCHQuery(10) { df => }
  }

  test("TPCH Q11") {
    runTPCHQuery(11) { df => }
  }

  test("TPCH Q12") {
    runTPCHQuery(12) { df => }
  }

  test("TPCH Q13") {
    runTPCHQuery(13) { df => }
  }

  test("TPCH Q14") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
      runTPCHQuery(14) { df => }
    }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) { df => }
  }

  test("TPCH Q16") {
    runTPCHQuery(16) { df => }
  }

  test("TPCH Q17") {
    withSQLConf(("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(17) { df => }
    }
  }

  test("TPCH Q18") {
    withSQLConf(("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(18) { df => }
    }
  }

  test("TPCH Q19") {
    runTPCHQuery(19) { df => }
  }

  test("TPCH Q20") {
    runTPCHQuery(20) { df => }
  }

  test("TPCH Q22") {
    runTPCHQuery(22) { df => }
  }

  ignore("test 'select count(*) from table'") {
    // currently, it can't support 'select count(*)' for non-partitioned tables.
    val df = spark.sql("""
                         |select count(*) from lineitem
                         |""".stripMargin)
    val result = df.collect()
  }

  test("test 'select count(*)'") {
    val df = spark.sql("""
                         |select count(*) from lineitem
                         |where l_quantity < 24
                         |""".stripMargin)
    val result = df.collect()
    assert(result(0).getLong(0) == 275436L)
  }

  test("test 'select count(1)'") {
    val df = spark.sql("""
                         |select count(1) from lineitem
                         |where l_quantity < 20
                         |""".stripMargin)
    val result = df.collect()
    assert(result(0).getLong(0) == 227302L)
  }

  test("test 'select count(1)' with empty columns to read") {
    val df = spark.sql("""
                         |select count(1) from lineitem
                         |""".stripMargin)
    val result = df.collect()
    assert(result(0).getLong(0) == 600572L)
  }

  test("test 'select count(*)' with empty columns to read") {
    val df = spark.sql("""
                         |select count(*) from lineitem
                         |""".stripMargin)
    val result = df.collect()
    assert(result(0).getLong(0) == 600572L)
  }

  test("test 'select sum(2)' with empty columns to read") {
    val df = spark.sql("""
                         |select sum(2) from lineitem
                         |""".stripMargin)
    val result = df.collect()
    assert(result(0).getLong(0) == 1201144L)
  }

  test("test 'select 1' with empty columns to read") {
    val df = spark.sql("""
                         |select 1 from lineitem limit 2
                         |""".stripMargin)
    val result = df.collect()
    assert(result.size == 2)
    assert(result(0).getInt(0) == 1 && result(1).getInt(0) == 1)
  }

  ignore("TPCH Q21") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")) {}
  }
}
