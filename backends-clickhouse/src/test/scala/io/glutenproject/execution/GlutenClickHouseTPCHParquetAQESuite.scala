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
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}

class GlutenClickHouseTPCHParquetAQESuite extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val resourcePath: String =
    "../../../../jvm/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../jvm/src/test/resources/queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  /**
    * Run Gluten + ClickHouse Backend with SortShuffleManager
    */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
      .set("spark.sql.adaptive.enabled", "true")
  }

  override protected def createTPCHTables(): Unit = {
    val customerData = tablesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS customer (
         | c_custkey    bigint,
         | c_name       string,
         | c_address    string,
         | c_nationkey  bigint,
         | c_phone      string,
         | c_acctbal    double,
         | c_mktsegment string,
         | c_comment    string)
         | USING PARQUET LOCATION '${customerData}'
         |""".stripMargin)

    val lineitemData = tablesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS lineitem (
         | l_orderkey      bigint,
         | l_partkey       bigint,
         | l_suppkey       bigint,
         | l_linenumber    bigint,
         | l_quantity      double,
         | l_extendedprice double,
         | l_discount      double,
         | l_tax           double,
         | l_returnflag    string,
         | l_linestatus    string,
         | l_shipdate      date,
         | l_commitdate    date,
         | l_receiptdate   date,
         | l_shipinstruct  string,
         | l_shipmode      string,
         | l_comment       string)
         | USING PARQUET LOCATION '${lineitemData}'
         |""".stripMargin)

    val nationData = tablesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS nation (
         | n_nationkey bigint,
         | n_name      string,
         | n_regionkey bigint,
         | n_comment   string)
         | USING PARQUET LOCATION '${nationData}'
         |""".stripMargin)

    val regionData = tablesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS region (
         | r_regionkey bigint,
         | r_name      string,
         | r_comment   string)
         | USING PARQUET LOCATION '${regionData}'
         |""".stripMargin)

    val ordersData = tablesPath + "/order"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS orders (
         | o_orderkey      bigint,
         | o_custkey       bigint,
         | o_orderstatus   string,
         | o_totalprice    double,
         | o_orderdate     date,
         | o_orderpriority string,
         | o_clerk         string,
         | o_shippriority  bigint,
         | o_comment       string)
         | USING PARQUET LOCATION '${ordersData}'
         |""".stripMargin)

    val partData = tablesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS part (
         | p_partkey     bigint,
         | p_name        string,
         | p_mfgr        string,
         | p_brand       string,
         | p_type        string,
         | p_size        bigint,
         | p_container   string,
         | p_retailprice double,
         | p_comment     string)
         | USING PARQUET LOCATION '${partData}'
         |""".stripMargin)

    val partsuppData = tablesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS partsupp (
         | ps_partkey    bigint,
         | ps_suppkey    bigint,
         | ps_availqty   bigint,
         | ps_supplycost double,
         | ps_comment    string)
         | USING PARQUET LOCATION '${partsuppData}'
         |""".stripMargin)

    val supplierData = tablesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS supplier (
         | s_suppkey   bigint,
         | s_name      string,
         | s_address   string,
         | s_nationkey bigint,
         | s_phone     string,
         | s_acctbal   double,
         | s_comment   string)
         | USING PARQUET LOCATION '${supplierData}'
         |""".stripMargin)

    val result = spark.sql(
      s"""
         | show tables;
         |""".stripMargin).collect()
    assert(result.size == 8)
  }

  test("TPCH Q1") {
    runTPCHQuery(1) { df =>
      assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
      val scanExec = collect(df.queryExecution.executedPlan) {
        case scanExec: BasicScanExecTransformer => scanExec
      }
      assert(scanExec.size == 1)
    }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) { df =>
      assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
      val scanExec = collect(df.queryExecution.executedPlan) {
        case scanExec: BasicScanExecTransformer => scanExec
      }
      assert(scanExec.size == 8)
    }
  }

  test("TPCH Q3") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(3) { df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val shjBuildLeft = collect(df.queryExecution.executedPlan) {
          case shj: ShuffledHashJoinExecTransformer if shj.buildSide == BuildLeft => shj
        }
        assert(shjBuildLeft.size == 2)
      }
    }
  }

  test("TPCH Q4") {
    runTPCHQuery(4) { df =>
    }
  }

  test("TPCH Q5") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(5) { df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val bhjRes = collect(df.queryExecution.executedPlan) {
          case bhj: BroadcastHashJoinExecTransformer => bhj
        }
        assert(bhjRes.isEmpty)
      }
    }
  }

  test("TPCH Q6") {
    runTPCHQuery(6) { df =>
    }
  }

  test("TPCH Q7") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
      runTPCHQuery(7) { df =>
      }
    }
  }

  test("TPCH Q8") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
      runTPCHQuery(8) { df =>
      }
    }
  }

  test("TPCH Q9") {
    runTPCHQuery(9, compareResult = false) { df =>
    }
  }

  test("TPCH Q10") {
    runTPCHQuery(10) { df =>
    }
  }

  test("TPCH Q11") {
    runTPCHQuery(11, compareResult = false) { df =>
    }
  }

  test("TPCH Q12") {
    runTPCHQuery(12) { df =>
    }
  }

  test("TPCH Q13") {
    runTPCHQuery(13) { df =>
    }
  }

  test("TPCH Q14") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.backend.ch.use.v2", "true")) {
      runTPCHQuery(14) { df =>
      }
    }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) { df =>
    }
  }

  test("TPCH Q16") {
    runTPCHQuery(16) { df =>
    }
  }

  test("TPCH Q17") {
    withSQLConf(
      ("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(17) { df =>
      }
    }
  }

  test("TPCH Q18") {
    withSQLConf(
      ("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(18) { df =>
      }
    }
  }

  test("TPCH Q19") {
    runTPCHQuery(19) { df =>
    }
  }

  test("TPCH Q20") {
    runTPCHQuery(20) { df =>
    }
  }

  test("TPCH Q22") {
    runTPCHQuery(22) { df =>
    }
  }
}
