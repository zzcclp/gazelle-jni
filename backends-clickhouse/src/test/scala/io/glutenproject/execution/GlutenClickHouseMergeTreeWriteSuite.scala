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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseMergeTreeWriteSuite
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
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.use_local_format", "true")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_config.user_defined_path",
        "/tmp/user_defined")
      .set("spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm", "sparkMurmurHash3_32")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createTPCHParquetTables(tablesPath)
  }

  test("test mergetree write") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS lineitem_mergetree
                 |(
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
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string,
                 | l_shipdate      date
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree
                 |   select
                 |   l_orderkey      ,
                 | l_partkey       ,
                 | l_suppkey       ,
                 | l_linenumber    ,
                 | l_quantity      ,
                 | l_extendedprice ,
                 | l_discount      ,
                 | l_tax           ,
                 | l_returnflag    ,
                 | l_linestatus    ,
                 | l_commitdate    ,
                 | l_receiptdate   ,
                 | l_shipinstruct  ,
                 | l_shipmode      ,
                 | l_comment,
                 | l_shipdate
                 |    from lineitem
                 |   where l_shipdate BETWEEN date'1992-01-01' AND date'1992-01-10'
                 |""".stripMargin)

    spark
      .sql(s"""
              |show tables;
              |""".stripMargin)
      .show(1000)

    val s = spark.sql(s"""
                         |desc formatted lineitem_mergetree;
                         |""".stripMargin)
    s.show(1000)

    val results = spark
      .sql(s"""
              |select l_partkey,l_orderkey from lineitem_mergetree;
              |""".stripMargin)
    val datas = results.collect()
    datas.length
  }

  test("test mergetree write with orderby keys") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_orderbykey;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS lineitem_mergetree_orderbykey
                 |(
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
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string,
                 | l_shipdate      date
                 |)
                 |USING clickhouse
                 |TBLPROPERTIES (orderByKey='l_shipdate,l_orderkey',
                 |               primaryKey='l_shipdate')
                 |LOCATION '$basePath/lineitem_mergetree_orderbykey'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_orderbykey
                 |   select
                 |   l_orderkey      ,
                 | l_partkey       ,
                 | l_suppkey       ,
                 | l_linenumber    ,
                 | l_quantity      ,
                 | l_extendedprice ,
                 | l_discount      ,
                 | l_tax           ,
                 | l_returnflag    ,
                 | l_linestatus    ,
                 | l_commitdate    ,
                 | l_receiptdate   ,
                 | l_shipinstruct  ,
                 | l_shipmode      ,
                 | l_comment,
                 | l_shipdate
                 |    from lineitem
                 |   where l_shipdate BETWEEN date'1992-01-01' AND date'1992-01-10'
                 |""".stripMargin)

    spark
      .sql(s"""
              |show tables;
              |""".stripMargin)
      .show(1000)

    val s = spark.sql(s"""
                         |desc formatted lineitem_mergetree_orderbykey;
                         |""".stripMargin)
    s.show(1000)

    val results = spark
      .sql(s"""
              |select l_orderkey,l_partkey from lineitem_mergetree_orderbykey;
              |""".stripMargin)
    val datas = results.collect()
    datas.length
  }

  test("test mergetree write with partition") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_partition;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS lineitem_mergetree_partition
                 |(
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
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string,
                 | l_shipdate      date
                 |)
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate, l_returnflag)
                 |TBLPROPERTIES (orderByKey='l_orderkey',
                 |               primaryKey='l_orderkey')
                 |LOCATION '$basePath/lineitem_mergetree_partition'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_partition
                 |   select
                 |   l_orderkey      ,
                 | l_partkey       ,
                 | l_suppkey       ,
                 | l_linenumber    ,
                 | l_quantity      ,
                 | l_extendedprice ,
                 | l_discount      ,
                 | l_tax           ,
                 | l_returnflag    ,
                 | l_linestatus    ,
                 | l_commitdate    ,
                 | l_receiptdate   ,
                 | l_shipinstruct  ,
                 | l_shipmode      ,
                 | l_comment,
                 | l_shipdate
                 |    from lineitem
                 |   where l_shipdate BETWEEN date'1992-01-01' AND date'1993-01-10'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into lineitem_mergetree_partition PARTITION (l_shipdate=date'1999-01-21',
                 | l_returnflag = 'A')
                 |   (l_orderkey,
                 |l_partkey,
                 |l_suppkey,
                 |l_linenumber,
                 |l_quantity,
                 |l_extendedprice,
                 |l_discount,
                 |l_tax,
                 |l_linestatus,
                 |l_commitdate,
                 |l_receiptdate,
                 |l_shipinstruct,
                 |l_shipmode,
                 |l_comment)
                 |   select l_orderkey,
                 |l_partkey,
                 |l_suppkey,
                 |l_linenumber,
                 |l_quantity,
                 |l_extendedprice,
                 |l_discount,
                 |l_tax,
                 |l_linestatus,
                 |l_commitdate,
                 |l_receiptdate,
                 |l_shipinstruct,
                 |l_shipmode,
                 |l_comment from lineitem
                 |   where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
                 |""".stripMargin)

    spark
      .sql(s"""
              |show tables;
              |""".stripMargin)
      .show(1000)

    val s = spark.sql(s"""
                         |desc formatted lineitem_mergetree_partition;
                         |""".stripMargin)
    s.show(1000)

    val results = spark
      .sql(s"""
              |select l_shipdate, l_orderkey,l_partkey from lineitem_mergetree_partition;
              |""".stripMargin)
    val datas = results.collect()
    results.show(10000)
    datas.length
  }
}
// scalastyle:off line.size.limit
