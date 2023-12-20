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
package io.glutenproject.benchmarks

import io.glutenproject.GlutenConfig
import io.glutenproject.execution.{BroadcastHashJoinExecTransformer, ShuffledHashJoinExecTransformerBase}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseLog
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

// scalastyle:off
object DSV2BenchmarkTest extends AdaptiveSparkPlanHelper {

  val tableName = "lineitem_ch"

  def main(args: Array[String]): Unit = {

    // val libPath = "/home/myubuntu/Works/c_cpp_projects/Kyligence-ClickHouse/" +
    //   "cmake-build-ch-debug/utils/extern-local-engine/libchd.so"
    val libPath = "/home/myubuntu/Works/c_cpp_projects/Kyligence-ClickHouse/" +
      "cmake-build-ch-release/utils/extern-local-engine/libch.so"
    // val libPath = "/usr/local/clickhouse/lib/libch.so"
    val thrdCnt = 8
    val shufflePartitions = 24
    val defaultParallelism = 8
    // val shuffleManager = "sort"
    val shuffleManager = "org.apache.spark.shuffle.sort.ColumnarShuffleManager"
    val ioCompressionCodec = "LZ4"
    val customShuffleCodec = "LZ4"
    val columnarColumnToRow = "true"
    val useV2 = "false"
    val separateScanRDD = "false"
    val coalesceBatches = "false"
    val broadcastThreshold = "10MB"
    val adaptiveEnabled = "true"
    val sparkLocalDir = "/data1/gazelle-jni-warehouse/spark_local_dirs"
    val (
      parquetFilesPath,
      fileFormat,
      executedCnt,
      configed,
      sqlFilePath,
      stopFlagFile,
      createTable,
      metaRootPath) = if (args.length > 0) {
      (args(0), args(1), args(2).toInt, true, args(3), args(4), args(5).toBoolean, args(6))
    } else {
      val rootPath = this.getClass.getResource("/").getPath
      val resourcePath = rootPath + "../../../../gluten-core/src/test/resources/"
      val dataPath = resourcePath + "/tpch-data/"
      val queryPath = resourcePath + "/tpch-queries/"
      // (new File(dataPath).getAbsolutePath, "parquet", 1, false, queryPath + "q06.sql", "", true,
      // "/data1/gazelle-jni-warehouse")
      (
        "/data1/test_output/tpch-data-sf10",
        "parquet",
        1,
        false,
        queryPath + "q01.sql",
        "",
        true,
        "/data1/gazelle-jni-warehouse")
    }

    val (warehouse, metaStorePathAbsolute, hiveMetaStoreDB) = if (metaRootPath.nonEmpty) {
      (
        metaRootPath + "/spark-warehouse",
        metaRootPath + "/meta",
        metaRootPath + "/meta/metastore_db")
    } else {
      ("/tmp/spark-warehouse", "/tmp/meta", "/tmp/meta/metastore_db")
    }

    if (warehouse.nonEmpty) {
      val warehouseDir = new File(warehouse)
      if (!warehouseDir.exists()) {
        warehouseDir.mkdirs()
      }
      val hiveMetaStoreDBDir = new File(metaStorePathAbsolute)
      if (!hiveMetaStoreDBDir.exists()) {
        hiveMetaStoreDBDir.mkdirs()
      }
    }

    val sqlStr = Source.fromFile(new File(sqlFilePath), "UTF-8")

    val sessionBuilderTmp = SparkSession
      .builder()
      .appName("Gluten-TPCH-Benchmark")

    val sessionBuilder = if (!configed) {
      val sessionBuilderTmp1 = sessionBuilderTmp
        .master(s"local[$thrdCnt]")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.driver.memory", "20G")
        .config("spark.driver.memoryOverhead", "10G")
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .config("spark.default.parallelism", defaultParallelism)
        .config("spark.sql.shuffle.partitions", shufflePartitions)
        .config("spark.sql.adaptive.enabled", adaptiveEnabled)
        .config("spark.sql.adaptive.logLevel", "DEBUG")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        // .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "3")
        // .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "")
        .config("spark.sql.adaptive.fetchShuffleBlocksInBatch", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
        .config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.2")
        // .config("spark.sql.adaptive.optimizer.excludedRules",
        //   "org.apache.spark.sql.catalyst.optimizer.OptimizeOneRowPlan")
        .config("spark.scheduler.listenerbus.eventqueue.capacity", "20000")
        // .config("spark.sql.files.maxPartitionBytes", 128 << 10 << 10) // default is 128M
        // .config("spark.sql.files.openCostInBytes", 128 << 10 << 10) // default is 4M
        .config("spark.sql.files.minPartitionNum", "12")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.locality.wait", "0s")
        .config("spark.sql.sources.ignoreDataLocality", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        // .config("spark.sql.sources.useV1SourceList",
        //   "avro,csv,json,kafka,orc,parquet,text,clickhouse")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        // .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "128")
        .config("spark.gluten.enabled", "true")
        .config("spark.plugins", "io.glutenproject.GlutenPlugin")
        .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
        .config("spark.shuffle.manager", shuffleManager)
        .config(
          "spark.gluten.sql.columnar.backend.ch.runtime_settings" +
            ".query_plan_enable_optimizations",
          "false")
        .config("spark.shuffle.compress", "true")
        .config("spark.io.compression.codec", ioCompressionCodec)
        .config("spark.gluten.sql.columnar.shuffle.codec", customShuffleCodec)
        .config("spark.gluten.sql.columnar.backend.ch.customized.shuffle.codec.enable", "true")
        .config("spark.gluten.sql.columnar.backend.ch.customized.buffer.size", "4096") // 1048576
        .config("spark.gluten.sql.columnar.shuffleSplitDefaultSize", "8192")
        .config("spark.shuffle.sort.bypassMergeThreshold", "200")
        .config("spark.io.compression.zstd.bufferSize", "32k")
        .config("spark.io.compression.zstd.level", "1")
        .config("spark.io.compression.zstd.bufferPool.enabled", "true")
        .config("spark.io.compression.snappy.blockSize", "32k")
        .config("spark.io.compression.lz4.blockSize", "32k")
        .config("spark.reducer.maxSizeInFlight", "48m")
        .config("spark.shuffle.file.buffer", "32k")
        .config("spark.databricks.delta.maxSnapshotLineageLength", 20)
        .config("spark.databricks.delta.snapshotPartitions", 1)
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", 5)
        .config("spark.databricks.delta.stalenessLimit", 3600 * 1000)
        // .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
        .config("spark.gluten.sql.columnar.columnartorow", columnarColumnToRow)
        .config("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .config("spark.gluten.sql.columnar.backend.ch.use.v2", useV2)
        .config(GlutenConfig.GLUTEN_LIB_PATH, libPath)
        .config("spark.gluten.sql.columnar.iterator", "true")
        .config("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
        .config("spark.gluten.sql.enable.native.validation", "false")
        .config("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", separateScanRDD)
        // .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", broadcastThreshold)
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.exchange.reuse", "true")
        .config("spark.sql.execution.reuseSubquery", "true")
        .config("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
        .config("spark.gluten.sql.columnar.coalesce.batches", coalesceBatches)
        // .config("spark.gluten.sql.columnar.filescan", "true")
        // .config("spark.sql.optimizeNullAwareAntiJoin", "false")
        // .config("spark.sql.join.preferSortMergeJoin", "false")
        .config("spark.sql.shuffledHashJoinFactor", "3")
        // .config("spark.sql.planChangeLog.level", "warn")
        // .config("spark.sql.planChangeLog.batches", "Optimize One Row Plan")
        // .config("spark.sql.optimizer.inSetConversionThreshold", "5")  // IN to INSET
        .config("spark.sql.columnVector.offheap.enabled", "true")
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "50G")
        .config("spark.local.dir", sparkLocalDir)
        .config("spark.executor.heartbeatInterval", "30s")
        .config("spark.network.timeout", "300s")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning.useStats", "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")
        .config("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "true")
        // .config("spark.sql.parquet.footer.use.old.api", "false")
        // .config("spark.sql.fileMetaCache.parquet.enabled", "true")
        // .config("spark.sql.columnVector.custom.clazz",
        //   "org.apache.spark.sql.execution.vectorized.PublicOffHeapColumnVector")
        // .config("spark.hadoop.io.file.buffer.size", "524288")
        .config("spark.sql.codegen.comments", "true")
        .config("spark.ui.retainedJobs", "2500")
        .config("spark.ui.retainedStages", "5000")
        .config(GlutenConfig.GLUTEN_SOFT_AFFINITY_ENABLED, "false")
        /* .config(
          "spark.extraListeners",
          "io.glutenproject.softaffinity.scheduler.SoftAffinityListener") */
        // debug information warning error
        .config("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "error")
        .config("spark.gluten.sql.columnar.sort", "true")
        .config(
          "spark.gluten.sql.columnar.backend.ch.runtime_settings" +
            "max_bytes_before_external_group_by",
          "3000000000")
        .config(
          "spark.gluten.sql.columnar.backend.ch.runtime_settings" +
            "max_bytes_before_external_sort",
          "3000000000")
        .config("spark.gluten.sql.columnar.shuffleSplitDefaultSize", "65505")
        .config("spark.gluten.sql.columnar.backend.ch.files.per.partition.threshold", "100")
        .config("spark.gluten.sql.transform.logLevel", "DEBUG")
        .config("spark.gluten.sql.substrait.plan.logLevel", "DEBUG")
        .config("spark.gluten.sql.cacheWholeStageTransformerContext", "false")
        .config("spark.gluten.sql.validation.logLevel", "WARN")
        .config("spark.gluten.sql.validation.printStackOnFailure", "true")
        .config("spark.gluten.sql.columnar.force.notnull.enabled", "false")
        .config("spark.gluten.sql.columnar.backend.ch.remove.isnotnull", "false")
        .config("spark.gluten.sql.native.bloomFilter", "true")
        .config("spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled", "false")
        .config("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
        .config("spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold", "100MB")
        .config("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB")
        .config("spark.sql.optimizer.runtime.bloomFilter.expectedNumItems", "1000000")
        .config("spark.sql.optimizer.runtime.bloomFilter.maxNumItems", "4000000")
        .config("spark.sql.optimizer.runtime.bloomFilter.numBits", "8388608")
        .config("spark.sql.optimizer.runtime.bloomFilter.maxNumBits", "67108864")
        .config("spark.sql.optimizer.runtimeFilter.number.threshold", "10")
        // .config("spark.gluten.sql.columnar.backend.ch.runtime_conf.enable_nullable", "true")
        .config("spark.gluten.sql.columnar.shuffle.preferSpill", "true")
        .config("spark.hadoop.fs.s3a.bucket.test_value111", "111")
        .config("fs.s3a.bucket.test_value222", "222")
        .config("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true")
        .config("spark.sql.sources.bucketing.enabled", "true")
        // cityHash64  sparkMurmurHash3_32
        .config(
          "spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm",
          "sparkMurmurHash3_32")
        .config("spark.gluten.sql.columnar.maxBatchSize", "4096")
        .config("spark.gluten.sql.native.writer.enabled", "true")
        .config("spark.sql.legacy.charVarcharAsString", "true")
        .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      // .config("spark.sql.execution.useObjectHashAggregateExec", "false")
      // .config("spark.gluten.sql.columnar.force.hashagg", "true")

      if (warehouse.nonEmpty) {
        sessionBuilderTmp1
          .config("spark.sql.warehouse.dir", warehouse)
          .config(
            "javax.jdo.option.ConnectionURL",
            s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
          .enableHiveSupport()
      } else {
        sessionBuilderTmp1.enableHiveSupport()
      }
    } else {
      sessionBuilderTmp
    }

    val spark = sessionBuilder.getOrCreate()
    if (!configed) {
      spark.sparkContext.setLogLevel("WARN")
    }

    val createTbl = false
    if (createTbl) {
      // createClickHouseTable(spark, parquetFilesPath, fileFormat)
      // createLocationClickHouseTable(spark)
      // createTables(spark, parquetFilesPath, fileFormat)
      /* createClickHouseTables(
        spark,
        "/data1/gazelle-jni-warehouse/tpch100_ch_data",
        "default",
        false,
        "ch_",
        "100") */
      createTablesNew(spark)
      // testMergeTreeBucketTables(spark)
    }
    val refreshTable = false
    if (refreshTable) {
      refreshClickHouseTable(spark)
    }
    // scalastyle:off println
    Thread.sleep(1000)
    println("start to query ... ")

    // createClickHouseTablesAsSelect(spark)
    // createClickHouseTablesAndInsert(spark)

    // selectClickHouseTable(spark, executedCnt, sqlStr.mkString, "ch_clickhouse")
    // selectLocationClickHouseTable(spark, executedCnt, sqlStr.mkString)
    // testSerializeFromObjectExec(spark)

    // selectQ1ClickHouseTable(spark, executedCnt, sqlStr.mkString, "ch_clickhouse")
    // selectStarClickHouseTable(spark)
    // selectQ1LocationClickHouseTable(spark, executedCnt, sqlStr.mkString)

    // testSparkTPCH(spark)
    // testSQL(spark, parquetFilesPath, fileFormat, executedCnt, sqlStr.mkString)

    // createTempView(spark, "/data1/test_output/tpch-data-sf10", "parquet")
    // createGlobalTempView(spark)
    // testJoinIssue(spark)
    // testTPCHOne(spark, executedCnt)
    testNongHangCase(spark, executedCnt)
    // testParquetFile(spark, executedCnt)
    // testParquetFile1(spark, executedCnt)
    // testTPCHAll(spark)
    // testSepScanRDD(spark, executedCnt)
    // benchmarkTPCH(spark, executedCnt)

    System.out.println("waiting for finishing")
    ClickHouseLog.clearCache()
    // JniLibLoader.unloadNativeLibs(libPath)
    if (stopFlagFile.isEmpty) {
      Thread.sleep(1800000)
    } else {
      while (new File(stopFlagFile).exists()) {
        Thread.sleep(1000)
      }
    }
    spark.stop()
    System.out.println("finished")
  }

  def collectAllJoinSide(executedPlan: SparkPlan): Unit = {
    val buildSides = collect(executedPlan) {
      case s: ShuffledHashJoinExecTransformerBase => "Shuffle-" + s.joinBuildSide.toString
      case b: BroadcastHashJoinExecTransformer => "Broadcast-" + b.joinBuildSide.toString
      case os: ShuffledHashJoinExec => "Shuffle-" + os.buildSide.toString
      case ob: BroadcastHashJoinExec => "Broadcast-" + ob.buildSide.toString
      case sm: SortMergeJoinExec => "SortMerge-Join"
    }
    println(buildSides.mkString(" -- "))
  }

  def testTPCHOne(spark: SparkSession, executedCnt: Int): Unit = {
    // | default |
    // | delta_ds |
    // | tpcds_gendb |
    // | tpcdsdb |
    // | tpcdsdb01 |
    // | tpcdsdb1 |
    // | tpcdsdb_decimal |
    // | tpch01 |
    // | tpch01_ch |
    // | tpch10 |
    // | tpch100 |
    // | tpch100_ch |
    // | tpch100bucketdb |
    // | tpch10_ch |
    // | tpch10_ch_bucket |
    // | tpch10bucketdb |
    // | tpch_nullable |
    spark
      .sql(s"""
              |use tpch10_ch;
              |""".stripMargin)
    // .show(1000, truncate = false)
    try {
      val tookTimeArr = ArrayBuffer[Long]()
      for (i <- 1 to executedCnt) {
        val startTime = System.nanoTime()
        spark.conf.set("spark.hadoop.fs.s3a.bucket.test_value1", "1")
        spark.conf.set("fs.s3a.bucket.test_value2", "1")
        val hadoopConfig = spark.sessionState.newHadoopConfWithOptions(Map.empty)
        println("------=" + hadoopConfig.get("spark.hadoop.fs.s3a.bucket.test_value1", ""))
        println("------=" + hadoopConfig.get("fs.s3a.bucket.test_value1", ""))
        println("------=" + hadoopConfig.get("fs.s3a.bucket.test_value2", ""))
        println("------=" + hadoopConfig.get("spark.hadoop.fs.s3a.bucket.test_value111", ""))
        println("------=" + hadoopConfig.get("fs.s3a.bucket.test_value111", ""))
        println("------=" + hadoopConfig.get("fs.s3a.bucket.test_value222", ""))
        /* val df1 = spark.sql(
          s"""
             |SELECT f.ps_partkey, f.ps_suppkey, f.ps_supplycost, f.ps_availqty FROM partsupp f
             |JOIN supplier s
             |ON named_struct('suppkey', f.ps_suppkey) = named_struct('suppkey', s.s_suppkey)
             |WHERE s.s_name = 'Supplier#000000001'
             |""".stripMargin
        ) // .show(30, false) */
        val df = spark.sql(
          s"""
             |SELECT
             |    l_returnflag,
             |    l_linestatus,
             |    sum(l_quantity) AS sum_qty,
             |    sum(l_extendedprice) AS sum_base_price,
             |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
             |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
             |    avg(l_quantity) AS avg_qty,
             |    avg(l_extendedprice) AS avg_price,
             |    avg(l_discount) AS avg_disc,
             |    count(*) AS count_order
             |FROM
             |    lineitem
             |WHERE
             |    l_shipdate <= date'1998-09-02' - interval 1 day
             |GROUP BY
             |    l_returnflag,
             |    l_linestatus
             |ORDER BY
             |    l_returnflag,
             |    l_linestatus;
             |
             |""".stripMargin
        )
        // df.queryExecution.debug.codegen
        df.explain(false)
        val result = df.collect() // .show(100, false)  //.collect()
        df.explain(false)
        val plan = df.queryExecution.executedPlan
        val logicalPlan = df.queryExecution.logical
        collectAllJoinSide(plan)
        println(result.length)
        result.foreach(r => println(r.mkString(",")))
        val tookTime = (System.nanoTime() - startTime) / 1000000
        println(s"Execute $i time, time: $tookTime")
        tookTimeArr += tookTime
        // Thread.sleep(5000)
      }

      println(tookTimeArr.mkString(","))

      if (executedCnt >= 30) {
        import spark.implicits._
        val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
        df.summary().show(100, truncate = false)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def testNongHangCase(spark: SparkSession, executedCnt: Int): Unit = {
    /* spark.sql(
      s"""
         |CREATE DATABASE IF NOT EXISTS nonghang_case;
         |""".stripMargin) */
    spark.sql(s"""
                 |use nonghang_case;
                 |""".stripMargin)
    /* spark
      .sql(
        s"""
           |show tables;
           |""".stripMargin)
      .show(1000, truncate = false) */

    val createTables = false
    if (createTables) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS bi_ifar_org;
                   |""".stripMargin)
      spark.sql(s"""
                   |CREATE EXTERNAL TABLE IF NOT EXISTS bi_ifar_org
                   |(
                   |orgprp STRING  NOT NULL  ,
                   |org_snam_2 STRING  NOT NULL  ,
                   |org_sort_3 INT  NOT NULL  ,
                   |org_no_0 STRING  NOT NULL  ,
                   |org_snam_0 STRING  NOT NULL  ,
                   |org_sort_1 INT  NOT NULL  ,
                   |org_snam STRING  NOT NULL  ,
                   |cnlflg STRING  NOT NULL  ,
                   |org_snam_6 STRING  NOT NULL  ,
                   |org_snam_4 STRING  NOT NULL  ,
                   |org_nam_6 STRING  NOT NULL  ,
                   |org_nam_4 STRING  NOT NULL  ,
                   |org_nam_2 STRING  NOT NULL  ,
                   |org_nam_0 STRING  NOT NULL  ,
                   |org_no_7 STRING  NOT NULL  ,
                   |org_sort STRING  NOT NULL  ,
                   |org_no_3 STRING  NOT NULL  ,
                   |org_sort_6 INT  NOT NULL  ,
                   |org_no_5 STRING  NOT NULL  ,
                   |org_snam_3 STRING  NOT NULL  ,
                   |org_sort_4 INT  NOT NULL  ,
                   |org_snam_1 STRING  NOT NULL  ,
                   |org_sort_2 INT  NOT NULL  ,
                   |org_no_1 STRING  NOT NULL  ,
                   |org_sort_0 INT  NOT NULL  ,
                   |org_lvl INT  NOT NULL  ,
                   |org STRING  NOT NULL  ,
                   |org_snam_7 STRING  NOT NULL  ,
                   |org_nam STRING  NOT NULL  ,
                   |cntflg STRING  NOT NULL  ,
                   |up_org_no STRING  NOT NULL  ,
                   |org_snam_5 STRING  NOT NULL  ,
                   |org_nam_5 STRING  NOT NULL  ,
                   |org_nam_3 STRING  NOT NULL  ,
                   |org_nam_1 STRING  NOT NULL  ,
                   |org_no_6 STRING  NOT NULL  ,
                   |org_no_2 STRING  NOT NULL  ,
                   |org_sort_7 INT  NOT NULL  ,
                   |org_no_4 STRING  NOT NULL  ,
                   |enddate STRING  NOT NULL  ,
                   |org_sort_5 INT  NOT NULL  ,
                   |org_nam_7 STRING  NOT NULL  ,
                   |startdate STRING  NOT NULL
                   |)
                   |USING clickhouse
                   |-- PARTITIONED BY (dte)
                   |TBLPROPERTIES (orderByKey='',
                   |               primaryKey='')
                   |LOCATION '/data1/nonghang-data/mergetree/bi_ifar_org'
                   |""".stripMargin)
    }

    try {
      val tookTimeArr = ArrayBuffer[Long]()
      for (i <- 1 to executedCnt) {
        val startTime = System.nanoTime()
        val df = spark.sql(
          s"""
             |SELECT BI_IFAR_ORG.ORG_NAM_3,
             |       SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.BUS_TYP        AS BUS_TYP,
             |       SUM(SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.APSHTRAMT) AS Sum_,
             |       SUM(SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.APSHTRCNT) AS Sum_bi
             |FROM SV_10DMP_S_TAG_DTAL_IDV_HQCKTR
             |         INNER JOIN BI_IFAR_ORG
             |                    ON sv_10dmp_s_tag_dtal_idv_hqcktr.org = bi_ifar_org.org
             |                        AND sv_10dmp_s_tag_dtal_idv_hqcktr.DTE >= bi_ifar_org.STARTDATE
             |                        AND sv_10dmp_s_tag_dtal_idv_hqcktr.DTE <= bi_ifar_org.ENDDATE
             |WHERE 1 = 1
             |  AND (BI_IFAR_ORG.ORG_NO_0 = '99H999'
             |    OR BI_IFAR_ORG.ORG_NO_1 = '99H999'
             |    OR BI_IFAR_ORG.ORG_NO_2 = '99H999'
             |    OR BI_IFAR_ORG.ORG_NO_3 = '99H999'
             |    OR BI_IFAR_ORG.ORG_NO_4 = '99H999'
             |    OR BI_IFAR_ORG.ORG_NO_5 = '99H999'
             |    OR BI_IFAR_ORG.ORG_NO_6 = '99H999'
             |    OR BI_IFAR_ORG.ORG_NO_7 = '99H999')
             |  AND (SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.DTE >= '20230803'
             |    AND SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.DTE <= '20230804')
             |  AND (((SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.AGE >= 0 AND SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.AGE <= 200)) AND
             |       (BI_IFAR_ORG.ORG_NO_0 = '99H999' AND BI_IFAR_ORG.ORG_NAM_3 != ''))
             |GROUP BY BI_IFAR_ORG.ORG_NAM_3, SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.BUS_TYP
             |ORDER BY BI_IFAR_ORG.ORG_NAM_3 ASC, SV_10DMP_S_TAG_DTAL_IDV_HQCKTR.BUS_TYP ASC
             |""".stripMargin
        )
        // df.queryExecution.debug.codegen
        df.explain(false)
        val result = df.collect() // .show(100, false)  //.collect()
        df.explain(false)
        val plan = df.queryExecution.executedPlan
        val logicalPlan = df.queryExecution.logical
        collectAllJoinSide(plan)
        println(result.length)
        result.foreach(r => println(r.mkString(",")))
        val tookTime = (System.nanoTime() - startTime) / 1000000
        println(s"Execute $i time, time: $tookTime")
        tookTimeArr += tookTime
        // Thread.sleep(5000)
      }

      println(tookTimeArr.mkString(","))

      if (executedCnt >= 30) {
        import spark.implicits._
        val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
        df.summary().show(100, truncate = false)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def createTablesNew(spark: SparkSession): Unit = {
    val bucketSQL =
      GenTPCHTableScripts.genTPCHParquetBucketTables("/data1/test_output/tpch-data-sf100-bucket")

    for (sql <- bucketSQL) {
      println(s"execute: $sql")
      spark.sql(sql).show(10, truncate = false)
    }
    spark
      .sql(s"""
              |show tables;
              |""".stripMargin)
      .show(1000, false)
  }

  def testMergeTreeBucketTables(spark: SparkSession): Unit = {
    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS tpch10_ch_bucket;
                 |""".stripMargin)
    spark.sql(s"""
                 |use tpch10_ch_bucket;
                 |""".stripMargin)
    spark
      .sql(s"""
              |show tables;
              |""".stripMargin)
      .show(1000, truncate = false)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
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
                 | USING clickhouse
                 | LOCATION 'file:///data1/gazelle-jni-warehouse/tpch10_ch_bucket/lineitem'
                 | CLUSTERED BY (l_orderkey) SORTED BY (l_shipdate, l_orderkey) INTO 4 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS orders;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS orders (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    double,
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 | USING clickhouse
                 | LOCATION 'file:///data1/gazelle-jni-warehouse/tpch10_ch_bucket/orders'
                 | CLUSTERED BY (o_orderkey) SORTED BY (o_orderkey, o_orderdate) INTO 4 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS nation;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING clickhouse
                 | LOCATION 'file:///data1/gazelle-jni-warehouse/tpch10_ch_bucket/nation'
                 | CLUSTERED BY (n_nationkey) SORTED BY (n_nationkey) INTO 1 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS part;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS part (
                 | p_partkey     bigint,
                 | p_name        string,
                 | p_mfgr        string,
                 | p_brand       string,
                 | p_type        string,
                 | p_size        bigint,
                 | p_container   string,
                 | p_retailprice double,
                 | p_comment     string)
                 | USING clickhouse
                 | LOCATION 'file:///data1/gazelle-jni-warehouse/tpch10_ch_bucket/part'
                 | CLUSTERED BY (p_partkey) SORTED BY (p_partkey) INTO 4 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS partsupp;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint,
                 | ps_suppkey    bigint,
                 | ps_availqty   bigint,
                 | ps_supplycost double,
                 | ps_comment    string)
                 | USING clickhouse
                 | LOCATION 'file:///data1/gazelle-jni-warehouse/tpch10_ch_bucket/partsupp'
                 | CLUSTERED BY (ps_partkey) SORTED BY (ps_partkey) INTO 4 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS region;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS region (
                 | r_regionkey bigint,
                 | r_name      string,
                 | r_comment   string)
                 | USING clickhouse
                 | LOCATION 'file:///data1/gazelle-jni-warehouse/tpch10_ch_bucket/region'
                 | CLUSTERED BY (r_regionkey) SORTED BY (r_regionkey) INTO 1 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS customer;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    double,
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING clickhouse
                 | LOCATION 'file:///data1/gazelle-jni-warehouse/tpch10_ch_bucket/customer'
                 | CLUSTERED BY (c_custkey) SORTED BY (c_custkey) INTO 4 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS supplier;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE EXTERNAL TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   double,
                 | s_comment   string)
                 | USING clickhouse
                 | LOCATION 'file:///data1/gazelle-jni-warehouse/tpch10_ch_bucket/supplier'
                 | CLUSTERED BY (s_suppkey) SORTED BY (s_suppkey) INTO 1 BUCKETS;
                 |""".stripMargin)

    spark
      .sql(s"""
              |show tables;
              |""".stripMargin)
      .show(1000, truncate = false)

    spark
      .sql(s"""
              |desc formatted lineitem;
              |""".stripMargin)
      .show(1000, truncate = false)

    spark.sql(s"""
                 |use default;
                 |""".stripMargin)
    spark
      .sql(
        s"""
           |show tables;
           |""".stripMargin
      )
      .show(1000, false)
  }

  def testSepScanRDD(spark: SparkSession, executedCnt: Int): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      val df = spark.sql(s"""
                            |SELECT /*+ SHUFFLE_HASH(ch_lineitem100) */
                            |    100.00 * sum(
                            |        CASE WHEN p_type LIKE 'PROMO%' THEN
                            |            l_extendedprice * (1 - l_discount)
                            |        ELSE
                            |            0
                            |        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
                            |FROM
                            |    ch_lineitem100,
                            |    ch_part100
                            |WHERE
                            |    l_partkey = p_partkey
                            |    AND l_shipdate >= date'1995-09-01'
                            |    AND l_shipdate < date'1995-09-01' + interval 1 month;
                            |
                            |""".stripMargin) // .show(30, false)
      df.explain(false)
      val plan = df.queryExecution.executedPlan
      // df.queryExecution.debug.codegen
      val result = df.collect() // .show(100, false)  //.collect()
      println(result.length)
      result.foreach(r => println(r.mkString(",")))
      // .show(30, false)
      // .explain(false)
      // .collect()
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute $i time, time: $tookTime")
      tookTimeArr += tookTime
      // Thread.sleep(5000)
    }

    println(tookTimeArr.mkString(","))

    if (executedCnt >= 10) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
      df.summary().show(100, truncate = false)
    }
  }

  def refreshClickHouseTable(spark: SparkSession): Unit = {
    spark
      .sql(s"""
              | refresh table $tableName
              |""".stripMargin)
      .show(100, truncate = false)
    spark
      .sql(s"""
              | desc formatted $tableName
              |""".stripMargin)
      .show(100, truncate = false)
    spark
      .sql(s"""
              | refresh table ch_clickhouse
              |""".stripMargin)
      .show(100, truncate = false)
    spark
      .sql(s"""
              | desc formatted ch_clickhouse
              |""".stripMargin)
      .show(100, truncate = false)
  }

  def testTPCHAll(spark: SparkSession): Unit = {
    spark
      .sql(s"""
              |use tpch100bucketdb;
              |""".stripMargin)
      .show(1000, truncate = false)
    val tookTimeArr = ArrayBuffer[Long]()
    val executedCnt = 1
    val executeExplain = false
    val sqlFilePath = "/home/myubuntu/Works/myworks/native-runtime/tpch-queries-spark-nohint/"
    for (i <- 1 to 22) {
      if (i != 21) {
        val sqlNum = "q" + "%02d".format(i)
        val sqlFile = sqlFilePath + sqlNum + ".sql"
        val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
        println("")
        println("")
        println(s"execute sql: $sqlNum")
        for (j <- 1 to executedCnt) {
          val startTime = System.nanoTime()
          val df = spark.sql(sqlStr)
          val result = df.collect()
          if (executeExplain) df.explain(false)
          collectAllJoinSide(df.queryExecution.executedPlan)
          println(result.length)
          result.foreach(r => println(r.mkString(",")))
          // .show(30, false)
          // .explain(false)
          // .collect()
          val tookTime = (System.nanoTime() - startTime) / 1000000
          // println(s"Execute ${i} time, time: ${tookTime}")
          tookTimeArr += tookTime
        }
      }
    }

    // println(tookTimeArr.mkString(","))

    if (executedCnt >= 10) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
      df.summary().show(100, truncate = false)
    }
  }

  def testParquetFile1(spark: SparkSession, executedCnt: Int): Unit = {
    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS `ds_dpa_js_th` (
                 |	`unique_no` STRING COMMENT '唯一键',
                 |	`gj_value` DECIMAL(36, 18) COMMENT '归集结果值（金额、数量）',
                 |	`ft_flog_code` STRING COMMENT '分摊标识',
                 |	`path_flog_code` STRING COMMENT '路径标识',
                 |	`sub_src_system_code` STRING COMMENT '细分来源系统（微应用）',
                 |	`data_grain_code` STRING COMMENT '来源数据颗粒度',
                 |	`gl_date` STRING COMMENT 'GL日期',
                 |	`period_code` STRING COMMENT '期间',
                 |	`bup_code` STRING COMMENT '账套',
                 |	`hs_account_code` STRING COMMENT '核算科目',
                 |	`ys_account_code` STRING COMMENT '预算科目',
                 |	`specialty_code` STRING COMMENT '管会专业',
                 |	`coa_com_code` STRING COMMENT '公司段',
                 |	`coa_dept_code` STRING COMMENT '部门段',
                 |	`coa_inter_code` STRING COMMENT '内往段',
                 |	`inst_code` STRING COMMENT '主数据机构',
                 |	`resp_center_lv7_code` STRING COMMENT '七级责任中心',
                 |	`resp_center_lv6_code` STRING COMMENT '六级责任中心',
                 |	`resp_center_lv5_code` STRING COMMENT '五级责任中心',
                 |	`resp_center_lv4_code` STRING COMMENT '四级责任中心',
                 |	`resp_center_lv3_code` STRING COMMENT '三级责任中心',
                 |	`resp_center_lv2_code` STRING COMMENT '二级责任中心',
                 |	`resp_center_lv1_code` STRING COMMENT '一级责任中心',
                 |	`ltb_mgmt_code` STRING COMMENT '管理揽投部编码',
                 |	`ltb_mgmt_name` STRING COMMENT '管理揽投部名称',
                 |	`ltb_mgmt_zysx_code` STRING COMMENT '管理揽投部作业属性编码',
                 |	`ltb_mgmt_zysx_name` STRING COMMENT '管理揽投部作业属性名称',
                 |	`ltb_mgmt_fl_code` STRING COMMENT '管理揽投部分类编码',
                 |	`ltb_mgmt_fl_name` STRING COMMENT '管理揽投部分类名称',
                 |	`ltb_flag` STRING COMMENT '是否揽投部',
                 |	`prcs_inst_code` STRING COMMENT '处理机构',
                 |	`trsp_inst_code` STRING COMMENT '运输机构',
                 |	`merge_level_code` STRING COMMENT '合并层级',
                 |	`report_item_code` STRING COMMENT '损益分析项',
                 |	`dp_item_code` STRING COMMENT '单票所需项目',
                 |	`business_item_name` STRING COMMENT '经济事项名称',
                 |	`product_code` STRING COMMENT '产品',
                 |	`product_lv5_code` STRING COMMENT '产品五级编码',
                 |	`product_lv4_code` STRING COMMENT '产品四级编码',
                 |	`product_lv3_code` STRING COMMENT '产品三级编码',
                 |	`product_lv2_code` STRING COMMENT '产品二级编码',
                 |	`product_biz_type_code` STRING COMMENT '产品业务分类',
                 |	`inst_biz_type_code` STRING COMMENT '机构业务分类',
                 |	`biz_type_code` STRING COMMENT '业务分类',
                 |	`employee_type_code` STRING COMMENT '人员类型（揽收/收寄/投递/内部处理/驾驶员）',
                 |	`outsource_flag` STRING COMMENT '是否外包（揽收/收寄/投递/内部处理/驾驶员）',
                 |	`waybill_id` STRING COMMENT '邮件id',
                 |	`waybill_no` STRING COMMENT '邮件号',
                 |	`route_code` STRING COMMENT '邮路编码',
                 |	`route_type_code` STRING COMMENT '邮路种类',
                 |	`routebill_no` STRING COMMENT '路单流水',
                 |	`dispatch_no` STRING COMMENT '派车单号',
                 |	`supplier_code` STRING COMMENT '供应商',
                 |	`vehicle_no` STRING COMMENT '车辆编码（代码）',
                 |	`licence_no` STRING COMMENT '车牌号',
                 |	`route_run_type_code` STRING COMMENT '邮路自委办标识',
                 |	`vehicle_run_type_code` STRING COMMENT '车辆自委办标识',
                 |	`step_code` STRING COMMENT '环节',
                 |	`dp_step_code` STRING COMMENT '单票环节',
                 |	`route_step_code` STRING COMMENT '邮路环节',
                 |	`vehicle_step_code` STRING COMMENT '车辆环节',
                 |	`analyse_step_lv3_code` STRING COMMENT '三级分析环节',
                 |	`analyse_step_lv2_code` STRING COMMENT '二级分析环节',
                 |	`analyse_step_lv1_code` STRING COMMENT '一级分析环节',
                 |	`asset_no` STRING COMMENT '资产编号',
                 |	`by_machine_flag_code` STRING COMMENT '是否上机',
                 |	`machine_barcode` STRING COMMENT '设备条码（包分机）',
                 |	`reserved1` STRING COMMENT '备用字段1',
                 |	`reserved2` STRING COMMENT '备用字段2',
                 |	`reserved3` STRING COMMENT '备用字段3',
                 |	`reserved4` STRING COMMENT '备用字段4',
                 |	`reserved5` STRING COMMENT '备用字段5',
                 |	`reserved6` STRING COMMENT '备用字段6',
                 |	`reserved7` STRING COMMENT '备用字段7',
                 |	`reserved8` STRING COMMENT '备用字段8',
                 |	`reserved9` STRING COMMENT '备用字段9',
                 |	`reserved10` STRING COMMENT '备用字段10',
                 |	`reserved11` STRING COMMENT '备用字段11',
                 |	`reserved12` STRING COMMENT '备用字段12',
                 |	`reserved13` STRING COMMENT '备用字段13',
                 |	`reserved14` STRING COMMENT '备用字段14',
                 |	`reserved15` STRING COMMENT '备用字段15',
                 |	`reserved16` STRING COMMENT '备用字段16',
                 |	`reserved17` STRING COMMENT '备用字段17',
                 |	`reserved18` STRING COMMENT '备用字段18',
                 |	`reserved19` STRING COMMENT '备用字段19',
                 |	`reserved20` STRING COMMENT '备用字段20',
                 |	`reserved21` STRING COMMENT '备用字段21',
                 |	`reserved22` STRING COMMENT '备用字段22',
                 |	`reserved23` STRING COMMENT '备用字段23',
                 |	`reserved24` STRING COMMENT '备用字段24',
                 |	`reserved25` STRING COMMENT '备用字段25',
                 |	`reserved26` STRING COMMENT '备用字段26',
                 |	`reserved27` STRING COMMENT '备用字段27',
                 |	`reserved28` STRING COMMENT '备用字段28',
                 |	`reserved29` STRING COMMENT '备用字段29',
                 |	`reserved30` STRING COMMENT '备用字段30',
                 |	`reserved31` STRING COMMENT '备用字段31',
                 |	`reserved32` STRING COMMENT '备用字段32',
                 |	`reserved33` STRING COMMENT '备用字段33',
                 |	`reserved34` STRING COMMENT '备用字段34',
                 |	`reserved35` STRING COMMENT '备用字段35',
                 |	`reserved36` STRING COMMENT '备用字段36',
                 |	`reserved37` STRING COMMENT '备用字段37',
                 |	`reserved38` STRING COMMENT '备用字段38',
                 |	`reserved39` STRING COMMENT '备用字段39',
                 |	`reserved40` STRING COMMENT '备用字段40',
                 |	`post_resp_center_lv4_code` STRING COMMENT '揽收四级责任中心',
                 |	`post_resp_center_lv3_code` STRING COMMENT '揽收三级责任中心',
                 |	`post_resp_center_lv2_code` STRING COMMENT '揽收二级责任中心',
                 |	`scene_code` STRING COMMENT '场景',
                 |	`centralized_process_type` STRING COMMENT '集中补录调整类型',
                 |	`offset_type` STRING COMMENT '抵消类型',
                 |	`post_type` STRING COMMENT '收寄方式',
                 |	`post_inst_code` STRING COMMENT '收寄机构',
                 |	`dlv_type` STRING COMMENT '投递方式',
                 |	`dlv_inst_code` STRING COMMENT '投递机构',
                 |	`ratio` DECIMAL(36, 18) COMMENT '系数',
                 |	`unique_no_dt` STRING COMMENT 'DT数据唯一标识',
                 |	`unique_no_yz` STRING COMMENT 'YZ数据唯一标识',
                 |	`value_dt` DECIMAL(36, 18) COMMENT 'DT值',
                 |	`value_yz` DECIMAL(36, 18) COMMENT 'YZ值',
                 |	`value_yz_sum` DECIMAL(36, 18) COMMENT 'YZ值分组合计'
                 |)
                 |PARTITIONED BY (
                 |	ps_month STRING COMMENT '月份',
                 |	ps_r2_code STRING COMMENT '省份',
                 |	ps_ft_path_code STRING COMMENT '分摊路径'
                 |)
                 |stored as parquet
                 |LOCATION 'file:///data1/test_output/youzheng/ds_dpa_js_th';
                 |""".stripMargin)

    spark.sql(s"""
                 |MSCK REPAIR TABLE ds_dpa_js_th;
                 |""".stripMargin)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS ds_dpa_f_dt_fdp_parquet_dest;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS `ds_dpa_f_dt_fdp_parquet_dest` (
                 |`reserved9` DATE COMMENT '备用字段9',
                 |`reserved10` DATE COMMENT '备用字段10'
                 |)
                 |COMMENT '数据总表'
                 |PARTITIONED BY (
                 |ps_month STRING,
                 |ps_resp_center STRING,
                 |ps_ft_flog_code STRING
                 |)
                 |stored as parquet
                 |LOCATION 'file:///data1/test_output/youzheng/ds_dpa_f_dt_fdp_parquet_dest';
                 |""".stripMargin)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ds_dpa_f_dt_fdp_parquet_dest PARTITION(ps_month = '202307',
         |ps_resp_center = 'M33-P', ps_ft_flog_code = 'FI01000031')
         |SELECT reserved9
         |,reserved10
         |FROM    ds_dpa_js_th
         |WHERE   ps_month = '202307'
         |AND     ps_r2_code = 'M33-P'
         |AND     ps_ft_path_code = '0524';
         |""".stripMargin
    )
    // df.write.mode(SaveMode.Overwrite).parquet("/data1/test_output/youzheng/ds_dpa_js_th_tmp")
    // df.explain(false)
  }

  def testParquetFile(spark: SparkSession, executedCnt: Int): Unit = {
    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS `ds_dpa_js_dt_raw` (
                 |	`unique_no` STRING COMMENT '唯一键',
                 |	`gj_value` DECIMAL(18, 8) COMMENT '归集结果值',
                 |	`ft_flog_code` STRING COMMENT '计算标识',
                 |	`path_flog_code` STRING COMMENT '路径标识',
                 |	`sub_src_system_code` STRING COMMENT '细分来源系统（微应用）',
                 |	`data_grain_code` STRING COMMENT '来源数据颗粒度',
                 |	`gl_date` STRING COMMENT 'GL日期',
                 |	`period_code` STRING COMMENT '期间',
                 |	`bup_code` STRING COMMENT '账套',
                 |	`hs_account_code` STRING COMMENT '核算科目',
                 |	`ys_account_code` STRING COMMENT '预算科目',
                 |	`specialty_code` STRING COMMENT '管会专业',
                 |	`coa_com_code` STRING COMMENT '公司段',
                 |	`coa_dept_code` STRING COMMENT '部门段',
                 |	`coa_inter_code` STRING COMMENT '内往段',
                 |	`inst_code` STRING COMMENT '主数据机构',
                 |	`resp_center_lv7_code` STRING COMMENT '七级责任中心',
                 |	`resp_center_lv6_code` STRING COMMENT '六级责任中心',
                 |	`resp_center_lv5_code` STRING COMMENT '五级责任中心',
                 |	`resp_center_lv4_code` STRING COMMENT '四级责任中心',
                 |	`resp_center_lv3_code` STRING COMMENT '三级责任中心',
                 |	`resp_center_lv2_code` STRING COMMENT '二级责任中心',
                 |	`resp_center_lv1_code` STRING COMMENT '一级责任中心',
                 |	`ltb_mgmt_code` STRING COMMENT '管理揽投部编码',
                 |	`ltb_mgmt_name` STRING COMMENT '管理揽投部名称',
                 |	`ltb_mgmt_zysx_code` STRING COMMENT '管理揽投部作业属性编码',
                 |	`ltb_mgmt_zysx_name` STRING COMMENT '管理揽投部作业属性名称',
                 |	`ltb_mgmt_fl_code` STRING COMMENT '管理揽投部分类编码',
                 |	`ltb_mgmt_fl_name` STRING COMMENT '管理揽投部分类名称',
                 |	`ltb_flag` STRING COMMENT '是否揽投部',
                 |	`prcs_inst_code` STRING COMMENT '处理机构',
                 |	`trsp_inst_code` STRING COMMENT '运输机构',
                 |	`merge_level_code` STRING COMMENT '合并层级',
                 |	`report_item_code` STRING COMMENT '损益分析项',
                 |	`dp_item_code` STRING COMMENT '单票所需项目',
                 |	`business_item_name` STRING COMMENT '经济事项名称',
                 |	`product_code` STRING COMMENT '产品',
                 |	`product_lv5_code` STRING COMMENT '产品五级编码',
                 |	`product_lv4_code` STRING COMMENT '产品四级编码',
                 |	`product_lv3_code` STRING COMMENT '产品三级编码',
                 |	`product_lv2_code` STRING COMMENT '产品二级编码',
                 |	`product_biz_type_code` STRING COMMENT '产品业务分类',
                 |	`inst_biz_type_code` STRING COMMENT '机构业务分类',
                 |	`biz_type_code` STRING COMMENT '业务分类',
                 |	`employee_type_code` STRING COMMENT '人员类型（揽收/收寄/投递/内部处理/驾驶员）',
                 |	`outsource_flag` STRING COMMENT '是否外包（揽收/收寄/投递/内部处理/驾驶员）',
                 |	`waybill_id` STRING COMMENT '邮件id',
                 |	`waybill_no` STRING COMMENT '邮件号',
                 |	`route_code` STRING COMMENT '邮路编码',
                 |	`route_type_code` STRING COMMENT '邮路种类',
                 |	`routebill_no` STRING COMMENT '路单流水',
                 |	`dispatch_no` STRING COMMENT '派车单号',
                 |	`supplier_code` STRING COMMENT '供应商',
                 |	`vehicle_no` STRING COMMENT '车辆编码（代码）',
                 |	`licence_no` STRING COMMENT '车牌号',
                 |	`route_run_type_code` STRING COMMENT '邮路自委办标识',
                 |	`vehicle_run_type_code` STRING COMMENT '车辆自委办标识',
                 |	`step_code` STRING COMMENT '环节',
                 |	`dp_step_code` STRING COMMENT '单票环节',
                 |	`route_step_code` STRING COMMENT '邮路环节',
                 |	`vehicle_step_code` STRING COMMENT '车辆环节',
                 |	`analyse_step_lv3_code` STRING COMMENT '三级分析环节',
                 |	`analyse_step_lv2_code` STRING COMMENT '二级分析环节',
                 |	`analyse_step_lv1_code` STRING COMMENT '一级分析环节',
                 |	`asset_no` STRING COMMENT '资产编号',
                 |	`by_machine_flag_code` STRING COMMENT '是否上机',
                 |	`machine_barcode` STRING COMMENT '设备条码（包分机）',
                 |	`reserved1` STRING COMMENT '备用字段1',
                 |	`reserved2` STRING COMMENT '备用字段2',
                 |	`reserved3` STRING COMMENT '备用字段3',
                 |	`reserved4` STRING COMMENT '备用字段4',
                 |	`reserved5` STRING COMMENT '备用字段5',
                 |	`reserved6` STRING COMMENT '备用字段6',
                 |	`reserved7` STRING COMMENT '备用字段7',
                 |	`reserved8` STRING COMMENT '备用字段8',
                 |	`reserved9` STRING COMMENT '备用字段9',
                 |	`reserved10` STRING COMMENT '备用字段10',
                 |	`reserved11` STRING COMMENT '备用字段11',
                 |	`reserved12` STRING COMMENT '备用字段12',
                 |	`reserved13` STRING COMMENT '备用字段13',
                 |	`reserved14` STRING COMMENT '备用字段14',
                 |	`reserved15` STRING COMMENT '备用字段15',
                 |	`reserved16` STRING COMMENT '备用字段16',
                 |	`reserved17` STRING COMMENT '备用字段17',
                 |	`reserved18` STRING COMMENT '备用字段18',
                 |	`reserved19` STRING COMMENT '备用字段19',
                 |	`reserved20` STRING COMMENT '备用字段20',
                 |	`reserved21` STRING COMMENT '备用字段21',
                 |	`reserved22` STRING COMMENT '备用字段22',
                 |	`reserved23` STRING COMMENT '备用字段23',
                 |	`reserved24` STRING COMMENT '备用字段24',
                 |	`reserved25` STRING COMMENT '备用字段25',
                 |	`reserved26` STRING COMMENT '备用字段26',
                 |	`reserved27` STRING COMMENT '备用字段27',
                 |	`reserved28` STRING COMMENT '备用字段28',
                 |	`reserved29` STRING COMMENT '备用字段29',
                 |	`reserved30` STRING COMMENT '备用字段30',
                 |	`reserved31` STRING COMMENT '备用字段31',
                 |	`reserved32` STRING COMMENT '备用字段32',
                 |	`reserved33` STRING COMMENT '备用字段33',
                 |	`reserved34` STRING COMMENT '备用字段34',
                 |	`reserved35` STRING COMMENT '备用字段35',
                 |	`reserved36` STRING COMMENT '备用字段36',
                 |	`reserved37` STRING COMMENT '备用字段37',
                 |	`reserved38` STRING COMMENT '备用字段38',
                 |	`reserved39` STRING COMMENT '备用字段39',
                 |	`reserved40` STRING COMMENT '备用字段40',
                 |	`post_resp_center_lv4_code` STRING COMMENT '揽收四级责任中心',
                 |	`post_resp_center_lv3_code` STRING COMMENT '揽收三级责任中心',
                 |	`post_resp_center_lv2_code` STRING COMMENT '揽收二级责任中心',
                 |	`scene_code` STRING COMMENT '场景',
                 |	`centralized_process_type` STRING COMMENT '集中补录调整类型',
                 |	`offset_type` STRING COMMENT '抵消类型',
                 |	`post_type` STRING COMMENT '收寄方式',
                 |	`post_inst_code` STRING COMMENT '收寄机构',
                 |	`dlv_type` STRING COMMENT '投递方式',
                 |	`dlv_inst_code` STRING COMMENT '投递机构',
                 |	`ratio` DECIMAL(18, 8) COMMENT '系数'
                 |)
                 |PARTITIONED BY (
                 |	ps_month STRING COMMENT '月份',
                 |	ps_r2_code STRING COMMENT '省份',
                 |	ps_ft_path_code STRING COMMENT '计算路径'
                 |)
                 |stored as parquet
                 |LOCATION 'file:///data1/test_output/youzheng/ds_dpa_js_dt_raw';
                 |""".stripMargin)

    spark.sql(s"""
                 |MSCK REPAIR TABLE ds_dpa_js_dt_raw;
                 |""".stripMargin)

    val df = spark.sql(
      s"""
         |SELECT uuid(),
         |       gj_value,
         |       null,
         |       null,
         |       sub_src_system_code,
         |       data_grain_code,
         |       gl_date,
         |       null,
         |       bup_code,
         |       hs_account_code,
         |       ys_account_code,
         |       specialty_code,
         |       coa_com_code,
         |       coa_dept_code,
         |       coa_inter_code,
         |       inst_code,
         |       resp_center_lv7_code,
         |       resp_center_lv6_code,
         |       resp_center_lv5_code,
         |       resp_center_lv4_code,
         |       resp_center_lv3_code,
         |       resp_center_lv2_code,
         |       resp_center_lv1_code,
         |       ltb_mgmt_code,
         |       ltb_mgmt_name,
         |       ltb_mgmt_zysx_code,
         |       ltb_mgmt_zysx_name,
         |       ltb_mgmt_fl_code,
         |       ltb_mgmt_fl_name,
         |       ltb_flag,
         |       prcs_inst_code,
         |       trsp_inst_code,
         |       merge_level_code,
         |       report_item_code,
         |       dp_item_code,
         |       business_item_name,
         |       null,
         |       null,
         |       product_lv4_code,
         |       null,
         |       null,
         |       product_biz_type_code,
         |       inst_biz_type_code,
         |       biz_type_code,
         |       null,
         |       null,
         |       null,
         |       null,
         |       route_code,
         |       route_type_code,
         |       routebill_no,
         |       dispatch_no,
         |       supplier_code,
         |       vehicle_no,
         |       licence_no,
         |       route_run_type_code,
         |       vehicle_run_type_code,
         |       step_code,
         |       dp_step_code,
         |       route_step_code,
         |       vehicle_step_code,
         |       analyse_step_lv3_code,
         |       analyse_step_lv2_code,
         |       analyse_step_lv1_code,
         |       asset_no,
         |       by_machine_flag_code,
         |       machine_barcode,
         |       null,
         |       null,
         |       null,
         |       null,
         |       null,
         |       reserved6,
         |       reserved7,
         |       reserved8,
         |       null,
         |       reserved10,
         |       reserved11,
         |       reserved12,
         |       reserved13,
         |       reserved14,
         |       reserved15,
         |       reserved16,
         |       reserved17,
         |       reserved18,
         |       reserved19,
         |       reserved20,
         |       reserved21,
         |       reserved22,
         |       reserved23,
         |       reserved24,
         |       reserved25,
         |       reserved26,
         |       reserved27,
         |       reserved28,
         |       reserved29,
         |       reserved30,
         |       reserved31,
         |       reserved32,
         |       reserved33,
         |       reserved34,
         |       reserved35,
         |       reserved36,
         |       reserved37,
         |       reserved38,
         |       reserved39,
         |       reserved40,
         |       post_resp_center_lv4_code,
         |       post_resp_center_lv3_code,
         |       post_resp_center_lv2_code,
         |       scene_code,
         |       centralized_process_type,
         |       offset_type,
         |       null,
         |       null,
         |       null,
         |       null,
         |       null
         |FROM (SELECT sum(gj_value) as gj_value,
         |             analyse_step_lv1_code,
         |             analyse_step_lv2_code,
         |             analyse_step_lv3_code,
         |             asset_no,
         |             biz_type_code,
         |             bup_code,
         |             business_item_name,
         |             by_machine_flag_code,
         |             centralized_process_type,
         |             coa_com_code,
         |             coa_dept_code,
         |             coa_inter_code,
         |             data_grain_code,
         |             dispatch_no,
         |             dp_item_code,
         |             dp_step_code,
         |             gl_date,
         |             hs_account_code,
         |             inst_biz_type_code,
         |             inst_code,
         |             licence_no,
         |             ltb_flag,
         |             ltb_mgmt_code,
         |             ltb_mgmt_fl_code,
         |             ltb_mgmt_fl_name,
         |             ltb_mgmt_name,
         |             ltb_mgmt_zysx_code,
         |             ltb_mgmt_zysx_name,
         |             machine_barcode,
         |             merge_level_code,
         |             offset_type,
         |             post_resp_center_lv2_code,
         |             post_resp_center_lv3_code,
         |             post_resp_center_lv4_code,
         |             prcs_inst_code,
         |             product_biz_type_code,
         |             product_lv4_code,
         |             report_item_code,
         |             reserved10,
         |             reserved11,
         |             reserved12,
         |             reserved13,
         |             reserved14,
         |             reserved15,
         |             reserved16,
         |             reserved17,
         |             reserved18,
         |             reserved19,
         |             reserved20,
         |             reserved21,
         |             reserved22,
         |             reserved23,
         |             reserved24,
         |             reserved25,
         |             reserved26,
         |             reserved27,
         |             reserved28,
         |             reserved29,
         |             reserved30,
         |             reserved31,
         |             reserved32,
         |             reserved33,
         |             reserved34,
         |             reserved35,
         |             reserved36,
         |             reserved37,
         |             reserved38,
         |             reserved39,
         |             reserved40,
         |             reserved6,
         |             reserved7,
         |             reserved8,
         |             resp_center_lv1_code,
         |             resp_center_lv2_code,
         |             resp_center_lv3_code,
         |             resp_center_lv4_code,
         |             resp_center_lv5_code,
         |             resp_center_lv6_code,
         |             resp_center_lv7_code,
         |             route_code,
         |             route_run_type_code,
         |             route_step_code,
         |             route_type_code,
         |             routebill_no,
         |             scene_code,
         |             specialty_code,
         |             step_code,
         |             sub_src_system_code,
         |             supplier_code,
         |             trsp_inst_code,
         |             vehicle_no,
         |             vehicle_run_type_code,
         |             vehicle_step_code,
         |             ys_account_code
         |      FROM ds_dpa_js_dt_raw
         |      WHERE ps_month = '202307'
         |        AND ps_r2_code = 'M33-P'
         |        AND ps_ft_path_code = '0524'
         |      GROUP BY analyse_step_lv1_code, analyse_step_lv2_code, analyse_step_lv3_code, asset_no, biz_type_code, bup_code,
         |               business_item_name, by_machine_flag_code, centralized_process_type, coa_com_code, coa_dept_code,
         |               coa_inter_code, data_grain_code, dispatch_no, dp_item_code, dp_step_code, gl_date, hs_account_code,
         |               inst_biz_type_code, inst_code, licence_no, ltb_flag, ltb_mgmt_code, ltb_mgmt_fl_code, ltb_mgmt_fl_name,
         |               ltb_mgmt_name, ltb_mgmt_zysx_code, ltb_mgmt_zysx_name, machine_barcode, merge_level_code, offset_type,
         |               post_resp_center_lv2_code, post_resp_center_lv3_code, post_resp_center_lv4_code, prcs_inst_code,
         |               product_biz_type_code, product_lv4_code, report_item_code, reserved10, reserved11, reserved12,
         |               reserved13, reserved14, reserved15, reserved16, reserved17, reserved18, reserved19, reserved20,
         |               reserved21, reserved22, reserved23, reserved24, reserved25, reserved26, reserved27, reserved28,
         |               reserved29, reserved30, reserved31, reserved32, reserved33, reserved34, reserved35, reserved36,
         |               reserved37, reserved38, reserved39, reserved40, reserved6, reserved7, reserved8, resp_center_lv1_code,
         |               resp_center_lv2_code, resp_center_lv3_code, resp_center_lv4_code, resp_center_lv5_code,
         |               resp_center_lv6_code, resp_center_lv7_code, route_code, route_run_type_code, route_step_code,
         |               route_type_code, routebill_no, scene_code, specialty_code, step_code, sub_src_system_code, supplier_code,
         |               trsp_inst_code, vehicle_no, vehicle_run_type_code, vehicle_step_code, ys_account_code);
         |
         |""".stripMargin
    )
    df.collect()
    df.explain(false)
  }

  def testJoinIssue(spark: SparkSession): Unit = {
    spark
      .sql(s"""
              |select
              |  l_returnflag, l_extendedprice, p_name
              |from
              |  ch_lineitem,
              |  ch_part
              |where
              |  l_partkey = p_partkey
              |""".stripMargin)
      .show(10, truncate = false) // .show(10, false) .explain("extended")
    /* spark.sql(
      s"""
         |select
         |  l_returnflag, sum(l_extendedprice * l_discount)
         |from
         |  ch_lineitem01,
         |  ch_part01
         |where
         |  l_partkey = p_partkey
         |group by l_returnflag
         |""".stripMargin).show(10, false)       // .show(10, false) .explain("extended")
    spark.sql(
      s"""
         |select
         |  l_orderkey,
         |  l_tax,
         |  l_returnflag
         |from
         |  ch_lineitem01,
         |  ch_customer01,
         |  ch_orders01
         |where
         |  c_custkey = o_custkey
         |  and l_orderkey = o_orderkey
         |""".stripMargin).show(10, false) // .show(10, false) .explain("extended") */
    /* spark.sql(
      s"""
         |SELECT
         |    sum(l_extendedprice * l_discount)
         |FROM
         |    ch_orders01,
         |    ch_lineitem01
         |WHERE
         |    l_orderkey = o_orderkey
         |""".stripMargin).explain("extended") // .show(10, false) .explain("extended") */

    /* spark.sql(
      s"""
         |-- Q4
         |SELECT
         |    o_orderpriority,
         |    count(*) AS order_count
         |FROM
         |    ch_orders
         |WHERE
         |    o_orderdate >= date'1993-07-01'
         |    AND o_orderdate < date'1993-07-01' + interval 3 month
         |    AND EXISTS (
         |        SELECT
         |            *
         |        FROM
         |            ch_lineitem
         |        WHERE
         |            l_orderkey = o_orderkey
         |            AND l_commitdate < l_receiptdate)
         |GROUP BY
         |    o_orderpriority
         |ORDER BY
         |    o_orderpriority;
         |
         |""".stripMargin).collect() //.show(30, false)
    spark.sql(
      s"""
         |-- Q3
         |SELECT
         |    l_orderkey,
         |    sum(l_extendedprice * (1 - l_discount)) AS revenue,
         |    o_orderdate,
         |    o_shippriority
         |FROM
         |    ch_customer,
         |    ch_orders,
         |    ch_lineitem
         |WHERE
         |    c_mktsegment = 'BUILDING'
         |    AND c_custkey = o_custkey
         |    AND l_orderkey = o_orderkey
         |    AND o_orderdate < date'1995-03-15'
         |    AND l_shipdate > date'1995-03-15'
         |GROUP BY
         |    l_orderkey,
         |    o_orderdate,
         |    o_shippriority
         |ORDER BY
         |    revenue DESC,
         |    o_orderdate
         |LIMIT 10;
         |""".stripMargin).collect() // .show(100, false) */
    /* spark.sql(
      s"""
         |SELECT
         |    p_brand,
         |    p_type,
         |    p_size,
         |    count(DISTINCT ps_suppkey) AS supplier_cnt
         |FROM
         |    ch_partsupp,
         |    ch_part
         |WHERE
         |    p_partkey = ps_partkey
         |    AND p_brand <> 'Brand#45'
         |    AND p_type NOT LIKE 'MEDIUM POLISHED%'
         |    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
         |    AND ps_suppkey IN (
         |        SELECT
         |            s_suppkey
         |        FROM
         |            ch_supplier
         |        WHERE
         |            s_comment = '%Customer%Complaints%')
         |GROUP BY
         |    p_brand,
         |    p_type,
         |    p_size
         |
         |""".stripMargin).explain(true) // .show(30, false) // .explain(true) */
  }

  def benchmarkTPCH(spark: SparkSession, executedCnt: Int): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      val df = spark.sql(s"""
                            |SELECT
                            |    l_orderkey,
                            |    sum(l_extendedprice * (1 - l_discount)) AS revenue,
                            |    o_orderdate,
                            |    o_shippriority
                            |FROM
                            |    ch_customer,
                            |    ch_orders,
                            |    ch_lineitem
                            |WHERE
                            |    c_mktsegment = 'BUILDING'
                            |    AND c_custkey = o_custkey
                            |    AND l_orderkey = o_orderkey
                            |    AND o_orderdate < date'1995-03-15'
                            |    AND l_shipdate > date'1995-03-15'
                            |GROUP BY
                            |    l_orderkey,
                            |    o_orderdate,
                            |    o_shippriority
                            |ORDER BY
                            |    revenue DESC,
                            |    o_orderdate
                            |LIMIT 10;
                            |
                            |
                            |
                            |""".stripMargin) // .show(30, false)
      // df.explain(false)
      val result = df.collect() // .show(100, false)  //.collect()
      println(result.length)
      // result.foreach(r => println(r.mkString(",")))
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute $i time, time: $tookTime")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
    df.summary().show(100, truncate = false)
  }

  def testSerializeFromObjectExec(spark: SparkSession): Unit = {
    // spark.conf.set("spark.gluten.enabled", "false")
    val tookTimeArr = Array(12, 23, 56, 100, 500, 20)
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    df.summary().show(100, truncate = false)
  }

  def createClickHouseTable(
      spark: SparkSession,
      parquetFilesPath: String,
      fileFormat: String): Unit = {
    spark
      .sql("""
             | show databases
             |""".stripMargin)
      .show(100, truncate = false)

    spark
      .sql("""
             | show tables
             |""".stripMargin)
      .show(100, truncate = false)

    spark
      .sql(s"""
              | USE default
              |""".stripMargin)
      .show(100, truncate = false)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    // Create a table
    println("Creating a table")
    // PARTITIONED BY (age)
    // engine='MergeTree' or engine='Parquet'
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS $tableName (
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
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 |""".stripMargin)

    spark
      .sql("""
             | show tables
             |""".stripMargin)
      .show(100, truncate = false)

    spark
      .sql(s"""
              | desc formatted $tableName
              |""".stripMargin)
      .show(100, truncate = false)

  }

  def createLocationClickHouseTable(spark: SparkSession): Unit = {
    spark
      .sql(s"""
              | USE default
              |""".stripMargin)
      .show(100, truncate = false)

    spark
      .sql("""
             | show tables
             |""".stripMargin)
      .show(100, truncate = false)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS ch_clickhouse")

    // Create a table
    // PARTITIONED BY (age)
    // engine='MergeTree' or engine='Parquet'
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS ch_clickhouse (
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
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '/data1/gazelle-jni-warehouse/ch_clickhouse'
                 |""".stripMargin)

    spark
      .sql("""
             | show tables
             |""".stripMargin)
      .show(100, truncate = false)
    spark
      .sql(s"""
              | desc formatted ch_clickhouse
              |""".stripMargin)
      .show(100, truncate = false)
  }

  def createClickHouseTablesAsSelect(spark: SparkSession): Unit = {
    val targetTable = "table_as_select"
    spark
      .sql(s"""
              | USE default
              |""".stripMargin)
      .show(10, truncate = false)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS $targetTable")

    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS $targetTable USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | AS SELECT
                 | l_orderkey,
                 | l_partkey,
                 | l_suppkey,
                 | l_linenumber,
                 | l_quantity,
                 | l_extendedprice,
                 | l_discount,
                 | l_tax,
                 | l_returnflag,
                 | l_linestatus,
                 | l_shipdate,
                 | l_commitdate,
                 | l_receiptdate,
                 | l_shipinstruct,
                 | l_shipmode,
                 | l_comment
                 | FROM lineitem;
                 |""".stripMargin)

    spark
      .sql("""
             | show tables
             |""".stripMargin)
      .show(100, truncate = false)

    spark
      .sql(s"""
              | desc formatted $targetTable
              |""".stripMargin)
      .show(100, truncate = false)
    spark
      .sql(s"""
              | select * from $targetTable
              |""".stripMargin)
      .show(10, truncate = false)
  }

  def createClickHouseTablesAndInsert(spark: SparkSession): Unit = {
    val targetTable = "table_insert"
    spark
      .sql(s"""
              | USE default
              |""".stripMargin)
      .show(10, truncate = false)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS $targetTable")

    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS $targetTable (
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
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 |""".stripMargin)

    spark
      .sql("""
             | show tables
             |""".stripMargin)
      .show(100, truncate = false)

    spark
      .sql(s"""
              | desc formatted $targetTable
              |""".stripMargin)
      .show(100, truncate = false)
    spark
      .sql(s"""
              | INSERT INTO $targetTable
              | SELECT
              | l_orderkey,
              | l_partkey,
              | l_suppkey,
              | l_linenumber,
              | l_quantity,
              | l_extendedprice,
              | l_discount,
              | l_tax,
              | l_returnflag,
              | l_linestatus,
              | l_shipdate,
              | l_commitdate,
              | l_receiptdate,
              | l_shipinstruct,
              | l_shipmode,
              | l_comment
              | FROM lineitem;
              |""".stripMargin)
      .show()

    spark
      .sql(s"""
              | select * from $targetTable
              |""".stripMargin)
      .show(10, truncate = false)

  }

  def selectClickHouseTable(
      spark: SparkSession,
      executedCnt: Int,
      sql: String,
      targetTable: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark
        .sql(s"""
                |SELECT
                |    sum(l_extendedprice * l_discount) AS revenue
                |FROM
                |    ch_lineitem
                |WHERE
                |    l_shipdate >= date'1994-01-01'
                |    AND l_shipdate < date'1994-01-01' + interval 1 year
                |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
                |    AND l_quantity < 24;
                |""".stripMargin)
        .show(200, truncate = false) // .explain("extended")
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute $i time, time: $tookTime")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    // spark.conf.set("spark.gluten.enabled", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
    df.summary().show(100, truncate = false)
  }

  def selectLocationClickHouseTable(spark: SparkSession, executedCnt: Int, sql: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark
        .sql(s"""
                |SELECT
                |    sum(l_extendedprice * l_discount) AS revenue
                |FROM
                |    ch_clickhouse
                |WHERE
                |    l_shipdate >= date'1994-01-01'
                |    AND l_shipdate < date'1994-01-01' + interval 1 year
                |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
                |    AND l_quantity < 24;
                |""".stripMargin)
        .show(200, truncate = false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute $i time, time: $tookTime")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    // spark.conf.set("spark.gluten.enabled", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
    df.summary().show(100, truncate = false)
  }

  def selectQ1ClickHouseTable(
      spark: SparkSession,
      executedCnt: Int,
      sql: String,
      targetTable: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark
        .sql(s"""
                |SELECT
                |    l_returnflag,
                |    l_linestatus,
                |    sum(l_quantity) AS sum_qty,
                |    sum(l_extendedprice) AS sum_base_price,
                |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                |    avg(l_quantity) AS avg_qty,
                |    avg(l_extendedprice) AS avg_price,
                |    avg(l_discount) AS avg_disc,
                |    count(*) AS count_order
                |FROM
                |    ch_lineitem
                |WHERE
                |    l_shipdate <= date'1998-09-02' - interval 1 day
                |GROUP BY
                |    l_returnflag,
                |    l_linestatus;
                |-- ORDER BY
                |--     l_returnflag,
                |--     l_linestatus;
                |""".stripMargin)
        .show(200, truncate = false) // .explain("extended")
      // can not use .collect(), will lead to error.
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute $i time, time: $tookTime")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    // spark.conf.set("spark.gluten.enabled", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
    df.summary().show(100, truncate = false)
  }

  def selectStarClickHouseTable(spark: SparkSession): Unit = {
    spark
      .sql("""
             | SELECT
             |    l_returnflag,
             |    l_linestatus,
             |    l_quantity,
             |    l_extendedprice,
             |    l_discount,
             |    l_tax
             | FROM lineitem
             |""".stripMargin)
      .show(20, truncate = false) // .explain("extended")
    spark
      .sql("""
             | SELECT * FROM lineitem
             |""".stripMargin)
      .show(20, truncate = false) // .explain("extended")
    // can not use .collect(), will lead to error.
  }

  def selectQ1LocationClickHouseTable(spark: SparkSession, executedCnt: Int, sql: String): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark
        .sql(s"""
                |SELECT
                |    l_returnflag,
                |    l_linestatus,
                |    sum(l_quantity) AS sum_qty,
                |    sum(l_extendedprice) AS sum_base_price,
                |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                |    avg(l_quantity) AS avg_qty,
                |    avg(l_extendedprice) AS avg_price,
                |    avg(l_discount) AS avg_disc,
                |    count(*) AS count_order
                |FROM
                |    ch_clickhouse
                |WHERE
                |    l_shipdate <= date'1998-09-02' - interval 1 day
                |GROUP BY
                |    l_returnflag,
                |    l_linestatus
                |ORDER BY
                |    l_returnflag,
                |    l_linestatus;
                |""".stripMargin)
        .show(200, truncate = false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute $i time, time: $tookTime")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    // spark.conf.set("spark.gluten.enabled", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
    df.summary().show(100, truncate = false)
  }

  def testSQL(
      spark: SparkSession,
      parquetFilesPath: String,
      fileFormat: String,
      executedCnt: Int,
      sql: String): Unit = {
    /* spark.sql(
      s"""
         | show tables
         |""".stripMargin).show(100, false) */

    val tookTimeArr = ArrayBuffer[Long]()
    for (i <- 1 to executedCnt) {
      val startTime = System.nanoTime()
      spark.sql(sql).show(200, truncate = false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute $i time, time: $tookTime")
      tookTimeArr += tookTime
    }

    println(tookTimeArr.mkString(","))

    // spark.conf.set("spark.gluten.enabled", "false")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(tookTimeArr, 1).toDF("time")
    df.summary().show(100, truncate = false)
  }

  def testSparkTPCH(spark: SparkSession): Unit = {
    val tookTimeArr = ArrayBuffer[Long]()
    val rootPath = this.getClass.getResource("/").getPath
    val resourcePath = rootPath + "../../../../gluten-core/src/test/resources/"
    val queryPath = resourcePath + "/queries/"
    for (i <- 1 to 22) {
      val startTime = System.nanoTime()
      val sqlFile = queryPath + "q" + "%02d".format(i) + ".sql"
      println(sqlFile)
      val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8")
      // spark.sql(sqlStr.mkString).collect()
      val sql = sqlStr.mkString
      spark.sql(sql).explain(false)
      spark.sql(sql).show(10, truncate = false)
      val tookTime = (System.nanoTime() - startTime) / 1000000
      println(s"Execute $i time, time: $tookTime")
      tookTimeArr += tookTime
    }
    println(tookTimeArr.mkString(","))
  }

  def createTables(spark: SparkSession, parquetFilesPath: String, fileFormat: String): Unit = {
    /* val dataSourceMap = Map(
      "customer" -> spark.read.format(fileFormat).load(parquetFilesPath + "/customer"),

      "lineitem" -> spark.read.format(fileFormat).load(parquetFilesPath + "/lineitem"),

      "nation" -> spark.read.format(fileFormat).load(parquetFilesPath + "/nation"),

      "region" -> spark.read.format(fileFormat).load(parquetFilesPath + "/region"),

      "orders" -> spark.read.format(fileFormat).load(parquetFilesPath + "/order"),

      "part" -> spark.read.format(fileFormat).load(parquetFilesPath + "/part"),

      "partsupp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/partsupp"),

      "supplier" -> spark.read.format(fileFormat).load(parquetFilesPath + "/supplier")) */

    val parquetFilePath = "/data1/test_output/tpch-data-sf100"
    val customerData = parquetFilePath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer100")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS customer100 (
              | c_custkey    bigint,
              | c_name       string,
              | c_address    string,
              | c_nationkey  bigint,
              | c_phone      string,
              | c_acctbal    double,
              | c_mktsegment string,
              | c_comment    string)
              | STORED AS PARQUET LOCATION '$customerData'
              |""".stripMargin)
      .show(1, truncate = false)

    val lineitemData = parquetFilePath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem100")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS lineitem100 (
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
              | STORED AS PARQUET LOCATION '$lineitemData'
              |""".stripMargin)
      .show(1, truncate = false)

    val nationData = parquetFilePath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation100")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS nation100 (
              | n_nationkey bigint,
              | n_name      string,
              | n_regionkey bigint,
              | n_comment   string)
              | STORED AS PARQUET LOCATION '$nationData'
              |""".stripMargin)
      .show(1, truncate = false)

    val regionData = parquetFilePath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region100")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS region100 (
              | r_regionkey bigint,
              | r_name      string,
              | r_comment   string)
              | STORED AS PARQUET LOCATION '$regionData'
              |""".stripMargin)
      .show(1, truncate = false)

    val ordersData = parquetFilePath + "/order"
    spark.sql(s"DROP TABLE IF EXISTS orders100")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS orders100 (
              | o_orderkey      bigint,
              | o_custkey       bigint,
              | o_orderstatus   string,
              | o_totalprice    double,
              | o_orderdate     date,
              | o_orderpriority string,
              | o_clerk         string,
              | o_shippriority  bigint,
              | o_comment       string)
              | STORED AS PARQUET LOCATION '$ordersData'
              |""".stripMargin)
      .show(1, truncate = false)

    val partData = parquetFilePath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part100")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS part100 (
              | p_partkey     bigint,
              | p_name        string,
              | p_mfgr        string,
              | p_brand       string,
              | p_type        string,
              | p_size        bigint,
              | p_container   string,
              | p_retailprice double,
              | p_comment     string)
              | STORED AS PARQUET LOCATION '$partData'
              |""".stripMargin)
      .show(1, truncate = false)

    val partsuppData = parquetFilePath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp100")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS partsupp100 (
              | ps_partkey    bigint,
              | ps_suppkey    bigint,
              | ps_availqty   bigint,
              | ps_supplycost double,
              | ps_comment    string)
              | STORED AS PARQUET LOCATION '$partsuppData'
              |""".stripMargin)
      .show(1, truncate = false)

    val supplierData = parquetFilePath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier100")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS supplier100 (
              | s_suppkey   bigint,
              | s_name      string,
              | s_address   string,
              | s_nationkey bigint,
              | s_phone     string,
              | s_acctbal   double,
              | s_comment   string)
              | STORED AS PARQUET LOCATION '$supplierData'
              |""".stripMargin)
      .show(1, truncate = false)

    spark
      .sql(s"""
              | show databases;
              |""".stripMargin)
      .show(100, truncate = false)
    spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .show(100, truncate = false)
    /* dataSourceMap.foreach {
      case (key, value) =>
        println(s"----------------create table $key")
        spark.sql(
          s"""
             | desc $key;
             |""".stripMargin).show(100, false)
        spark.sql(
          s"""
             | select count(1) from $key;
             |""".stripMargin).show(10, false)

    } */
  }

  def createClickHouseTables(
      spark: SparkSession,
      dataFilesPath: String,
      dbName: String,
      notNull: Boolean = false,
      tablePrefix: String = "ch_",
      tableSuffix: String = "100"): Unit = {
    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS $dbName
                 |WITH DBPROPERTIES (engine='MergeTree');
                 |""".stripMargin)

    spark.sql(s"use $dbName;")

    val notNullStr = if (notNull) {
      " not null"
    } else {
      " "
    }

    val customerData = dataFilesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}customer$tableSuffix")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}customer$tableSuffix (
              | c_custkey    bigint $notNullStr,
              | c_name       string $notNullStr,
              | c_address    string $notNullStr,
              | c_nationkey  bigint $notNullStr,
              | c_phone      string $notNullStr,
              | c_acctbal    double $notNullStr,
              | c_mktsegment string $notNullStr,
              | c_comment    string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION '$customerData'
              |""".stripMargin)
      .show(1, truncate = false)

    val lineitemData = dataFilesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}lineitem$tableSuffix")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}lineitem$tableSuffix (
              | l_orderkey      bigint $notNullStr,
              | l_partkey       bigint $notNullStr,
              | l_suppkey       bigint $notNullStr,
              | l_linenumber    bigint $notNullStr,
              | l_quantity      double $notNullStr,
              | l_extendedprice double $notNullStr,
              | l_discount      double $notNullStr,
              | l_tax           double $notNullStr,
              | l_returnflag    string $notNullStr,
              | l_linestatus    string $notNullStr,
              | l_shipdate      date $notNullStr,
              | l_commitdate    date $notNullStr,
              | l_receiptdate   date $notNullStr,
              | l_shipinstruct  string $notNullStr,
              | l_shipmode      string $notNullStr,
              | l_comment       string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION '$lineitemData'
              |""".stripMargin)
      .show(1, truncate = false)

    /* val lineitemLCData = dataFilesPath + "/lineitem_lowcardinality"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}lineitem${tableSuffix}_lc")
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}lineitem${tableSuffix}_lc (
         | l_orderkey      bigint ${notNullStr},
         | l_partkey       bigint ${notNullStr},
         | l_suppkey       bigint ${notNullStr},
         | l_linenumber    bigint ${notNullStr},
         | l_quantity      double ${notNullStr},
         | l_extendedprice double ${notNullStr},
         | l_discount      double ${notNullStr},
         | l_tax           double ${notNullStr},
         | l_returnflag    string ${notNullStr},
         | l_linestatus    string ${notNullStr},
         | l_shipdate      date ${notNullStr},
         | l_commitdate    date ${notNullStr},
         | l_receiptdate   date ${notNullStr},
         | l_shipinstruct  string ${notNullStr},
         | l_shipmode      string ${notNullStr},
         | l_comment       string ${notNullStr})
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION '${lineitemLCData}'
         |""".stripMargin).show(1, false) */

    val nationData = dataFilesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}nation$tableSuffix")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}nation$tableSuffix (
              | n_nationkey bigint $notNullStr,
              | n_name      string $notNullStr,
              | n_regionkey bigint $notNullStr,
              | n_comment   string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION '$nationData'
              |""".stripMargin)
      .show(1, truncate = false)

    val regionData = dataFilesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}region$tableSuffix")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}region$tableSuffix (
              | r_regionkey bigint $notNullStr,
              | r_name      string $notNullStr,
              | r_comment   string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION '$regionData'
              |""".stripMargin)
      .show(1, truncate = false)

    val ordersData = dataFilesPath + "/order"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}orders$tableSuffix")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}orders$tableSuffix (
              | o_orderkey      bigint $notNullStr,
              | o_custkey       bigint $notNullStr,
              | o_orderstatus   string $notNullStr,
              | o_totalprice    double $notNullStr,
              | o_orderdate     date $notNullStr,
              | o_orderpriority string $notNullStr,
              | o_clerk         string $notNullStr,
              | o_shippriority  bigint $notNullStr,
              | o_comment       string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION '$ordersData'
              |""".stripMargin)
      .show(1, truncate = false)

    val partData = dataFilesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}part$tableSuffix")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}part$tableSuffix (
              | p_partkey     bigint $notNullStr,
              | p_name        string $notNullStr,
              | p_mfgr        string $notNullStr,
              | p_brand       string $notNullStr,
              | p_type        string $notNullStr,
              | p_size        bigint $notNullStr,
              | p_container   string $notNullStr,
              | p_retailprice double $notNullStr,
              | p_comment     string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION '$partData'
              |""".stripMargin)
      .show(1, truncate = false)

    val partsuppData = dataFilesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}partsupp$tableSuffix")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}partsupp$tableSuffix (
              | ps_partkey    bigint $notNullStr,
              | ps_suppkey    bigint $notNullStr,
              | ps_availqty   bigint $notNullStr,
              | ps_supplycost double $notNullStr,
              | ps_comment    string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION '$partsuppData'
              |""".stripMargin)
      .show(1, truncate = false)

    val supplierData = dataFilesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS ${tablePrefix}supplier$tableSuffix")
    spark
      .sql(s"""
              | CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}supplier$tableSuffix (
              | s_suppkey   bigint $notNullStr,
              | s_name      string $notNullStr,
              | s_address   string $notNullStr,
              | s_nationkey bigint $notNullStr,
              | s_phone     string $notNullStr,
              | s_acctbal   double $notNullStr,
              | s_comment   string $notNullStr)
              | USING clickhouse
              | TBLPROPERTIES (engine='MergeTree'
              |                )
              | LOCATION '$supplierData'
              |""".stripMargin)
      .show(1, truncate = false)

    spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .show(100, truncate = false)
  }

  def createTempView(spark: SparkSession, parquetFilesPath: String, fileFormat: String): Unit = {
    val dataSourceMap = Map(
      "customer_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/customer"),
      "lineitem_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/lineitem"),
      "nation_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/nation"),
      "region_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/region"),
      "orders_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/order"),
      "part_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/part"),
      "partsupp_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/partsupp"),
      "supplier_tmp" -> spark.read.format(fileFormat).load(parquetFilesPath + "/supplier")
    )

    dataSourceMap.foreach { case (key, value) => value.createOrReplaceTempView(key) }
  }

  def createGlobalTempView(spark: SparkSession): Unit = {
    spark.sql(s"""DROP VIEW IF EXISTS global_temp.view_lineitem;""")
    spark.sql(s"""
                 |CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_lineitem
                 |AS SELECT * FROM lineitem;
                 |
                 |""".stripMargin)
    spark.sql(s"""DROP VIEW IF EXISTS global_temp.view_orders;""")
    spark.sql(s"""
                 |CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_orders
                 |AS SELECT * FROM orders;
                 |""".stripMargin)
    spark
      .sql(s"""
              |SHOW VIEWS IN global_temp;
              |""".stripMargin)
      .show(100, truncate = false)
    spark
      .sql(s"""
              |SHOW VIEWS;
              |""".stripMargin)
      .show(100, truncate = false)
  }
}
// scalastyle:on
