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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._
import org.apache.spark.sql.udaf._

import org.apache.commons.lang3.time.StopWatch
import org.roaringbitmap.longlong.Roaring64NavigableMap

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
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.files.maxPartitionBytes", "32m")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.execution.useObjectHashAggregateExec", "true")
      .set(
        "spark.gluten.sql.columnar.extended.expressions.transformer",
        "org.apache.spark.sql.udaf.CustomerExpressionTransformer")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (preciseCardinalityExpr, preciseCardinalityBuilder) =
      FunctionRegistryBase.build[PreciseCardinality]("bitmap_cardinality", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("bitmap_cardinality"),
      preciseCardinalityExpr,
      preciseCardinalityBuilder
    )

    val (preciseCountDistinctDecodeExpr, preciseCountDistinctDecodeBuilder) =
      FunctionRegistryBase
        .build[PreciseCountDistinctDecode]("precise_count_distinct_decode", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("precise_count_distinct_decode"),
      preciseCountDistinctDecodeExpr,
      preciseCountDistinctDecodeBuilder
    )

    val (bitmapOrExpr, bitmapOrBuilder) =
      FunctionRegistryBase.build[ReusePreciseCountDistinct]("bitmap_or", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("bitmap_or"),
      bitmapOrExpr,
      bitmapOrBuilder
    )

    val (bitmapIdsExpr, bitmapIdsBuilder) =
      FunctionRegistryBase.build[PreciseCountDistinctAndArray]("bitmap_and_ids", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("bitmap_and_ids"),
      bitmapIdsExpr,
      bitmapIdsBuilder
    )

    val (bitmapValuesExpr, bitmapValuesBuilder) =
      FunctionRegistryBase.build[PreciseCountDistinctAndValue]("bitmap_and_value", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("bitmap_and_value"),
      bitmapValuesExpr,
      bitmapValuesBuilder
    )

    val (preciseCountDistinctExpr, preciseCountDistinctBuilder) =
      FunctionRegistryBase.build[PreciseCountDistinct]("ke_bitmap_or_cardinality", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("ke_bitmap_or_cardinality"),
      preciseCountDistinctExpr,
      preciseCountDistinctBuilder
    )
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createTPCHParquetTables(tablesPath)
  }

  test("test ke bitmap cardinality") {
    RoaringBitmapJniTest.getRoaringBitmapCardinality(
      BitmapSerAndDeSer.getBitmapArray(1, 3, 5, 7, 8, 9, 1000000000, 9999999999999999L,
        8999999999999999L, 8999999999999999L, 7999999999999999L, 6999999999999999L))
    val values = (1L to 9999999L).filter(_ % 3 == 0).toArray
    RoaringBitmapJniTest.getRoaringBitmapCardinality(BitmapSerAndDeSer.getBitmapArray(values: _*))
    RoaringBitmapJniTest.getRoaringBitmapCardinality(
      BitmapSerAndDeSer.getBitmapArray(234, 56, 234, 1000, 56))
    RoaringBitmapJniTest.getRoaringBitmapCardinality(BitmapSerAndDeSer.getBitmapArray(1, 2))
    RoaringBitmapJniTest.getRoaringBitmapCardinality(
      BitmapSerAndDeSer.getBitmapArray(2, 2, 2, 2, 2))

    val expected =
      Seq(Row("a", 2), Row("b", 11), Row("c", 3), Row("d", 1), Row("e", 3333333), Row("f", null))
    val array1: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(1, 2)
    val array2: Array[Byte] = BitmapSerAndDeSer
      .getBitmapArray(1, 3, 5, 7, 8, 9, 1000000000, 9999999999999999L, 8999999999999999L,
        8999999999999999L, 7999999999999999L, 6999999999999999L)
    val array3: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(234, 56, 234, 1000, 56)
    val array4: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(2, 2, 2, 2, 2)
    val array5: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values: _*)

    // import testImplicits._
    val schema = StructType(Seq(StructField("col1", StringType), StructField("col2", BinaryType)))
    val data = Seq(
      Row("a": String, array1),
      Row("b": String, array2),
      Row("c": String, array3),
      Row("d": String, array4),
      Row("e": String, array5),
      Row("f": String, null))

    val df0 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df0.createOrReplaceTempView("t")
    val SQL = "select col1, bitmap_cardinality(col2) from t"

    val exec = spark.sql(SQL)
    checkAnswer(exec, expected)
    assert(BitmapSerAndDeSer.deserialize(array3).getLongCardinality == 3)
  }

  ignore("test ke index file") {
    val SQL1 =
      """
        |select `0`, `100000`, bitmap_cardinality(`100001`), `100002`
        |from parquet.`/data1/ke-bitmap-data.snappy.parquet`
        |""".stripMargin

    compareResultsAgainstVanillaSpark(
      SQL1,
      true,
      df => {
        df.show(1000, false)
        df.explain(false)
      },
      true
    )

    val SQL2 =
      """
        |select `100000`, bitmap_cardinality(bitmap_or(`100001`)), max(`100002`)
        |from parquet.`/data1/ke-bitmap-data.snappy.parquet`
        |group by `100000`
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL2,
      true,
      df => {
        df.show(1000, false)
        df.explain(false)
      },
      true
    )

    val SQL2_1 =
      """
        |select `100000`, ke_bitmap_or_cardinality(`100001`), max(`100002`)
        |from parquet.`/data1/ke-bitmap-data.snappy.parquet`
        |group by `100000`
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL2_1,
      true,
      df => {
        df.show(1000, false)
        df.explain(false)
      },
      true
    )

    val SQL3 =
      """
        |select `100000`, bitmap_and_value(`100001`), max(`100002`)
        |from parquet.`/data1/ke-bitmap-data.snappy.parquet`
        |group by `100000`
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL3,
      true,
      df => {
        df.show(1000, false)
        df.explain(false)
      },
      true
    )
  }

  test("test ke bitmap functions") {
    RoaringBitmapJniTest.testCRoaringBitmap()
  }

  test("test ke bitmap functions vanilla spark") {

    val size = 6000;
    val bitmapArray = new Array[Roaring64NavigableMap](size)

    for (i <- 0 until size) {
      bitmapArray(i) = new Roaring64NavigableMap()
      // val s = i * 100
      val s = 0
      val e = (i + 1) * 100
      for (j <- s until e) {
        if ((j % 3) == 0) {
          bitmapArray(i).add((j / 3).toLong)
        }
      }
    }

    val result = new Roaring64NavigableMap()
    val w = StopWatch.createStarted()
    for (j <- 0 until 1000) {
      for (i <- 0 until size) {
        result.naivelazyor(bitmapArray(i))
      }
    }
    result.repairAfterLazy()
    // scalastyle:off println
    println(s"vanilla spark or time: ${w.getNanoTime} cardinality: ${result.getLongCardinality}")
    // scalastyle:on
    w.stop()
  }

  ignore("test ke bitmap index files") {
    /* import org.apache.spark.sql.functions._
    import org.apache.spark.sql.SaveMode

    val SQL =
      """
        |select `71`, `49`, `61`, `100000`, `100010`
        |from parquet.`/data1/ke-index-data-bitmap/q21-470001`
        |""".stripMargin
    withSQLConf(vanillaSparkConfs(): _*) {
      val df = spark.sql(SQL)
      df.printSchema()

      val data = (0 until 2000).map(d => {
        val r = scala.util.Random.nextInt(300).toLong
        val values = (1L to r).filter(_ % 3 == 0).toArray
        BitmapSerAndDeSer.getBitmapArray(values: _*)
      }).toArray

      println(s"start to write data")
      val col_udf = udf(() => {
        data(scala.util.Random.nextInt(2000))
      })
      df.withColumn("100020", col_udf())
        .write.mode(SaveMode.Overwrite).parquet("/data1/ke-index-data-bitmap/q21-470001-tt")
    } */

    // 470001
    val SQL1 =
      """
        |select `49`, ke_bitmap_or_cardinality(`100010`)
        |from parquet.`/data1/ke-index-data-bitmap/q21-470001`
        |where `71` = 'F'
        |group by `49`
        |""".stripMargin

    for (i <- 0 until 2) {
      withSQLConf(vanillaSparkConfs(): _*) {
        spark.sql(SQL1).collect()
      }

      val df = spark.sql(SQL1)
      df.explain(false)
      df.collect()
    }

    /*
    // 80001
    val SQL1 =
      """
        |select `4`, `12`, `11`, ke_bitmap_or_cardinality(`100003`)
        |from parquet.`/data1/ke-index-data-bitmap/q16-80001`
        |where `4` <> 'Brand#45'
        |    AND `12` NOT LIKE 'ECONOMY BRUSHED%'
        |    AND `11` IN (22, 14, 27, 49, 21, 33, 35, 28)
        |    AND `18` LIKE '%Customer%Complaints%'
        |group by `4`, `12`, `11`
        |""".stripMargin

    for (i <- 0 until 2) {
      withSQLConf(vanillaSparkConfs(): _*) {
        spark.sql(SQL1).collect()
      }

      val df = spark.sql(SQL1)
      df.explain(false)
      df.collect()
    } */
  }

  test("test ke functions") {
    val values = (1L to 9999999L).filter(_ % 3 == 0).toArray
    val array1: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(1, 2)
    val array2: Array[Byte] = BitmapSerAndDeSer
      .getBitmapArray(1, 3, 5, 7, 8, 9, 1000000000, 9999999999999999L, 8999999999999999L,
        8999999999999999L, 7999999999999999L, 6999999999999999L)
    val array3: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(234, 56, 234, 1000, 56)
    val array4: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(2, 2, 2, 2, 2)
    val array5: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values: _*)
    val array6: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(Array.empty[Long]: _*)

    // import testImplicits._
    val schema =
      StructType(
        Seq(
          StructField("col1", StringType, true),
          StructField("col2", IntegerType, true),
          StructField("col3", BinaryType, true)))
    val data = Seq(
      Row("a": String, 1: Int, array1),
      Row("b": String, 2: Int, array2),
      Row("a": String, 3: Int, array3),
      Row("b": String, 4: Int, array4),
      Row("c": String, null, array5),
      Row("c": String, 5: Int, null),
      Row("e": String, 6: Int, array6)
    )

    val df0 = spark.createDataFrame(spark.sparkContext.parallelize(data, 2), schema)

    df0.createOrReplaceTempView("t")

    val SQL1 = "select col1, bitmap_cardinality(col3) from t"

    compareResultsAgainstVanillaSpark(
      SQL1,
      true,
      df => {},
      false
    )

    val SQL2 = "select col1, precise_count_distinct_decode(col3) from t"

    compareResultsAgainstVanillaSpark(
      SQL2,
      true,
      df => {},
      false
    )
  }

  test("test bitmap_or") {
    val values = (1L to 9999L).filter(_ % 3 == 0).toArray
    val array1: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(1, 2)
    val array2: Array[Byte] = BitmapSerAndDeSer
      .getBitmapArray(1, 3, 5, 7, 8, 9, 1000000000, 9999999999999999L, 8999999999999999L,
        8999999999999999L, 7999999999999999L, 6999999999999999L)
    val array3: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(234, 56, 234, 1000, 56)
    val array4: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(2, 2, 2, 2, 2)
    val array5: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values: _*)
    val array6: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(Array.empty[Long]: _*)

    import testImplicits._
    val df0 = Seq(
      ("a": String, array1),
      ("b": String, array2),
      ("a": String, array2),
      ("b": String, array5),
      ("c": String, array2),
      ("c": String, array5),
      ("a": String, array4),
      ("d": String, array1),
      ("e": String, array2),
      ("d": String, array2),
      ("e": String, array5),
      ("f": String, array5),
      ("f": String, array5),
      ("g": String, array4)
    )
      .toDF("col1", "col2")
      .coalesce(2)

    df0.createOrReplaceTempView("t")

    val SQL1 = "select col1, ke_bitmap_or_cardinality(col2) as col2 from t group by col1"

    compareResultsAgainstVanillaSpark(
      SQL1,
      true,
      df => {},
      true
    )
  }

  test("test bitmap_and_value") {
    val values = (1L to 9999999L).filter(_ % 3 == 0).toArray
    val array1: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(1, 2)
    val array2: Array[Byte] = BitmapSerAndDeSer
      .getBitmapArray(1, 3, 5, 7, 8, 9, 1000000000, 9999999999999999L, 8999999999999999L,
        8999999999999999L, 7999999999999999L, 6999999999999999L)
    val array3: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(234, 56, 234, 1000, 56)
    val array4: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(1, 1, 2, 2, 2)
    val array5: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values: _*)
    val array6: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(Array.empty[Long]: _*)

    import testImplicits._
    val df0 = Seq(
      ("a": String, array1),
      ("b": String, array6),
      ("a": String, array2),
      ("b": String, array3),
      ("b": String, array5),
      ("c": String, array3),
      ("c": String, array2),
      ("d": String, array2),
      ("d": String, array5),
      ("a": String, null)
    ).toDF("col1", "col2").coalesce(2)

    df0.createOrReplaceTempView("t")

    val SQL1 = "select col1, bitmap_and_value(col2) as col2 from t group by col1"
    compareResultsAgainstVanillaSpark(
      SQL1,
      true,
      df => {},
      true
    )
  }

  test("test bitmap_and_ids") {
    val values = (1L to 100L).filter(_ % 3 == 0).toArray
    val array1: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values: _*)
    val array2: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(
      1L,
      2L,
      3L,
      234556L,
      234556234556L,
      1000000000L,
      (Int.MaxValue * 2L) + 1L,
      (Int.MaxValue * 2L) + 2L,
      -((Int.MaxValue * 2L) + 1L),
      -1000000000L,
      9999999999999999L,
      8999999999999999L,
      1000000000L,
      9999999999999999L,
      8999999999999999L,
      8999999999999999L,
      7999999999999999L,
      6999999999999999L,
      -8999999999999999L,
      -8999999999999999L,
      -7999999999999999L,
      -6999999999999999L
    )
    val array3: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(2, 3, 234, 56, 234, 1000, 56)
    val array4: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(1, 1, 2, 2, 2, 3, 6)
    val array5: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(Array.empty[Long]: _*)

    import testImplicits._
    val df0 = Seq(
      ("a": String, array2),
      ("b": String, array2),
      ("a": String, array3),
      ("b": String, array4),
      ("c": String, array2),
      ("c": String, array3),
      ("b": String, array1)).toDF("col1", "col2").coalesce(2)
    df0.createOrReplaceTempView("t")
    val SQL1 = "select col1, bitmap_and_ids(col2) as ids from t group by col1"

    compareResultsAgainstVanillaSpark(
      SQL1,
      true,
      df => {},
      true
    )
  }

  test("test explode and array") {
    val values1 = Array(1L, 2L)
    val values2 = Array(1L, 3L, 5L, 7L, 8L, 9L, 1000000000L, 9999999999999999L, 8999999999999999L,
      8999999999999999L, 7999999999999999L, 6999999999999999L)
    val values3 = Array(234L, 56L, 234L, 1000L, 56L)
    val values4 = Array(2L, 2L, 2L, 2L, 2L)
    val values5 = (1L to 99L).filter(_ % 3 == 0).toArray
    val values6 = Array.empty[Long]
    val array1: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values1: _*)
    val array2: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values2: _*)
    val array3: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values3: _*)
    val array4: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values4: _*)
    val array5: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values5: _*)
    val array6: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values6: _*)

    // import testImplicits._
    val schema =
      StructType(
        Seq(
          StructField("col1", StringType, true),
          StructField("col2", ArrayType(LongType), true),
          StructField("col3", BinaryType, true)))
    val data = Seq(
      Row("a": String, values1, array1),
      Row("b": String, values2, array2),
      Row("a": String, values3, array3),
      Row("b": String, values4, array4),
      Row("c": String, values5, array5),
      Row("e": String, values6, array6)
    )

    val df0 = spark.createDataFrame(spark.sparkContext.parallelize(data, 2), schema)

    df0.createOrReplaceTempView("t")

    val SQL1 = "select col1, col2 from t"
    val SQL2 = "select explode(ids) from (select col2 as ids from t)"

    compareResultsAgainstVanillaSpark(
      SQL1,
      true,
      df => {},
      false
    )

    compareResultsAgainstVanillaSpark(
      SQL2,
      true,
      df => {},
      false
    )
  }

  ignore("ke bitmap cardinality benchmark") {
    val values1 = (1L to 999999L).filter(_ % 3 == 0).toArray
    val values2 = (1L to 9999999L).filter(_ % 3 == 0).toArray
    val values3 = (1L to 99999999L).filter(_ % 3 == 0).toArray
    val values4 = (1L to 999999999L).filter(_ % 3 == 0).toArray
    val values5 = (1L to 99999L).filter(_ % 3 == 0).toArray
    val array1: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(1, 2)
    val array2: Array[Byte] = BitmapSerAndDeSer
      .getBitmapArray(1, 3, 5, 7, 8, 9, 1000000000, 9999999999999999L, 8999999999999999L,
        8999999999999999L, 7999999999999999L, 6999999999999999L)
    val array3: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(234, 56, 234, 1000, 56)
    val array4: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(2, 2, 2, 2, 2)
    val array5: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values1: _*)
    val array6: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values2: _*)
    val array7: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values3: _*)
    val array8: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values4: _*)
    val array9: Array[Byte] = BitmapSerAndDeSer.getBitmapArray(values5: _*)

    val execCnt = 50
    def vanillaRoaringBitmap1(): Unit = {
      BitmapSerAndDeSer.deserialize(array1).getLongCardinality
      BitmapSerAndDeSer.deserialize(array2).getLongCardinality
      BitmapSerAndDeSer.deserialize(array3).getLongCardinality
      BitmapSerAndDeSer.deserialize(array4).getLongCardinality
      BitmapSerAndDeSer.deserialize(array5).getLongCardinality
    }

    def vanillaRoaringBitmap2(): Unit = {
      BitmapSerAndDeSer.deserialize(array1).getLongCardinality
      BitmapSerAndDeSer.deserialize(array2).getLongCardinality
      BitmapSerAndDeSer.deserialize(array3).getLongCardinality
      BitmapSerAndDeSer.deserialize(array4).getLongCardinality
      BitmapSerAndDeSer.deserialize(array6).getLongCardinality
    }

    def vanillaRoaringBitmap3(): Unit = {
      BitmapSerAndDeSer.deserialize(array1).getLongCardinality
      BitmapSerAndDeSer.deserialize(array2).getLongCardinality
      BitmapSerAndDeSer.deserialize(array3).getLongCardinality
      BitmapSerAndDeSer.deserialize(array4).getLongCardinality
      BitmapSerAndDeSer.deserialize(array7).getLongCardinality
    }

    def vanillaRoaringBitmap4(): Unit = {
      BitmapSerAndDeSer.deserialize(array1).getLongCardinality
      BitmapSerAndDeSer.deserialize(array2).getLongCardinality
      BitmapSerAndDeSer.deserialize(array3).getLongCardinality
      BitmapSerAndDeSer.deserialize(array4).getLongCardinality
      BitmapSerAndDeSer.deserialize(array8).getLongCardinality
    }

    def vanillaRoaringBitmap5(): Unit = {
      BitmapSerAndDeSer.deserialize(array1).getLongCardinality
      BitmapSerAndDeSer.deserialize(array2).getLongCardinality
      BitmapSerAndDeSer.deserialize(array3).getLongCardinality
      BitmapSerAndDeSer.deserialize(array4).getLongCardinality
      BitmapSerAndDeSer.deserialize(array9).getLongCardinality
    }

    def nativeRoaringBitmap1(): Unit = {
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array1)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array2)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array3)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array4)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array5)
    }

    def nativeRoaringBitmap2(): Unit = {
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array1)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array2)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array3)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array4)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array6)
    }

    def nativeRoaringBitmap3(): Unit = {
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array1)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array2)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array3)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array4)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array7)
    }

    def nativeRoaringBitmap4(): Unit = {
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array1)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array2)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array3)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array4)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array8)
    }

    def nativeRoaringBitmap5(): Unit = {
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array1)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array2)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array3)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array4)
      RoaringBitmapJniTest.getRoaringBitmapCardinality(array9)
    }

    def exec(name: String)(func: () => Unit): Unit = {
      val startTime = System.nanoTime()
      for (i <- 1 to execCnt) {
        func()
      }
      // scalastyle:off println
      println(s"$name exec time: ${(System.nanoTime() - startTime) / 1000000}")
      // scalastyle:on println
    }
    exec("vanilla 333333")(vanillaRoaringBitmap1)
    exec("gluten 333333")(nativeRoaringBitmap1)

    exec("vanilla 3333333")(vanillaRoaringBitmap2)
    exec("gluten 3333333")(nativeRoaringBitmap2)

    exec("vanilla 33333333")(vanillaRoaringBitmap3)
    exec("gluten 33333333")(nativeRoaringBitmap3)

    exec("vanilla 333333333")(vanillaRoaringBitmap4)
    exec("gluten 333333333")(nativeRoaringBitmap4)

    exec("vanilla 33333")(vanillaRoaringBitmap5)
    exec("gluten 33333")(nativeRoaringBitmap5)
  }
}
