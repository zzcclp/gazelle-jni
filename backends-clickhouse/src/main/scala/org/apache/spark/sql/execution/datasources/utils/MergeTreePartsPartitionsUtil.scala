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
package org.apache.spark.sql.execution.datasources.utils

import io.glutenproject.backendsapi.clickhouse.CHBackendSettings
import io.glutenproject.execution.{GlutenMergeTreePartition, MergeTreePartitionedFile}
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.utils.SubstraitPlanPrinterUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SparkResourceUtil
import org.apache.spark.util.collection.BitSet

import io.substrait.proto.{NamedStruct, Type}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

// scalastyle:off argcount
object MergeTreePartsPartitionsUtil extends Logging {

  val DEFAULT_ORDER_BY_KEY = "tuple()"

  def genOrderByAndPrimaryKeyStr(
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]]): (String, String) = {
    val orderByKey =
      if (orderByKeyOption.isDefined && orderByKeyOption.get.nonEmpty) {
        orderByKeyOption.get.mkString(",")
      } else DEFAULT_ORDER_BY_KEY

    val primaryKey =
      if (
        !orderByKey.equals(DEFAULT_ORDER_BY_KEY) && primaryKeyOption.isDefined &&
        primaryKeyOption.get.nonEmpty
      ) {
        primaryKeyOption.get.mkString(",")
      } else ""

    (orderByKey, primaryKey)
  }

  def genNamedStructJson(tableSchema: StructType): String = {
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(tableSchema)
    val nameList = tableSchema.fieldNames

    val structBuilder = Type.Struct.newBuilder
    for (typeNode <- typeNodes.asScala) {
      structBuilder.addTypes(typeNode.toProtobuf)
    }

    val nStructBuilder = NamedStruct.newBuilder
    nStructBuilder.setStruct(structBuilder.build)
    for (name <- nameList) {
      nStructBuilder.addNames(name)
    }

    val namedStructJson = SubstraitPlanPrinterUtil.substraitNamedStructToJson(
      nStructBuilder
        .build())
    namedStructJson.replaceAll("\\\n", "").replaceAll(" ", "")
  }

  def getPartsPartitions(
      sparkSession: SparkSession,
      table: ClickHouseTableV2): Seq[InputPartition] = {
    val partsFiles = table.listFiles()

    val partitions = new ArrayBuffer[InputPartition]
    val (database, tableName) = if (table.catalogTable.isDefined) {
      (table.catalogTable.get.identifier.database.get, table.catalogTable.get.identifier.table)
    } else {
      // for file_format.`file_path`
      ("default", "file_format")
    }
    val engine = table.snapshot.metadata.configuration.get("engine").get
    // TODO: remove `substring`
    val tablePath = table.deltaLog.dataPath.toString.substring(6)
    var currentMinPartsNum = -1L
    var currentMaxPartsNum = -1L
    var currentSize = 0L
    var currentFileCnt = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentMinPartsNum > 0L && currentMaxPartsNum >= currentMinPartsNum) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          tablePath,
          DEFAULT_ORDER_BY_KEY,
          "",
          Array.empty,
          "")
        partitions += newPartition
      }
      currentMinPartsNum = -1L
      currentMaxPartsNum = -1L
      currentSize = 0
      currentFileCnt = 0L
    }

    val totalCores = SparkResourceUtil.getTotalCores(sparkSession.sessionState.conf)
    val fileCntPerPartition = math.ceil((partsFiles.size * 1.0) / totalCores).toInt
    val fileCntThreshold = sparkSession.sessionState.conf
      .getConfString(
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD,
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT
      )
      .toInt

    if (fileCntThreshold > 0 && fileCntPerPartition > fileCntThreshold) {
      // generate `Seq[InputPartition]` by file count
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentFileCnt >= fileCntPerPartition) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentFileCnt += 1
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    } else {
      // generate `Seq[InputPartition]` by file size
      val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
      val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
      logInfo(
        s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
          s"open cost is considered as scanning $openCostInBytes bytes.")
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentSize + parts.bytesOnDisk > maxSplitBytes) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentSize += parts.bytesOnDisk + openCostInBytes
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    }
    closePartition()
    partitions
  }

  def getMergeTreePartsPartitions(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      sparkSession: SparkSession,
      table: ClickHouseTableV2,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] = {
    val partsFiles = table.listFiles()

    val partitions = new ArrayBuffer[InputPartition]
    val (database, tableName) = if (table.catalogTable.isDefined) {
      (table.catalogTable.get.identifier.database.get, table.catalogTable.get.identifier.table)
    } else {
      // for file_format.`file_path`
      ("default", "file_format")
    }
    val engine = table.snapshot.metadata.configuration.get("engine").get
    // TODO: remove `substring`
    val tablePath = table.deltaLog.dataPath.toUri.getPath

    val (orderByKey, primaryKey) =
      genOrderByAndPrimaryKeyStr(table.orderByKeyOption, table.primaryKeyOption)

    val tableSchemaJson = genNamedStructJson(table.schema())

    // bucket table
    if (table.bucketOption.isDefined) {
      if (bucketedScan) {
        genBucketedInputPartitionSeq(
          engine,
          database,
          tableName,
          tablePath,
          table.bucketOption.get,
          partsFiles,
          partitions,
          optionalBucketSet,
          optionalNumCoalescedBuckets,
          sparkSession
        )
      } else {
        genInputPartitionSeqWithBucketTable(
          engine,
          database,
          tableName,
          tablePath,
          table.bucketOption.get,
          partsFiles,
          partitions,
          optionalBucketSet,
          sparkSession)
      }
    } else {
      genInputPartitionSeq(
        engine,
        database,
        tableName,
        tablePath,
        partsFiles,
        selectedPartitions,
        tableSchemaJson,
        partitions,
        orderByKey,
        primaryKey,
        sparkSession
      )
    }
    partitions
  }

  def genInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      tablePath: String,
      partsFiles: Seq[AddMergeTreeParts],
      selectedPartitions: Array[PartitionDirectory],
      tableSchemaJson: String,
      partitions: ArrayBuffer[InputPartition],
      orderByKey: String,
      primaryKey: String,
      sparkSession: SparkSession): Unit = {
    val selectedPartitionMap = selectedPartitions
      .flatMap(
        p => {
          p.files.map(
            f => {
              (f.getPath.toUri.getPath, f)
            })
        })
      .toMap

    val selectPartsFiles = partsFiles.filter(part => selectedPartitionMap.contains(part.name))

    if (selectPartsFiles.isEmpty) {
      return
    }

    val maxSplitBytes = getMaxSplitBytes(sparkSession, selectPartsFiles)
    val total_marks = selectPartsFiles.map(p => p.marks).sum
    val total_Bytes = selectPartsFiles.map(p => p.bytesOnDisk).sum
    val markCntPerPartition = maxSplitBytes * total_marks / total_Bytes + 1

    logInfo(s"Planning scan with bin packing, max mark: $markCntPerPartition")
    val splitFiles = selectPartsFiles
      .flatMap {
        part =>
          (0L until part.marks by markCntPerPartition).map {
            offset =>
              val remaining = part.marks - offset
              val size = if (remaining > markCntPerPartition) markCntPerPartition else remaining
              MergeTreePartitionedFile(
                part.name,
                part.path,
                part.targetNode,
                offset,
                size,
                size * part.bytesOnDisk / part.marks)
          }
      }
      .sortBy(_.targetNode)

    var currentSize = 0L
    val currentFiles = new ArrayBuffer[MergeTreePartitionedFile]
    var currentHostName = splitFiles(0).targetNode

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          tablePath,
          orderByKey,
          primaryKey,
          currentFiles.toArray,
          tableSchemaJson
        )
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // generate `Seq[InputPartition]` by file size
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    // val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    // Assign files to partitions using "Next Fit Decreasing"
    splitFiles.foreach {
      parts =>
        if (
          (currentSize + parts.bytesOnDisk > maxSplitBytes) ||
          !currentHostName.equals(parts.targetNode)
        ) {
          closePartition()
        }
        // Add the given file to the current partition.
        currentSize += parts.bytesOnDisk + openCostInBytes
        currentFiles += parts
        currentHostName = parts.targetNode
    }
    closePartition()
  }

  /** Generate bucket partition */
  def genBucketedInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      tablePath: String,
      bucketSpec: BucketSpec,
      partsFiles: Seq[AddMergeTreeParts],
      partitions: ArrayBuffer[InputPartition],
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      sparkSession: SparkSession): Unit = {
    val bucketGroupParts = partsFiles.groupBy(p => Integer.parseInt(p.bucketNum))

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      bucketGroupParts.filter(f => bucketSet.get(f._1))
    } else {
      bucketGroupParts
    }

    if (optionalNumCoalescedBuckets.isDefined) {
      throw new UnsupportedOperationException(
        "Currently CH backend can't support coalesced buckets.")
    }
    Seq.tabulate(bucketSpec.numBuckets) {
      bucketId =>
        val currBucketParts = prunedFilesGroupedToBuckets.getOrElse(bucketId, Seq.empty)
        if (!currBucketParts.isEmpty) {
          var currentMinPartsNum = Long.MaxValue
          var currentMaxPartsNum = -1L
          var currTableName = tableName + "_" + currBucketParts(0).bucketNum
          var currTablePath = tablePath + "/" + currBucketParts(0).bucketNum
          currBucketParts.foreach(
            p => {
              if (currentMinPartsNum >= p.minBlockNumber) currentMinPartsNum = p.minBlockNumber
              if (currentMaxPartsNum <= p.maxBlockNumber) currentMaxPartsNum = p.maxBlockNumber
            })
          if (currentMaxPartsNum >= currentMinPartsNum) {
            val newPartition = GlutenMergeTreePartition(
              bucketId,
              engine,
              database,
              tableName,
              tablePath,
              DEFAULT_ORDER_BY_KEY,
              "",
              Array.empty,
              "")
            partitions += newPartition
          }
        }
    }
  }

  /** Generate partition from the bucket table */
  def genInputPartitionSeqWithBucketTable(
      engine: String,
      database: String,
      tableName: String,
      tablePath: String,
      bucketSpec: BucketSpec,
      partsFiles: Seq[AddMergeTreeParts],
      partitions: ArrayBuffer[InputPartition],
      optionalBucketSet: Option[BitSet],
      sparkSession: SparkSession): Unit = {
    val bucketGroupParts = partsFiles.groupBy(p => Integer.parseInt(p.bucketNum))

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      bucketGroupParts.filter(f => bucketSet.get(f._1))
    } else {
      bucketGroupParts
    }

    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes

    def closePartition(
        currTableName: String,
        currTablePath: String,
        currentMinPartsNum: Long,
        currentMaxPartsNum: Long): Unit = {
      if (currentMaxPartsNum >= currentMinPartsNum) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          tablePath,
          DEFAULT_ORDER_BY_KEY,
          "",
          Array.empty,
          "")
        partitions += newPartition
      }
    }

    Seq.tabulate(bucketSpec.numBuckets) {
      bucketId =>
        val currBucketParts = prunedFilesGroupedToBuckets.getOrElse(bucketId, Seq.empty)
        if (!currBucketParts.isEmpty) {
          var currentMinPartsNum = Long.MaxValue
          var currentMaxPartsNum = -1L
          var currentSize = 0L
          var currTableName = tableName + "_" + currBucketParts(0).bucketNum
          var currTablePath = tablePath + "/" + currBucketParts(0).bucketNum

          currBucketParts.foreach {
            parts =>
              if (currentSize + parts.bytesOnDisk > maxSplitBytes) {
                closePartition(currTableName, currTablePath, currentMinPartsNum, currentMaxPartsNum)
                currentMinPartsNum = Long.MaxValue
                currentMaxPartsNum = -1L
                currentSize = 0L
              }
              // Add the given file to the current partition.
              currentSize += parts.bytesOnDisk + openCostInBytes
              if (currentMinPartsNum >= parts.minBlockNumber) {
                currentMinPartsNum = parts.minBlockNumber
              }
              if (currentMaxPartsNum <= parts.maxBlockNumber) {
                currentMaxPartsNum = parts.maxBlockNumber
              }
          }
          closePartition(currTableName, currTablePath, currentMinPartsNum, currentMaxPartsNum)
        }
    }
  }

  /** Generate partition from the non-bucket table */
  def genNonBuckedInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      tablePath: String,
      partsFiles: Seq[AddMergeTreeParts],
      partitions: ArrayBuffer[InputPartition],
      sparkSession: SparkSession): Unit = {
    var currentMinPartsNum = -1L
    var currentMaxPartsNum = -1L
    var currentSize = 0L
    var currentFileCnt = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentMinPartsNum > 0L && currentMaxPartsNum >= currentMinPartsNum) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          tablePath,
          DEFAULT_ORDER_BY_KEY,
          "",
          Array.empty,
          "")
        partitions += newPartition
      }
      currentMinPartsNum = -1L
      currentMaxPartsNum = -1L
      currentSize = 0
      currentFileCnt = 0L
    }

    val totalCores = SparkResourceUtil.getTotalCores(sparkSession.sessionState.conf)
    val fileCntPerPartition = math.ceil((partsFiles.size * 1.0) / totalCores).toInt
    val fileCntThreshold = sparkSession.sessionState.conf
      .getConfString(
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD,
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT
      )
      .toInt

    if (fileCntThreshold > 0 && fileCntPerPartition > fileCntThreshold) {
      // generate `Seq[InputPartition]` by file count
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentFileCnt >= fileCntPerPartition) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentFileCnt += 1
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    } else {
      // generate `Seq[InputPartition]` by file size
      val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
      val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
      logInfo(
        s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
          s"open cost is considered as scanning $openCostInBytes bytes.")
      // Assign files to partitions using "Next Fit Decreasing"
      partsFiles.foreach {
        parts =>
          if (currentSize + parts.bytesOnDisk > maxSplitBytes) {
            closePartition()
          }
          // Add the given file to the current partition.
          currentSize += parts.bytesOnDisk + openCostInBytes
          if (currentMinPartsNum == -1L) {
            currentMinPartsNum = parts.minBlockNumber
          }
          currentMaxPartsNum = parts.maxBlockNumber
      }
    }
    closePartition()
  }

  def getMaxSplitBytes(sparkSession: SparkSession, selectedParts: Seq[AddMergeTreeParts]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.leafNodeDefaultParallelism)
    val totalBytes = selectedParts.map(_.bytesOnDisk + openCostInBytes).sum
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
}
// scalastyle:on argcount
