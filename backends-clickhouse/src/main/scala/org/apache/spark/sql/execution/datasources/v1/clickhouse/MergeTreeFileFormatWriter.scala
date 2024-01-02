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
package org.apache.spark.sql.execution.datasources.v1.clickhouse

import io.glutenproject.execution.ColumnarToRowExecBase

import org.apache.spark.{SparkException, TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.files.MergeTreeCommitProtocol
import org.apache.spark.sql.delta.schema._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.FileFormatWriter.{processStats, ConcurrentOutputWriterSpec, OutputSpec}
import org.apache.spark.sql.execution.datasources.v1.GlutenMergeTreeWriterInjects
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.{SerializableConfiguration, Utils}

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.util.{Date, UUID}

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.{runtimeMirror, typeOf, TermName}

object MergeTreeFileFormatWriter extends Logging {

  def performCDCPartition(
      txn: OptimisticTransaction,
      inputData: Dataset[_]): (DataFrame, StructType) = {
    // If this is a CDC write, we need to generate the CDC_PARTITION_COL in order to properly
    // dispatch rows between the main table and CDC event records. This is a virtual partition
    // and will be stripped out later in [[DelayedCommitProtocolEdge]].
    // Note that the ordering of the partition schema is relevant - CDC_PARTITION_COL must
    // come first in order to ensure CDC data lands in the right place.
    if (
      CDCReader.isCDCEnabledOnTable(txn.metadata) &&
      inputData.schema.fieldNames.contains(CDCReader.CDC_TYPE_COLUMN_NAME)
    ) {
      val augmentedData = inputData.withColumn(
        CDCReader.CDC_PARTITION_COL,
        col(CDCReader.CDC_TYPE_COLUMN_NAME).isNotNull)
      val partitionSchema = StructType(
        StructField(
          CDCReader.CDC_PARTITION_COL,
          StringType) +: txn.metadata.physicalPartitionSchema)
      (augmentedData, partitionSchema)
    } else {
      (inputData.toDF(), txn.metadata.physicalPartitionSchema)
    }
  }

  def makeOutputNullable(output: Seq[Attribute]): Seq[Attribute] = {
    output.map {
      case ref: AttributeReference =>
        val nullableDataType = SchemaUtils.typeAsNullable(ref.dataType)
        ref.copy(dataType = nullableDataType, nullable = true)(ref.exprId, ref.qualifier)
      case attr => attr.withNullability(true)
    }
  }

  def checkPartitionColumns(
      partitionSchema: StructType,
      output: Seq[Attribute],
      colsDropped: Boolean): Unit = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map {
      col =>
        // schema is already normalized, therefore we can do an equality check
        output
          .find(f => f.name == col.name)
          .getOrElse(
            throw DeltaErrors.partitionColumnNotFoundException(col.name, output)
          )
    }
    if (partitionColumns.nonEmpty && partitionColumns.length == output.length) {
      throw DeltaErrors.nonPartitionColumnAbsentException(colsDropped)
    }
  }

  def mapColumnAttributes(
      metadata: Metadata,
      output: Seq[Attribute],
      mappingMode: DeltaColumnMappingMode): Seq[Attribute] = {
    DeltaColumnMapping.createPhysicalAttributes(output, metadata.schema, mappingMode)
  }

  def normalizeData(
      txn: OptimisticTransaction,
      metadata: Metadata,
      deltaLog: DeltaLog,
      data: Dataset[_]): (QueryExecution, Seq[Attribute], Seq[Constraint], Set[String]) = {
    val normalizedData = SchemaUtils.normalizeColumnNames(metadata.schema, data)
    val enforcesDefaultExprs =
      ColumnWithDefaultExprUtils.tableHasDefaultExpr(txn.protocol, metadata)
    val (dataWithDefaultExprs, generatedColumnConstraints, trackHighWaterMarks) =
      if (enforcesDefaultExprs) {
        ColumnWithDefaultExprUtils.addDefaultExprsOrReturnConstraints(
          deltaLog,
          // We need the original query execution if this is a streaming query, because
          // `normalizedData` may add a new projection and change its type.
          data.queryExecution,
          metadata.schema,
          normalizedData
        )
      } else {
        (normalizedData, Nil, Set[String]())
      }
    val cleanedData = SchemaUtils.dropNullTypeColumns(dataWithDefaultExprs)
    val queryExecution = if (cleanedData.schema != dataWithDefaultExprs.schema) {
      // This must be batch execution as DeltaSink doesn't accept NullType in micro batch DataFrame.
      // For batch executions, we need to use the latest DataFrame query execution
      cleanedData.queryExecution
    } else if (enforcesDefaultExprs) {
      dataWithDefaultExprs.queryExecution
    } else {
      assert(
        normalizedData == dataWithDefaultExprs,
        "should not change data when there is no generate column")
      // Ideally, we should use `normalizedData`. But it may use `QueryExecution` rather than
      // `IncrementalExecution`. So we use the input `data` and leverage the `nullableOutput`
      // below to fix the column names.
      data.queryExecution
    }
    val nullableOutput = makeOutputNullable(cleanedData.queryExecution.analyzed.output)
    val columnMapping = metadata.columnMappingMode
    // Check partition column errors
    checkPartitionColumns(
      metadata.partitionSchema,
      nullableOutput,
      nullableOutput.length < data.schema.size
    )
    // Rewrite column physical names if using a mapping mode
    val mappedOutput =
      if (columnMapping == NoMapping) nullableOutput
      else {
        mapColumnAttributes(metadata, nullableOutput, columnMapping)
      }
    (queryExecution, mappedOutput, generatedColumnConstraints, trackHighWaterMarks)
  }

  def getPartitioningColumns(
      partitionSchema: StructType,
      output: Seq[Attribute]): Seq[Attribute] = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map {
      col =>
        // schema is already normalized, therefore we can do an equality check
        // we have already checked for missing columns, so the fields must exist
        output.find(f => f.name == col.name).get
    }
    partitionColumns
  }

  def convertEmptyToNullIfNeeded(
      plan: SparkPlan,
      partCols: Seq[Attribute],
      constraints: Seq[Constraint]): SparkPlan = {
    if (
      !SparkSession.active.conf
        .get(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL)
    ) {
      return plan
    }
    // No need to convert if there are no constraints. The empty strings will be converted later by
    // FileFormatWriter and FileFormatDataWriter. Note that we might still do unnecessary convert
    // here as the constraints might not be related to the string partition columns. A precise
    // check will need to walk the constraints to see if such columns are really involved. It
    // doesn't seem to worth the effort.
    if (constraints.isEmpty) return plan

    val partSet = AttributeSet(partCols)
    var needConvert = false
    val projectList: Seq[NamedExpression] = plan.output.map {
      case p if partSet.contains(p) && p.dataType == StringType =>
        needConvert = true
        Alias(FileFormatWriter.Empty2Null(p), p.name)()
      case attr => attr
    }
    if (needConvert) {
      plan match {
        case adaptor: FakeRowAdaptor =>
          adaptor.withNewChildren(Seq(ProjectExec(projectList, adaptor.child)))
        case p: SparkPlan => p
      }
    } else plan
  }

  def setOptimisticTransactionHasWritten(txn: OptimisticTransaction): Unit = {
    val txnRuntimeMirror = runtimeMirror(classOf[OptimisticTransaction].getClassLoader)
    val txnInstanceMirror = txnRuntimeMirror.reflect(txn)
    val txnHasWritten = typeOf[OptimisticTransaction].member(TermName("hasWritten_$eq")).asMethod
    val txnHasWrittenMirror = txnInstanceMirror.reflectMethod(txnHasWritten)
    txnHasWrittenMirror(true)
  }

  // scalastyle:off argcount
  def writeFiles(
      txn: OptimisticTransaction,
      inputData: Dataset[_],
      deltaOptions: Option[DeltaOptions],
      writeOptions: Map[String, String],
      database: String,
      tableName: String,
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[String],
      bucketSpec: Option[BucketSpec],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    // use reflect to set the protected field: hasWritten
    setOptimisticTransactionHasWritten(txn)

    val deltaLog = txn.deltaLog
    val metadata = txn.metadata

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(txn, inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, _) =
      normalizeData(txn, metadata, deltaLog, data)
    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = new MergeTreeCommitProtocol("delta-mergetree", outputPath.toString, None)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    // val (optionalStatsTracker, _) = getOptionalStatsTrackerAndStatsCollection(output, outputPath,
    //   partitionSchema, data)
    val (optionalStatsTracker, _) = (None, None)

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, output)

      val queryPlan = queryExecution.executedPlan
      val newQueryPlan = queryPlan match {
        // if the child is columnar, we can just wrap&transfer the columnar data
        case c2r: ColumnarToRowExecBase =>
          FakeRowAdaptor(c2r.child)
        // If the child is aqe, we make aqe "support columnar",
        // then aqe itself will guarantee to generate columnar outputs.
        // So FakeRowAdaptor will always consumes columnar data,
        // thus avoiding the case of c2r->aqe->r2c->writer
        case aqe: AdaptiveSparkPlanExec =>
          FakeRowAdaptor(
            AdaptiveSparkPlanExec(
              aqe.inputPlan,
              aqe.context,
              aqe.preprocessingRules,
              aqe.isSubquery,
              supportsColumnar = true
            ))
        case other => queryPlan.withNewChildren(Array(FakeRowAdaptor(other)))
      }

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        // registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val options = writeOptions.filterKeys {
        key =>
          key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
          key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
      }.toMap

      try {
        write(
          sparkSession = spark,
          plan = newQueryPlan,
          fileFormat = new DeltaMergeTreeFileFormat(
            metadata,
            database,
            tableName,
            output,
            orderByKeyOption,
            primaryKeyOption,
            partitionColumns,
            bucketSpec),
          // formats.
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          orderByKeyOption = orderByKeyOption,
          primaryKeyOption = primaryKeyOption,
          partitionColumns = partitioningColumns,
          bucketSpec = bucketSpec,
          statsTrackers = optionalStatsTracker.toSeq ++ statsTrackers,
          options = options,
          constraints = constraints
        )
      } catch {
        case s: SparkException =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          val violationException = ExceptionUtils.getRootCause(s)
          if (violationException.isInstanceOf[InvariantViolationException]) {
            throw violationException
          } else {
            throw s
          }
      }
    }

    // val resultFiles = committer.addedStatuses.map { a =>
    //   a.copy(stats = optionalStatsTracker.map(
    //    _.recordedStats(new Path(new URI(a.path)).getName)).getOrElse(a.stats))
    /* val resultFiles = committer.addedStatuses.filter {
      // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
      // may be fixed in the future) are the MERGE command when you delete with empty source, or
      // empty target, or on disjoint tables. This is hard to catch before the write without
      // collecting the DF ahead of time. Instead, we can return only the AddFiles that
      // a) actually add rows, or
      // b) don't have any stats so we don't know the number of rows at all
      case a: AddFile => a.numLogicalRecords.forall(_ > 0)
      case _ => true
    } */

    committer.addedStatuses.toSeq ++ committer.changeFiles
  }

  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String],
      constraints: Seq[Constraint]): Set[String] = write(
    sparkSession = sparkSession,
    plan = plan,
    fileFormat = fileFormat,
    committer = committer,
    outputSpec = outputSpec,
    hadoopConf = hadoopConf,
    orderByKeyOption = orderByKeyOption,
    primaryKeyOption = primaryKeyOption,
    partitionColumns = partitionColumns,
    bucketSpec = bucketSpec,
    statsTrackers = statsTrackers,
    options = options,
    constraints,
    numStaticPartitionCols = 0
  )

  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String],
      constraints: Seq[Constraint],
      numStaticPartitionCols: Int = 0): Set[String] = {

    assert(plan.isInstanceOf[IFakeRowAdaptor])

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])

    val outputPath = new Path(outputSpec.outputPath)
    val outputPathNam = outputPath.toUri.getPath

    FileOutputFormat.setOutputPath(job, outputPath)

    val partitionSet = AttributeSet(partitionColumns)
    // cleanup the internal metadata information of
    // the file source metadata attribute if any before write out
    // val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns
    //   .map(FileSourceMetadataAttribute.cleanupFileSourceMetadataInformation))
    val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns)
    val dataColumns = finalOutputSpec.outputColumns.filterNot(partitionSet.contains)

    val empty2NullPlan = plan // convertEmptyToNullIfNeeded(plan, partitionColumns, constraints)

    val writerBucketSpec = bucketSpec.map {
      spec =>
        val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
        // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
        // expression, so that we can guarantee the data distribution is same between shuffle and
        // bucketed data source, which enables us to only shuffle one side when join a bucketed
        // table and a normal one.
        val bucketIdExpression =
          HashPartitioning(bucketColumns, spec.numBuckets).partitionIdExpression
        MergeTreeWriterBucketSpec(bucketIdExpression, (_: Int) => "")
    }
    val sortColumns = bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }

    val caseInsensitiveOptions = CaseInsensitiveMap(options)

    val dataSchema = dataColumns.toStructType
    DataSourceUtils.verifySchema(fileFormat, dataSchema)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, caseInsensitiveOptions, dataSchema)

    val description = new MergeTreeWriteJobDescription(
      uuid = UUID.randomUUID.toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = finalOutputSpec.outputColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketSpec = writerBucketSpec,
      path = outputPathNam,
      customPartitionLocations = finalOutputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions
        .get("maxRecordsPerFile")
        .map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions
        .get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = statsTrackers
    )

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val requiredOrdering = partitionColumns.drop(numStaticPartitionCols) ++
      writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns
    // the sort order doesn't matter
    val actualOrdering = empty2NullPlan.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    // propagate the description UUID into the jobs, so that committers
    // get an ID guaranteed to be unique.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", description.uuid)

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)

    def nativeWrap(plan: SparkPlan) = {
      var wrapped: SparkPlan = plan
      if (writerBucketSpec.isDefined) {
        // We need to add the bucket id expression to the output of the sort plan,
        // so that we can use backend to calculate the bucket id for each row.
        wrapped = ProjectExec(
          wrapped.output :+ Alias(writerBucketSpec.get.bucketIdExpression, "__bucket_value__")(),
          wrapped)
        // TODO: to optimize, bucket value is computed twice here
      }

      val nativeFormat = sparkSession.sparkContext.getLocalProperty("nativeFormat")
      (GlutenMergeTreeWriterInjects.getInstance().executeWriterWrappedSparkPlan(wrapped), None)
    }

    try {
      val (rdd, concurrentOutputWriterSpec) = if (orderingMatched) {
        nativeWrap(empty2NullPlan)
      } else {
        // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
        // the physical plan may have different attribute ids due to optimizer removing some
        // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
        val orderingExpr = bindReferences(
          requiredOrdering.map(SortOrder(_, Ascending)),
          finalOutputSpec.outputColumns)
        val sortPlan = SortExec(orderingExpr, global = false, child = empty2NullPlan)

        val maxWriters = sparkSession.sessionState.conf.maxConcurrentOutputFileWriters
        var concurrentWritersEnabled = maxWriters > 0 && sortColumns.isEmpty
        if (concurrentWritersEnabled) {
          log.warn(
            s"spark.sql.maxConcurrentOutputFileWriters(being set to $maxWriters) will be " +
              "ignored when native writer is being active. No concurrent Writers.")
          concurrentWritersEnabled = false
        }

        if (concurrentWritersEnabled) {
          (
            empty2NullPlan.execute(),
            Some(ConcurrentOutputWriterSpec(maxWriters, () => sortPlan.createSorter())))
        } else {
          nativeWrap(sortPlan)
        }
      }

      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
        sparkSession.sparkContext.parallelize(Array.empty[InternalRow], 1)
      } else {
        rdd
      }

      val jobIdInstant = new Date().getTime
      val ret = new Array[MergeTreeWriteTaskResult](rddWithNonEmptyPartitions.partitions.length)
      sparkSession.sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
          executeTask(
            description = description,
            jobIdInstant = jobIdInstant,
            sparkStageId = taskContext.stageId(),
            sparkPartitionId = taskContext.partitionId(),
            sparkAttemptNumber = taskContext.taskAttemptId().toInt & Integer.MAX_VALUE,
            committer,
            iterator = iter,
            concurrentOutputWriterSpec = concurrentOutputWriterSpec
          )
        },
        rddWithNonEmptyPartitions.partitions.indices,
        (index, res: MergeTreeWriteTaskResult) => {
          committer.onTaskCommit(res.commitMsg)
          ret(index) = res
        }
      )

      val commitMsgs = ret.map(_.commitMsg)

      logInfo(s"Start to commit write Job ${description.uuid}.")
      val (_, duration) = Utils.timeTakenMs(committer.commitJob(job, commitMsgs))
      logInfo(s"Write Job ${description.uuid} committed. Elapsed time: $duration ms.")

      processStats(description.statsTrackers, ret.map(_.summary.stats), duration)
      logInfo(s"Finished processing stats for write job ${description.uuid}.")

      // return a set of all the partition paths that were updated during this job
      ret.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    } catch {
      case cause: Throwable =>
        logError(s"Aborting job ${description.uuid}.", cause)
        committer.abortJob(job)
        throw QueryExecutionErrors.jobAbortedError(cause)
    }
  }
  // scalastyle:on argcount

  def executeTask(
      description: MergeTreeWriteJobDescription,
      jobIdInstant: Long,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow],
      concurrentOutputWriterSpec: Option[ConcurrentOutputWriterSpec]
  ): MergeTreeWriteTaskResult = {

    val jobId = SparkHadoopWriterUtils.createJobID(new Date(jobIdInstant), sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)
      hadoopConf.set("mapreduce.mergetree.job.uuid", description.uuid)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    val dataWriter =
      if (sparkPartitionId != 0 && !iterator.hasNext) {
        // In case of empty job,
        // leave first partition to save meta for file format like parquet/orc.
        new MergeTreeEmptyDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
        new MergeTreeSingleDirectoryDataWriter(description, taskAttemptContext, committer)
      } else {
        concurrentOutputWriterSpec match {
          case Some(spec) =>
            new MergeTreeDynamicPartitionDataConcurrentWriter(
              description,
              taskAttemptContext,
              committer,
              spec)
          case _ =>
            new MergeTreeDynamicPartitionDataSingleWriter(
              description,
              taskAttemptContext,
              committer)
        }
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        dataWriter.writeWithIterator(iterator)
        dataWriter.commit()
      })(
        catchBlock = {
          // If there is an error, abort the task
          dataWriter.abort()
          logError(s"Job $jobId aborted.")
        },
        finallyBlock = {
          dataWriter.close()
        })
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        // If any output file to write already exists, it does not make sense to re-run this task.
        // We throw the exception and let Executor throw ExceptionFailure to abort the job.
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw QueryExecutionErrors.taskFailedWhileWritingRowsError(t)
    }
  }
}
