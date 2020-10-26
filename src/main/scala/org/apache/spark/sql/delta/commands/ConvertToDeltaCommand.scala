/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import java.util.Locale
import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaSQLConf, DeltaSourceUtils}
import org.apache.spark.sql.delta.util.{DateFormatter, DeltaFileOperations, PartitionUtils, TimestampFormatter}
import org.apache.spark.sql.delta.util.FileNames.deltaFile
import org.apache.spark.sql.delta.util.SerializableFileStatus
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.delta.services.{ConvertToDeltaEvent, DeltaTableMetadata}
import org.apache.spark.sql.execution.command.{AlterTableDropPartitionCommand, AlterTableDropRangePartitionCommand, DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * Convert an existing parquet table to a delta table by creating delta logs based on
 * existing files. Here are the main components:
 *
 *   - File Listing:      Launch a spark job to list files from a given directory in parallel.
 *
 *   - Schema Inference:  Given an iterator on the file list result, we group the iterator into
 *                        sequential batches and launch a spark job to infer schema for each batch,
 *                        and finally merge schemas from all batches.
 *
 *   - Stats collection:  Again, we group the iterator on file list results into sequential batches
 *                        and launch a spark job to collect stats for each batch.
 *
 *   - Commit the files:  We take the iterator of files with stats and write out a delta
 *                        log file as the first commit. This bypasses the transaction protocol, but
 *                        it's ok as this would be the very first commit.
 *
 * @param tableIdentifier the target parquet table.
 * @param partitionSchema the partition schema of the table, required when table is partitioned.
 * @param deltaPath if provided, the delta log will be written to this location.
 */
abstract class ConvertToDeltaCommandBase(
    tableIdentifier: TableIdentifier,
    partitionSchema: Option[StructType],
    deltaPath: Option[String]) extends RunnableCommand with DeltaCommand {

  var partitionColNames: Seq[String] = Nil
  var partitionFields: Seq[StructField] = Nil

  val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"

  override def run(spark: SparkSession): Seq[Row] = {
    val convertProperties = getConvertProperties(spark, tableIdentifier)

    convertProperties.provider match {
      case Some(providerName) => providerName.toLowerCase(Locale.ROOT) match {
        // Make convert to delta idempotent
        case delta if DeltaSourceUtils.isDeltaDataSourceName(delta) =>
          logConsole("The table you are trying to convert is already a delta table")
          logWarning("The table you are trying to convert is already a delta table")
          return Seq.empty[Row]
        case checkProvider if checkProvider != "parquet" =>
          throw DeltaErrors.convertNonParquetTablesException(tableIdentifier, checkProvider)
        case _ =>
      }
      case None =>
        throw DeltaErrors.missingProviderForConvertException(convertProperties.targetDir)
    }

    val deltaLog = DeltaLog.forTable(spark, deltaPath.getOrElse(convertProperties.targetDir))
    deltaLog.update()
    val txn = deltaLog.startTransaction()
    if (txn.readVersion > -1) {
      handleExistingTransactionLog(spark, txn, convertProperties)
      return Seq.empty[Row]
    }

    performConvert(spark, txn, convertProperties)
  }

  protected def getConvertProperties(
      spark: SparkSession,
      tableIdentifier: TableIdentifier): ConvertProperties = {
    def convertForPath(tableIdentifier: TableIdentifier): ConvertProperties = {
      // convert to delta format.`path`
      ConvertProperties(
        None,
        tableIdentifier.database,
        tableIdentifier.table,
        Map.empty[String, String])
    }

    def convertForTable(tableIdentifier: TableIdentifier): ConvertProperties = {
      val table = spark.sessionState.catalog.getTableMetadata(tableIdentifier)
      ConvertProperties(
        Some(table),
        table.provider,
        CatalogUtils.URIToString(table.location),
        table.properties)
    }

    val identifier = if (tableIdentifier.database.isEmpty) {
      tableIdentifier.copy(database = Some(spark.sessionState.catalog.getCurrentDatabase))
    } else {
      tableIdentifier.copy(
        database = tableIdentifier.database.map(_.toLowerCase(Locale.ROOT)))
    }

    val formats = Seq("delta", "parquet", "orc", "json", "csv", "tsv", "hive", "jdbc")
    if (formats.exists(identifier.database.contains(_))) {
      // convert to delta format.`path`
      convertForPath(identifier)
    } else {
      // convert to delta table
      convertForTable(identifier)
    }
  }

  /**
   * Calls DeltaCommand.isCatalogTable. With Convert, we may get a format check error in cases where
   * the metastore and the underlying table don't align, e.g. external table where the underlying
   * files are converted to delta but the metadata has not been converted yet. In these cases,
   * catch the error and return based on whether the provided Table Identifier could reasonably be
   * a path
   *
   * @param analyzer The session state analyzer to call
   * @param tableIdent Table Identifier to determine whether is path based or not
   * @return Boolean where true means that the table is a table in a metastore and false means the
   *         table is a path based table
   */
  override def isCatalogTable(analyzer: Analyzer, tableIdent: TableIdentifier): Boolean = {
    try {
      super.isCatalogTable(analyzer, tableIdentifier)
    } catch {
      case e: AnalysisException if e.getMessage.contains("Incompatible format detected") =>
        !isPathIdentifier(tableIdentifier)
    }
  }

  /**
   * Override this method since parquet paths are valid for Convert
   * @param tableIdent the provided table or path
   * @return Whether or not the ident provided can refer to a table by path
   */
  override def isPathIdentifier(tableIdent: TableIdentifier): Boolean = {
    val provider = tableIdent.database.getOrElse("")
    // If db doesnt exist or db is called delta/tahoe then check if path exists
    (DeltaSourceUtils.isDeltaDataSourceName(provider) || provider == "parquet") &&
      new Path(tableIdent.table).isAbsolute
  }

  protected def handleExistingTransactionLog(
      spark: SparkSession,
      txn: OptimisticTransaction,
      convertProperties: ConvertProperties): Unit = {
    logConsole("The table you are trying to convert is already a delta table")
    logWarning("The table you are trying to convert is already a delta table")
  }

  protected def performConvert(
      spark: SparkSession,
      txn: OptimisticTransaction,
      convertProperties: ConvertProperties): Seq[Row] =
    recordDeltaOperation(txn.deltaLog, "delta.convert") {

    val targetPath = new Path(convertProperties.targetDir)
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val fs = targetPath.getFileSystem(sessionHadoopConf)
    val qualifiedPath = fs.makeQualified(targetPath)
    val qualifiedDir = qualifiedPath.toString
    if (!fs.exists(qualifiedPath)) {
      throw DeltaErrors.pathNotExistsException(qualifiedDir)
    }
    txn.deltaLog.ensureLogDirectoryExist()

    val conf = spark.sparkContext.broadcast(
      new SerializableConfiguration(spark.sessionState.newHadoopConf()))

    val fileListResultDf = DeltaFileOperations.recursiveListDirs(
        spark, Seq(qualifiedDir), conf).where("!isDir")
    fileListResultDf.cache()

    def fileListResult = fileListResultDf.collectAsIterator()

    var numFiles = 0L
    var dataSchema: StructType = StructType(Seq())
    try {
      if (convertProperties.catalogTable.isDefined) {
        dataSchema = convertProperties.catalogTable.get.dataSchema
        numFiles = fileListResultDf.count()
      } else {
        if (!fileListResult.hasNext) {
          throw DeltaErrors.emptyDirectoryException(qualifiedDir)
        } else {
          val schemaBatchSize =
            spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_SCHEMA_INFERENCE)
          fileListResult.grouped(schemaBatchSize).foreach { batch =>
            numFiles += batch.size
            // Obtain a union schema from all files.
            // Here we explicitly mark the inferred schema nullable.
            // This also means we don't currently
            // support specifying non-nullable columns after the table conversion.
            val batchSchema =
            recordDeltaOperation(txn.deltaLog, "delta.convert.schemaInference") {
              mergeSchemasInParallel(spark, batch.map(_.toFileStatus)).getOrElse(
                throw new RuntimeException("Failed to infer schema from the given list of files."))
            }.asNullable
            dataSchema = SchemaUtils.mergeSchemas(dataSchema, batchSchema)
          }
        }
      }

      partitionColNames = partitionSchema.map(_.fieldNames.toSeq).getOrElse(
        convertProperties.catalogTable.map(_.partitionSchema.fieldNames.toSeq).getOrElse(Nil))
      partitionFields = partitionSchema.map(_.fields.toSeq).getOrElse(
        convertProperties.catalogTable.map(_.partitionSchema.fields.toSeq).getOrElse(Nil))
      val schema = constructTableSchema(spark, dataSchema, partitionFields)
      val metadata = Metadata(schemaString = schema.json, partitionColumns = partitionColNames,
        bucketSpec = convertProperties.catalogTable.flatMap(_.bucketSpec))
      txn.updateMetadata(metadata)

      val statsBatchSize =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_STATS_COLLECTION)

      val addFilesIter = fileListResult.grouped(statsBatchSize).flatMap { batch =>
        val adds = batch.map(createAddFile(_, txn.deltaLog.dataPath, fs, spark.sessionState.conf))
        adds.toIterator
      }

      if (convertProperties.catalogTable.isDefined) {
        val oldTable = convertProperties.catalogTable.get
        val newTable = oldTable.copy(provider = Some("delta"), tracksPartitionsInCatalog = false)
        try {
          // 1. now we can drop partition metadata from hive metastore
          val deletedPartitions = spark.sessionState.catalog.listPartitions(oldTable.identifier)
          if (deletedPartitions.nonEmpty) {
            val isRangePartitionedTable = false // todo (lajin)
            if (isRangePartitionedTable) {
              AlterTableDropRangePartitionCommand(
                oldTable.identifier,
                deletedPartitions.map(_.spec.head._2),
                ifExists = true,
                purge = false,
                retainData = true).run(spark)
            } else {
              AlterTableDropPartitionCommand.fromSpecs(
                oldTable.identifier,
                deletedPartitions.map(_.spec),
                ifExists = true,
                purge = false,
                retainData = true /* already deleted */).run(spark)
            }
          }
        } catch {
          case e: Throwable =>
            // just logging, won't rollback
            logError(s"Failed to drop partition metadata for ${oldTable.identifier}, " +
              s"some dirty data will keep in Hive Metastore.", e)
        }

        // 2. change provider to delta. prevent spark reading it as parquet
        // todo (lajin) step 2 should execute before step 1.
        // but now we can not DropPartition to delta table, keep this as todo
        spark.sessionState.catalog.alterTable(newTable)

        spark.catalog.refreshTable(newTable.identifier.quotedString)
        val qualifiedTableName = QualifiedTableName(newTable.database, newTable.identifier.table)
        spark.sessionState.catalog.invalidateCachedTable(qualifiedTableName)
      }

      // 3. write delta log
      streamWrite(
        spark,
        txn,
        addFilesIter,
        DeltaOperations.Convert(
          numFiles,
          partitionColNames,
          collectStats = false,
          convertProperties))
    } finally {
      try {
        fileListResultDf.unpersist(blocking = true)
      } catch {
        case t: TimeoutException =>
          if (convertProperties.catalogTable.isDefined) {
            val tbl = convertProperties.catalogTable.get.identifier
            logWarning(s"Unpersist fileListResultDf for $tbl timeout, no need to retry", t)
          }
      }
    }
    Seq.empty[Row]
  }

  protected def createAddFile(
      file: SerializableFileStatus,
      basePath: Path,
      fs: FileSystem,
      conf: SQLConf): AddFile = {
    val path = file.getPath
    val pathStr = file.getPath.toUri.toString
    val dateFormatter = DateFormatter()
    val timestampFormatter =
      TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)
    val resolver = conf.resolver
    val dir = if (file.isDir) file.getPath else file.getPath.getParent
    val (partitionOpt, _) = PartitionUtils.parsePartition(
      dir,
      typeInference = false,
      basePaths = Set.empty,
      userSpecifiedDataTypes = Map.empty,
      validatePartitionColumns = false,
      java.util.TimeZone.getDefault,
      dateFormatter,
      timestampFormatter)

    val partition = partitionOpt.map { partValues =>
      if (partitionColNames.size != partValues.columnNames.size) {
        throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(
          pathStr, partValues.columnNames, partitionColNames)
      }

      val tz = Option(conf.sessionLocalTimeZone)
      // Check if the partition value can be casted to the provided type
      partValues.literals.zip(partitionFields).foreach { case (literal, field) =>
        if (literal.eval() != null && Cast(literal, field.dataType, tz).eval() == null) {
          val partitionValue = Cast(literal, StringType, tz).eval()
          val partitionValueStr = Option(partitionValue).map(_.toString).orNull
          throw DeltaErrors.castPartitionValueException(partitionValueStr, field.dataType)
        }
      }

      val values = partValues
        .literals
        .map(l => Cast(l, StringType, tz).eval())
        .map(Option(_).map(_.toString).orNull)

      partitionColNames.zip(partValues.columnNames).foreach { case (expected, parsed) =>
        if (!resolver(expected, parsed)) {
          throw DeltaErrors.unexpectedPartitionColumnFromFileNameException(
            pathStr, parsed, expected)
        }
      }
      partitionColNames.zip(values).toMap
    }.getOrElse {
      if (partitionColNames.nonEmpty) {
        throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(
          pathStr, Seq.empty, partitionColNames)
      }
      Map[String, String]()
    }

    val pathStrForAddFile = if (deltaPath.isEmpty) {
      val relativePath = DeltaFileOperations.tryRelativizePath(fs, basePath, path)
      assert(!relativePath.isAbsolute,
        s"Fail to relativize path $path against base path $basePath.")
      relativePath.toUri.toString
    } else {
      path.toUri.toString
    }

    AddFile(pathStrForAddFile, partition, file.length, file.modificationTime, dataChange = true)
  }

  /**
   * Construct a table schema by merging data schema and partition schema.
   * We follow the merge logic in [[org.apache.spark.sql.execution.datasources.HadoopFsRelation]]:
   *
   *   When data and partition schemas have overlapping columns, the output
   *   schema respects the order of the data schema for the overlapping columns, and it
   *   respects the data types of the partition schema.
   */
  protected def constructTableSchema(
      spark: SparkSession,
      dataSchema: StructType,
      partitionFields: Seq[StructField]): StructType = {

    def getColName(f: StructField): String = {
      if (spark.sessionState.conf.caseSensitiveAnalysis) {
        f.name
      } else {
        f.name.toLowerCase(Locale.ROOT)
      }
    }

    val overlappedPartCols = collection.mutable.Map.empty[String, StructField]
    partitionFields.foreach { partitionField =>
      if (dataSchema.exists(getColName(_) == getColName(partitionField))) {
        overlappedPartCols += getColName(partitionField) -> partitionField
      }
    }

    StructType(dataSchema.map(f => overlappedPartCols.getOrElse(getColName(f), f)) ++
      partitionFields.filterNot(f => overlappedPartCols.contains(getColName(f))))
  }

  protected def getContext: Map[String, String] = {
    Map.empty
  }

  protected def getContext(spark: SparkSession): Map[String, String] = {
    Map("user" -> spark.sessionState.catalog.getCurrentUser)
  }

  /**
   * Create the first commit on the Delta log by directly writing an iterator of AddFiles to the
   * LogStore. This bypasses the Delta transactional protocol, but we assume this is ok as this is
   * the very first commit and only happens at table conversion which is a one-off process.
   */
  protected def streamWrite(
      spark: SparkSession,
      txn: OptimisticTransaction,
      addFiles: Iterator[AddFile],
      op: DeltaOperations.Convert): Long = {
    val firstVersion = 0L
    try {
      val deltaLog = txn.deltaLog
      val metadata = txn.metadata
      val context = getContext(spark)
      val commitInfo = CommitInfo(
        time = txn.clock.getTimeMillis(),
        operation = op.name,
        operationParameters = op.jsonEncodedValues,
        context,
        readVersion = None,
        isolationLevel = None,
        isBlindAppend = None,
        None)

      val extraActions = Seq(commitInfo, Protocol(), metadata)
      val actions = extraActions.toIterator ++ addFiles
      deltaLog.store.write(deltaFile(deltaLog.logPath, firstVersion), actions.map(_.json))

      val currentSnapshot = deltaLog.update()
      if (currentSnapshot.version != firstVersion) {
        throw new IllegalStateException(
          s"The committed version is $firstVersion but the current version is " +
            s"${currentSnapshot.version}. Please contact Databricks support.")
      }

      logInfo(s"Committed delta #$firstVersion to ${deltaLog.logPath}")
      saveToMetaTable(spark, op.convertProperties)

      try {
        deltaLog.checkpoint()
      } catch {
        case e: IllegalStateException =>
          logWarning("Failed to checkpoint table state.", e)
      }

      firstVersion
    } catch {
      case e: java.nio.file.FileAlreadyExistsException =>
        recordDeltaEvent(
          txn.deltaLog,
          "delta.convert.commitFailure",
          data = Map("exception" -> Utils.exceptionString(e)))
        throw DeltaErrors.commitAlreadyExistsException(firstVersion, txn.deltaLog.logPath)

      case NonFatal(e) =>
        recordDeltaEvent(
          txn.deltaLog,
          "delta.convert.commitFailure",
          data = Map("exception" -> Utils.exceptionString(e)))
        throw e
    }
  }

  /**
   * This method is forked from [[ParquetFileFormat]]. The only change here is that we use
   * our SchemaUtils.mergeSchemas() instead of StructType.merge(), where we allow upcast between
   * ByteType, ShortType and IntegerType.
   *
   * Figures out a merged Parquet schema with a distributed Spark job.
   *
   * Note that locality is not taken into consideration here because:
   *
   *  1. For a single Parquet part-file, in most cases the footer only resides in the last block of
   *     that file.  Thus we only need to retrieve the location of the last block.  However, Hadoop
   *     `FileSystem` only provides API to retrieve locations of all blocks, which can be
   *     potentially expensive.
   *
   *  2. This optimization is mainly useful for S3, where file metadata operations can be pretty
   *     slow.  And basically locality is not available when using S3 (you can't run computation on
   *     S3 nodes).
   */
  protected def mergeSchemasInParallel(
      sparkSession: SparkSession, filesToTouch: Seq[FileStatus]): Option[StructType] = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp
    val serializedConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf())

    // !! HACK ALERT !!
    //
    // Parquet requires `FileStatus`es to read footers.  Here we try to send cached `FileStatus`es
    // to executor side to avoid fetching them again.  However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo = filesToTouch.map(f => (f.getPath.toString, f.getLen))

    // Set the number of partitions to prevent following schema reads from generating many tasks
    // in case of a small number of parquet files.
    val numParallelism = Math.min(Math.max(partialFileStatusInfo.size, 1),
      sparkSession.sparkContext.defaultParallelism)

    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles

    // Issues a Spark job to read Parquet schema in parallel.
    val partiallyMergedSchemas =
      sparkSession
        .sparkContext
        .parallelize(partialFileStatusInfo, numParallelism)
        .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
          }.toSeq

          // Reads footers in multi-threaded manner within each task
          val footers =
            DeltaFileOperations.readParquetFootersInParallel(
              serializedConf.value, fakeFileStatuses, ignoreCorruptFiles)

          // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
          val converter = new ParquetToSparkSchemaConverter(
            assumeBinaryIsString = assumeBinaryIsString,
            assumeInt96IsTimestamp = assumeInt96IsTimestamp)
          if (footers.isEmpty) {
            Iterator.empty
          } else {
            var mergedSchema = ParquetFileFormat.readSchemaFromFooter(footers.head, converter)
            footers.tail.foreach { footer =>
              val schema = ParquetFileFormat.readSchemaFromFooter(footer, converter)
              try {
                mergedSchema = SchemaUtils.mergeSchemas(mergedSchema, schema)
              } catch { case cause: AnalysisException =>
                throw new SparkException(
                  s"Failed to merge schema of file ${footer.getFile}:\n${schema.treeString}", cause)
              }
            }
            Iterator.single(mergedSchema)
          }
        }.collect()

    if (partiallyMergedSchemas.isEmpty) {
      None
    } else {
      var finalSchema = partiallyMergedSchemas.head
      partiallyMergedSchemas.tail.foreach { schema =>
        finalSchema = SchemaUtils.mergeSchemas(finalSchema, schema)
      }
      Some(finalSchema)
    }
  }

  private def saveToMetaTable(spark: SparkSession, convertProperties: ConvertProperties): Unit = {
    val defaultRetentionHours =
      spark.sessionState.conf.getConf(DeltaSQLConf.AUTO_VACUUM_RETENTION_HOURS)
    convertProperties.catalogTable.foreach { table =>
      val metadata = DeltaTableMetadata(
        table.identifier.database.getOrElse(""),
        table.identifier.table,
        spark.sessionState.catalog.getCurrentUser,
        convertProperties.targetDir,
        vacuum = true,
        retention = defaultRetentionHours)
      val isTemp = DDLUtils.isTemporaryTable(table)
      spark.sharedState.externalCatalog.postToAll(ConvertToDeltaEvent(metadata, isTemp))
    }
  }
}

case class ConvertProperties(
    catalogTable: Option[CatalogTable],
    provider: Option[String],
    targetDir: String,
    properties: Map[String, String])

case class ConvertToDeltaCommand(
    tableIdentifier: TableIdentifier,
    partitionSchema: Option[StructType],
    deltaPath: Option[String])
  extends ConvertToDeltaCommandBase(tableIdentifier, partitionSchema, deltaPath) {
}
