/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.services.{ConvertToParquetEvent, DeltaTableMetadata}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, CommandUtils, DDLUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.{Failure, Success, Try}

case class ConvertBackCommand(
    tableIdentifier: TableIdentifier,
    partitionSchema: Option[StructType],
    deltaPath: Option[String])
  extends ConvertToDeltaCommandBase(tableIdentifier, partitionSchema, deltaPath) {

  override def run(spark: SparkSession): Seq[Row] = {
    val convertProperties = resolveConvertTarget(spark, tableIdentifier) match {
      case Some(props) if DeltaSourceUtils.isDeltaTable(props.provider) => props
      case Some(_) =>
        logConsole("The table you are trying to convert is already a non delta table")
        return Seq.empty[Row]
      case None =>
        val v2SessionCatalog =
          spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[TableCatalog]
        val ident = Identifier.of(
          tableIdentifier.database.map(Array(_))
            .getOrElse(spark.sessionState.catalogManager.currentNamespace),
          tableIdentifier.table)
        val table = v2SessionCatalog.loadTable(ident).asInstanceOf[DeltaTableV2].catalogTable
        if (table.isDefined) {
          val props = table.get.properties.filterKeys(_ != "transient_lastDdlTime")
          ConvertTarget(table, table.get.provider, new Path(table.get.location).toString, props)
        } else {
          throw DeltaErrors.unsupportedInHiveMetastoreException(tableIdentifier.identifier)
        }
    }

    if (convertProperties.catalogTable.isEmpty) {
      throw DeltaErrors.unsupportedInHiveMetastoreException(convertProperties.targetDir)
    }

    val deltaLog = DeltaLog.forTable(spark, convertProperties.catalogTable.get)
    val txn = deltaLog.startTransaction()
    performConvertBack(spark, txn, convertProperties)
  }

  protected def performConvertBack(
      spark: SparkSession,
      txn: OptimisticTransaction,
      convertProperties: ConvertTarget): Seq[Row] = {

    val targetPath = new Path(convertProperties.targetDir)
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val fs = targetPath.getFileSystem(sessionHadoopConf)
    val qualifiedPath = fs.makeQualified(targetPath)
    val qualifiedDir = qualifiedPath.toString
    if (!fs.exists(qualifiedPath)) {
      throw DeltaErrors.pathNotExistsException(qualifiedDir)
    }
    txn.deltaLog.checkLogDirectoryExist()

    // 1. clean all untracked files by vacuum
    VacuumCommand.gc(spark, txn.deltaLog, dryRun = false, Some(0), safetyCheckEnabled = false)

    val oldTable = convertProperties.catalogTable.get
    val newTable = convertProperties.catalogTable.get
      .copy(provider = Some("parquet"), tracksPartitionsInCatalog = true)

    try {
      // todo (lajin) step 3 should execute before step 2.
      // but now we can not AddPartition to delta table, keep this as todo
      // 2. change provider to parquet. after this, spark reads it as parquet table
      spark.sessionState.catalog.alterTable(newTable)
      // 3. write partition metadata to hive metastore todo (lajin)
//      val partitionColumns = Try {
//        txn.deltaLog.snapshot.listPartitions(newTable)
//      } match {
//        case Success(value) => value
//        case Failure(exception) => Nil
//      }
//      if (partitionColumns.nonEmpty) {
//        val isRangePartitionedTable = false // todo (lajin)
//        if (isRangePartitionedTable) {
//          val namedPartitions = partitionColumns.map { p =>
//            val name = p.spec.values.head
//            ((Some(name), p.spec), None)
//          }
//          AlterTableAddRangePartitionCommand(newTable.identifier,
//            namedPartitions, ifNotExists = true).run(spark)
//        } else {
//          AlterTableAddPartitionCommand(newTable.identifier,
//            partitionColumns.map(p => (p.spec, None)), ifNotExists = true).run(spark)
//        }
//      }
    } catch {
      case e: Throwable =>
        // rollback provider to delta
        spark.sessionState.catalog.alterTable(oldTable)
        throw e
    }

    // 4. succeed to convert to parquet, safety delete delta log dir
    val store = txn.deltaLog.store
    store.delete(Seq(txn.deltaLog.logPath), recursive = true)

    // 5. clean cache and refresh
    val qualified = QualifiedTableName(newTable.database, newTable.identifier.table)
    spark.sessionState.catalog.invalidateCachedTable(qualified)
//    spark.catalog.refreshTable(newTable.identifier.quotedString) todo (lajin) 3.0 0.8 needs
    CommandUtils.updateTableStats(spark, newTable)

    removeFromMetaTable(spark, convertProperties)
    Seq.empty[Row]
  }

  private def removeFromMetaTable(
      spark: SparkSession, convertProperties: ConvertTarget): Unit = {
    convertProperties.catalogTable.foreach { table =>
      val searchCondition = DeltaTableMetadata.buildSearchCondition(
        table.identifier.database.getOrElse(""), table.identifier.table)
      val isTemp = DDLUtils.isTemporaryTable(table)
      spark.sharedState.externalCatalog.postToAll(ConvertToParquetEvent(searchCondition, isTemp))
    }
  }
}
