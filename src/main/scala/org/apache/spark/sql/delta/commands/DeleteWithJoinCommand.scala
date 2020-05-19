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

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.util.{AnalysisHelper, SetAccumulator}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType

/**
 * Performs a Delete based on a join with a source query/table.
 *
 * Algorithm:
 *   1) Scan all the files and determine which files have
 *      the rows that need to be deleted.
 *      This is implemented as an inner-join using the given condition. See [[findTouchedFiles]]
 *      for more details.
 *   2) Traverse the affected files and rebuild the touched files.
 *   3) Use the Delta protocol to atomically write the remaining rows to new files and remove
 *      the affected files that are identified in step 1.
 *
 * @param source            Source data to join with
 * @param target            Target table to delete from
 * @param targetFileIndex   TahoeFileIndex of the target table
 * @param condition         Condition for a source row to match with a target row
 * @param deleteClause      Info related to matched clauses.
 */
case class DeleteWithJoinCommand(
    @transient source: LogicalPlan,
    @transient target: LogicalPlan,
    @transient targetFileIndex: TahoeFileIndex,
    condition: Option[Expression],
    deleteClause: DeltaMergeIntoDeleteClause) extends RunnableCommand
  with DeltaCommand with PredicateHelper with AnalysisHelper {

  import SQLMetrics._
  import MergeIntoCommand._

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient private lazy val targetDeltaLog: DeltaLog = targetFileIndex.deltaLog

  private val catalogTable = getCatalogTableFromTargetPlan(target)

  override lazy val metrics = Map[String, SQLMetric](
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numRowsDeleted" -> createMetric(sc, "number of deleted rows"),
    "numFilesBeforeSkipping" -> createMetric(sc, "number of target files before skipping"),
    "numFilesAfterSkipping" -> createMetric(sc, "number of target files after skipping"),
    "numRemovedFiles" -> createMetric(sc, "number of files removed to target"),
    "numAddedFiles" -> createMetric(sc, "number of files added to target"))

  override def run(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.delete") {
      targetDeltaLog.assertRemovable()
      targetDeltaLog.withNewTransaction { deltaTxn =>
        val deltaActions = {
          val filesToRewrite =
            recordDeltaOperation(targetDeltaLog, "delta.dml.delete.findTouchedFiles") {
              findTouchedFiles(spark, deltaTxn)
            }

          val newWrittenFiles = writeAllChanges(spark, deltaTxn, filesToRewrite)
          filesToRewrite.map(_.remove) ++ newWrittenFiles
        }
        if (deltaActions.nonEmpty) {
          deltaTxn.registerSQLMetrics(spark, metrics)

          deltaTxn.commit(
            deltaActions,
            DeltaOperations.Delete(condition.map(_.sql).toSeq),
            catalogTable)
        }
        // Record metrics
        val stats = DeleteStats(
          // Expressions
          conditionExpr = condition.map(_.sql).getOrElse("true"),
          updateConditionExpr = deleteClause.condition.map(_.sql).orNull,
          updateExprs = deleteClause.actions.map(_.sql).toArray,

          // Data sizes of source and target at different stages of processing
          DeleteDataRows(metrics("numSourceRows").value),
          beforeSkipping =
            DeleteDataFiles(metrics("numFilesBeforeSkipping").value),
          afterSkipping =
            DeleteDataFiles(metrics("numFilesAfterSkipping").value),

          // Data change sizes
          filesAdded = metrics("numAddedFiles").value,
          filesRemoved = metrics("numRemovedFiles").value,
          rowsDeleted = metrics("numRowsDeleted").value)
        recordDeltaEvent(targetFileIndex.deltaLog, "delta.dml.delete.stats", data = stats)

        // This is needed to make the SQL metrics visible in the Spark UI
        val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(spark.sparkContext, executionId, metrics.values.toSeq)
      }
      spark.sharedState.cacheManager.recacheByPlan(spark, target)
      Seq.empty
    }
  }

  /**
   * Find the target table files that contain the rows that satisfy the delete condition. This is
   * implemented as an inner-join between the source query/table and the target table using
   * the delete condition.
   */
  private def findTouchedFiles(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction
  ): Seq[AddFile] = {

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()
    spark.sparkContext.register(touchedFilesAccum, "DeleteWithJoin.touchedFiles")

    // UDFs to records touched files names and add them to the accumulator
    val recordTouchedFileName = udf { (fileName: String) => {
      touchedFilesAccum.add(fileName)
      1
    }}.asNondeterministic()

    // Skip data based on the delete condition
    val joinCondition = condition.getOrElse(Literal(true, BooleanType))
    val targetOnlyPredicates =
      splitConjunctivePredicates(joinCondition).filter(_.references.subsetOf(target.outputSet))
    val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

    // Apply inner join to between source and target using the delete condition to find matches
    // In addition, we attach two columns
    // - a monotonically increasing row id for target rows to later identify whether the same
    //     target row is modified by multiple user or not
    // - the target file name the row is from to later identify the files touched by matched rows
    val sourceDF = Dataset.ofRows(spark, source)
    val targetDF = Dataset.ofRows(spark, buildTargetPlanWithFiles(deltaTxn, dataSkippedFiles))
    val targetDFWithFilterPushdown = addFilterPushdown(targetDF, targetOnlyPredicates)
      .withColumn(ROW_ID_COL, monotonically_increasing_id())
      .withColumn(FILE_NAME_COL, input_file_name())
    val joinToFindTouchedFiles = {
      sourceDF.join(targetDFWithFilterPushdown, new Column(joinCondition), "inner")
    }

    // Process the matches from the inner join to record touched files and find multiple matches
    val collectTouchedFiles = joinToFindTouchedFiles.select(
      col(ROW_ID_COL), recordTouchedFileName(col(FILE_NAME_COL)).as("one"))

    // Calculate frequency of matches per source row
    val matchedRowCounts = collectTouchedFiles.groupBy(ROW_ID_COL).agg(sum("one").as("count"))
    if (matchedRowCounts.filter("count > 1").count() != 0) {
      throw DeltaErrors.multipleSourceRowMatchingTargetRowException(spark, "DELETE")
    }

    // Get the AddFiles using the touched file names.
    val touchedFileNames = touchedFilesAccum.value.iterator().asScala.toSeq
    logTrace(s"findTouchedFiles: matched files:\n\t${touchedFileNames.mkString("\n\t")}")

    val nameToAddFileMap = generateCandidateFileMap(targetDeltaLog.dataPath, dataSkippedFiles)
    val touchedAddFiles = touchedFileNames.filter(_.nonEmpty).map(f =>
      getTouchedFile(targetDeltaLog.dataPath, f, nameToAddFileMap))

    metrics("numFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
    metrics("numFilesAfterSkipping") += dataSkippedFiles.size
    metrics("numRemovedFiles") += touchedAddFiles.size
    touchedAddFiles
  }

  /**
   * Write new files by reading the touched files and deleting data using the source
   * query/table. This is implemented using a xx-outer-join using the update condition.
   */
  private def writeAllChanges(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction,
      filesToRewrite: Seq[AddFile]
  ): Seq[AddFile] = {
    if (filesToRewrite.isEmpty) {
      return Nil
    }

    // UDFs to delete metrics
    val incrSourceRowCountExpr = makeMetricDeleteUDF("numSourceRows")
    val incrDeletedCountExpr = makeMetricDeleteUDF("numRowsDeleted")

    val fileIndex = new TahoeBatchFileIndex(
      spark, "delete", filesToRewrite, targetDeltaLog, targetFileIndex.path, deltaTxn.snapshot)
    val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex, catalogTable)

    // we also can use leftanti join to implement delete.
    // but it cannot get the metrics like 'numRowsDeleted'
//    val targetDF = Dataset.ofRows(spark, newTarget)
//    val sourceDF = Dataset.ofRows(spark, source)
//    // Apply left anti join.
//    val joinCondition = condition.getOrElse(Literal(true, BooleanType))
//    val outputDF = targetDF.join(sourceDF, new Column(joinCondition), "leftanti")

    val joinedDF = {
      val sourceDF = Dataset.ofRows(spark, source)
        .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))
      val targetDF = Dataset.ofRows(spark, newTarget)
        .withColumn(TARGET_ROW_PRESENT_COL, lit(true))
      targetDF.join(sourceDF, new Column(condition.getOrElse(Literal(true, BooleanType))), "left")
    }

    val joinedPlan = joinedDF.queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      exprs.map { expr => tryResolveReferences(spark)(expr, joinedPlan) }
    }

    def deleteOutput(u: DeltaMergeIntoDeleteClause): Seq[Expression] = {
      // Generate expressions to set the ROW_DELETED_COL = true
      val exprs = target.output :+ Literal(true) :+ incrDeletedCountExpr
      resolveOnJoinedPlan(exprs)
    }

    def clauseCondition(clause: Option[DeltaMergeIntoClause]): Option[Expression] = {
      val condExprOption = clause.map(_.condition.getOrElse(Literal(true)))
      resolveOnJoinedPlan(condExprOption.toSeq).headOption
    }

    val joinedRowEncoder = RowEncoder(joinedPlan.schema, checkOverflow = false)
    val outputRowEncoder = RowEncoder(target.schema, checkOverflow = false).resolveAndBind()

    val processor = new JoinedRowProcessor(
      targetRowHasNoMatch = resolveOnJoinedPlan(Seq(col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head,
      sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(col(TARGET_ROW_PRESENT_COL).isNull.expr)).head,
      matchedCondition1 = clauseCondition(Some(deleteClause)),
      matchedOutput1 = Option(deleteOutput(deleteClause)),
      matchedCondition2 = None,
      matchedOutput2 = None,
      notMatchedCondition = None,
      notMatchedOutput = None,
      noopCopyOutput = resolveOnJoinedPlan(target.output :+ Literal(false) :+ Literal(true)),
      deleteRowOutput = resolveOnJoinedPlan(target.output :+ Literal(true) :+ Literal(true)),
      joinedAttributes = joinedPlan.output,
      joinedRowEncoder = joinedRowEncoder,
      outputRowEncoder = outputRowEncoder)

    val outputDF = Dataset.ofRows(spark, joinedPlan)
      .mapPartitions(processor.processPartition)(outputRowEncoder)
    // Add a InsertIntoDataSource node to reuse the processing on node InsertIntoDataSource.
    val normalized = convertToInsertIntoDataSource(conf, target, outputDF.queryExecution.logical)
    val normalizedDF = Dataset.ofRows(spark, normalized)

    // Write to Delta
    val newFiles = deltaTxn.writeFiles(normalizedDF, metrics)
    metrics("numAddedFiles") += newFiles.size
    newFiles
  }


  /**
   * Build a new logical plan using the given `files` that has the same output columns (exprIds)
   * as the `target` logical plan, so that existing update/insert expressions can be applied
   * on this new plan.
   */
  private def buildTargetPlanWithFiles(
    deltaTxn: OptimisticTransaction,
    files: Seq[AddFile]): LogicalPlan = {
    val plan = deltaTxn.deltaLog.createDataFrame(deltaTxn.snapshot, files, table = catalogTable)
      .queryExecution.analyzed

    // For each plan output column, find the corresponding target output column (by name) and
    // create an alias
    val aliases = plan.output.map {
      case newAttrib: AttributeReference =>
        val existingTargetAttrib = target.output.find(_.name == newAttrib.name).getOrElse {
          throw new AnalysisException(
            s"Could not find ${newAttrib.name} among the existing target output ${target.output}")
        }.asInstanceOf[AttributeReference]
        Alias(newAttrib, existingTargetAttrib.name)(exprId = existingTargetAttrib.exprId)
    }
    Project(aliases, plan)
  }

  /** Expressions to increment SQL metrics */
  private def makeMetricDeleteUDF(name: String): Expression = {
    // only capture the needed metric in a local variable
    val metric = metrics(name)
    udf { () => { metric += 1; true }}.asNondeterministic().apply().expr
  }
}

case class DeleteDataRows(rows: Long)
case class DeleteDataFiles(files: Long)

case class DeleteStats(
    // Expressions used in UPDATE
    conditionExpr: String,
    updateConditionExpr: String,
    updateExprs: Array[String],

    // Data sizes of source and target at different stages of processing
    source: DeleteDataRows,
    beforeSkipping: DeleteDataFiles,
    afterSkipping: DeleteDataFiles,

    // Data change sizes
    filesRemoved: Long,
    filesAdded: Long,
    rowsDeleted: Long)
