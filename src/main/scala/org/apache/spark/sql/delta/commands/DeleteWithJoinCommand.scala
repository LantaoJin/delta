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

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
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

  val targetTable = getCatalogTableFromTargetPlan(target)

  override lazy val metrics = Map[String, SQLMetric](
    "numSourceRows" -> createMetric(sc, "number of source rows participated in delete"),
    "numDeletedRows" -> createMetric(sc, "number of deleted rows"),
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
              withStatusCode("DELTA", "Filtering files for cross table DELETE") {
                findTouchedFiles(spark, deltaTxn)
              }
            }

          val newWrittenFiles = withStatusCode("DELTA", "Writing for cross table DELETE") {
            writeAllChanges(spark, deltaTxn, filesToRewrite)
          }
          filesToRewrite.map(_.remove) ++ newWrittenFiles
        }
        if (deltaActions.nonEmpty) {
          deltaTxn.registerSQLMetrics(spark, metrics)

          deltaTxn.commit(
            deltaActions,
            DeltaOperations.Delete(condition.map(_.sql).toSeq),
            targetTable)
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
          rowsDeleted = metrics("numDeletedRows").value)
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
    deltaTxn: OptimisticTransaction): Seq[AddFile] = {
    import spark.implicits._

    // Skip data based on the delete condition
    val joinCondition = condition.getOrElse(Literal(true, BooleanType))
    val targetOnlyPredicates =
      splitConjunctivePredicates(joinCondition).filter(_.references.subsetOf(target.outputSet))
    val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

    val targetUsedColumns = splitConjunctivePredicates(joinCondition)
      .flatMap(_.references.intersect(target.outputSet)).map(_.name).toSet

    // Apply inner join to between source and target using the delete condition to find matches
    // In addition, we attach two columns
    // - a monotonically increasing row id for target rows to later identify whether the same
    //     target row is modified by multiple user or not
    // - the target file name the row is from to later identify the files touched by matched rows
    val sourceDF = Dataset.ofRows(spark, source)
    val targetDF = Dataset.ofRows(spark,
      buildTargetPlanWithFiles(deltaTxn, dataSkippedFiles, targetUsedColumns))
    val targetDFWithFilterPushdown = addFilterPushdown(targetDF, targetOnlyPredicates)
      .withColumn(ROW_ID_COL, monotonically_increasing_id())
      .withColumn(FILE_NAME_COL, input_file_name())
    val joinToFindTouchedFiles = {
      sourceDF.join(targetDFWithFilterPushdown, new Column(joinCondition), "inner")
    }

    // Get the AddFiles using the touched file names.
    val touchedFileNames =
      joinToFindTouchedFiles.select(col(FILE_NAME_COL)).distinct().as[String].collect()
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
    val incrDeletedCountExpr = makeMetricDeleteUDF("numDeletedRows")

    val fileIndex = new TahoeBatchFileIndex(
      spark, "delete", filesToRewrite, targetDeltaLog, targetFileIndex.path, deltaTxn.snapshot)
    val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex, targetTable)

    // we also can use leftanti join to implement delete.
    // but it cannot get the metrics like 'numDeletedRows'
    //    val targetDF = Dataset.ofRows(spark, newTarget)
    //    val sourceDF = Dataset.ofRows(spark, source)
    //    // Apply left anti join.
    //    val joinCondition = condition.getOrElse(Literal(true, BooleanType))
    //    val outputDF = targetDF.join(sourceDF, new Column(joinCondition), "leftanti")

    val joinCondition = condition.getOrElse(Literal(true, BooleanType))
    val (targetOnlyPredicates, otherPredicates) =
      splitConjunctivePredicates(joinCondition).partition { expr =>
        expr.references.subsetOf(target.outputSet)
      }

    val sourceOnlyPredicates =
      splitConjunctivePredicates(joinCondition).filter(_.references.subsetOf(source.outputSet))

    val leftJoinRewriteEnabled = spark.sessionState.conf.getConf(DeltaSQLConf.REWRITE_LEFT_JOIN)
    val sourceDF = addFilterPushdown(Dataset.ofRows(spark, source), sourceOnlyPredicates)
      .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))
    val targetDF = Dataset.ofRows(spark, newTarget)
      .withColumn(TARGET_ROW_PRESENT_COL, lit(true))

    val joinedDF = if (leftJoinRewriteEnabled && targetOnlyPredicates.nonEmpty) {
      targetDF.join(sourceDF, new Column(otherPredicates.reduceLeftOption(And)
        .getOrElse(Literal(true, BooleanType))), "left")
        .filter(new Column(targetOnlyPredicates.reduceLeftOption(And)
          .getOrElse(Literal(true, BooleanType))))
    } else {
      targetDF.join(sourceDF, new Column(joinCondition), "left")
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

    def clauseCondition(clause: DeltaMergeIntoClause): Expression = {
      // if condition is None, then expression always evaluates to true
      val condExpr = clause.condition.getOrElse(Literal(true))
      resolveOnJoinedPlan(Seq(condExpr)).head
    }

    val joinedRowEncoder = RowEncoder(joinedPlan.schema, checkOverflow = false)
    val outputRowEncoder = RowEncoder(target.schema, checkOverflow = false).resolveAndBind()

    val processor = new JoinedRowProcessor(
      targetRowHasNoMatch = resolveOnJoinedPlan(Seq(col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head,
      sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(col(TARGET_ROW_PRESENT_COL).isNull.expr)).head,
      matchedConditions = Seq(clauseCondition(deleteClause)),
      matchedOutputs = Seq(deleteOutput(deleteClause)),
      notMatchedConditions = Nil,
      notMatchedOutputs = Nil,
      noopCopyOutput = resolveOnJoinedPlan(target.output :+ Literal(false) :+ Literal(true)),
      deleteRowOutput = resolveOnJoinedPlan(target.output :+ Literal(true) :+ Literal(true)),
      joinedAttributes = joinedPlan.output,
      joinedRowEncoder = joinedRowEncoder,
      outputRowEncoder = outputRowEncoder)

    val outputDF = Dataset.ofRows(spark, joinedPlan)
      .mapPartitions(processor.processPartition)(outputRowEncoder)

    val unionDF = if (leftJoinRewriteEnabled && targetOnlyPredicates.nonEmpty) {
      val targetDF = Dataset.ofRows(spark, newTarget)
      outputDF.union(targetDF.filter(new Column(
        Not(EqualNullSafe(targetOnlyPredicates.reduceLeft(And), Literal(true, BooleanType))))))
    } else {
      outputDF
    }

    // Add a InsertIntoDataSource node to reuse the processing on node InsertIntoDataSource.
    val normalized = convertToInsertIntoDataSource(deltaTxn.metadata, conf, unionDF)
    val normalizedDF = Dataset.ofRows(spark, normalized)
    logInfo("writeAllChanges: join output plan:\n" + normalizedDF.queryExecution)

    // Write to Delta
    val options = new DeltaOptions(Map.empty[String, String], spark.sessionState.conf, metrics)
    val newFiles = deltaTxn.writeFiles(normalizedDF, Some(options))
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
    files: Seq[AddFile],
    targetUsedColumns: Set[String] = Set.empty): LogicalPlan = {
    val plan = deltaTxn.deltaLog.createDataFrame(deltaTxn.snapshot, files,
      table = targetTable, projectedColumns = targetUsedColumns).queryExecution.analyzed

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
