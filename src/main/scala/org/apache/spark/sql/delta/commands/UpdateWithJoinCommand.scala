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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.util.{AnalysisHelper, SetAccumulator}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._

/**
 * Performs an update join with a source query/table.
 *
 * Issues an error message when the WHERE search_condition of the UPDATE statement can match
 * a single row from the target table with multiple rows of the source table-reference.
 *
 * Algorithm:
 *
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 *    the condition and verify that no two source rows match with the same target row.
 *    This is implemented as an inner-join using the given condition. See [[findTouchedFiles]]
 *    for more details.
 *
 * Phase 2: Read the touched files again and write new files with updated rows.
 *
 * Phase 3: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source            Source data to join with
 * @param target            Target table to update into
 * @param targetFileIndex   TahoeFileIndex of the target table
 * @param condition         Condition for a source row to match with a target row
 * @param updateClause      Info related to matched clauses.
 */
case class UpdateWithJoinCommand(
    @transient source: LogicalPlan,
    @transient target: LogicalPlan,
    @transient targetFileIndex: TahoeFileIndex,
    condition: Expression,
    updateClause: MergeIntoUpdateClause) extends RunnableCommand
  with DeltaCommand with PredicateHelper with AnalysisHelper {

  import SQLMetrics._
  import MergeIntoCommand._

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient private lazy val targetDeltaLog: DeltaLog = targetFileIndex.deltaLog

  override lazy val metrics = Map[String, SQLMetric](
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numRowsCopied" -> createMetric(sc, "number of target rows rewritten unmodified"),
    "numRowsUpdated" -> createMetric(sc, "number of updated rows"),
    "numFilesBeforeSkipping" -> createMetric(sc, "number of target files before skipping"),
    "numFilesAfterSkipping" -> createMetric(sc, "number of target files after skipping"),
    "numFilesRemoved" -> createMetric(sc, "number of files removed to target"),
    "numFilesAdded" -> createMetric(sc, "number of files added to target"))

  override def run(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.update") {
      targetDeltaLog.withNewTransaction { deltaTxn =>
        val deltaActions = {
          val filesToRewrite =
            recordDeltaOperation(targetDeltaLog, "delta.dml.update.findTouchedFiles") {
              findTouchedFiles(spark, deltaTxn)
            }

          val newWrittenFiles = writeAllChanges(spark, deltaTxn, filesToRewrite)
          filesToRewrite.map(_.remove) ++ newWrittenFiles
        }
        deltaTxn.commit(
          deltaActions,
          DeltaOperations.Update(
            Option(condition.sql)))

        // Record metrics
        val stats = UpdateStats(
          // Expressions
          conditionExpr = condition.sql,
          updateConditionExpr = updateClause.condition.map(_.sql).orNull,
          updateExprs = updateClause.actions.map(_.sql).toArray,

          // Data sizes of source and target at different stages of processing
          UpdateDataRows(metrics("numSourceRows").value),
          beforeSkipping =
            UpdateDataFiles(metrics("numFilesBeforeSkipping").value),
          afterSkipping =
            UpdateDataFiles(metrics("numFilesAfterSkipping").value),

          // Data change sizes
          filesAdded = metrics("numFilesAdded").value,
          filesRemoved = metrics("numFilesRemoved").value,
          rowsCopied = metrics("numRowsCopied").value,
          rowsUpdated = metrics("numRowsUpdated").value)
        recordDeltaEvent(targetFileIndex.deltaLog, "delta.dml.update.stats", data = stats)

        // This is needed to make the SQL metrics visible in the Spark UI
        val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(spark.sparkContext, executionId, metrics.values.toSeq)
      }
      spark.sharedState.cacheManager.recacheByPlan(spark, target)
      Seq.empty
    }
  }

  /**
   * Find the target table files that contain the rows that satisfy the update condition. This is
   * implemented as an inner-join between the source query/table and the target table using
   * the update condition.
   */
  private def findTouchedFiles(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction
  ): Seq[AddFile] = {

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()
    spark.sparkContext.register(touchedFilesAccum, "UpdateWithJoin.touchedFiles")

    // UDFs to records touched files names and add them to the accumulator
    val recordTouchedFileName = udf { (fileName: String) => {
      touchedFilesAccum.add(fileName)
      1
    }}.asNondeterministic()

    // Skip data based on the update condition
    val targetOnlyPredicates =
      splitConjunctivePredicates(condition).filter(_.references.subsetOf(target.outputSet))
    val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

    // Apply inner join to between source and target using the update condition to find matches
    // In addition, we attach two columns
    // - a monotonically increasing row id for target rows to later identify whether the same
    //     target row is modified by multiple user or not
    // - the target file name the row is from to later identify the files touched by matched rows
    val joinToFindTouchedFiles = {
      val sourceDF = Dataset.ofRows(spark, source)
      val targetDF = Dataset.ofRows(spark, buildTargetPlanWithFiles(deltaTxn, dataSkippedFiles))
        .withColumn(ROW_ID_COL, monotonically_increasing_id())
        .withColumn(FILE_NAME_COL, input_file_name())
      sourceDF.join(targetDF, new Column(condition), "inner")
    }

    // Process the matches from the inner join to record touched files and find multiple matches
    val collectTouchedFiles = joinToFindTouchedFiles
      .select(col(ROW_ID_COL), recordTouchedFileName(col(FILE_NAME_COL)).as("one"))

    // Calculate frequency of matches per source row
    val matchedRowCounts = collectTouchedFiles.groupBy(ROW_ID_COL).agg(sum("one").as("count"))
    if (matchedRowCounts.filter("count > 1").count() != 0) {
      throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
    }

    // Get the AddFiles using the touched file names.
    val touchedFileNames = touchedFilesAccum.value.iterator().asScala.toSeq
    logTrace(s"findTouchedFiles: matched files:\n\t${touchedFileNames.mkString("\n\t")}")

    val nameToAddFileMap = generateCandidateFileMap(targetDeltaLog.dataPath, dataSkippedFiles)
    val touchedAddFiles = touchedFileNames.filter(_.nonEmpty).map(f =>
      getTouchedFile(targetDeltaLog.dataPath, f, nameToAddFileMap))

    metrics("numFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
    metrics("numFilesAfterSkipping") += dataSkippedFiles.size
    metrics("numFilesRemoved") += touchedAddFiles.size
    touchedAddFiles
  }

  /**
   * Write new files by reading the touched files and updating/inserting data using the source
   * query/table. This is implemented using a full-outer-join using the update condition.
   */
  private def writeAllChanges(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction,
    filesToRewrite: Seq[AddFile]
  ): Seq[AddFile] = {

    // Generate a new logical plan that has same output attributes exprIds as the target plan.
    // This allows us to apply the existing resolved update/insert expressions.
    val newTarget = buildTargetPlanWithFiles(deltaTxn, filesToRewrite)

    logDebug(s"""writeAllChanges using full outer join:
                |  source.output: ${source.outputSet}
                |  target.output: ${target.outputSet}
                |  condition: $condition
                |  newTarget.output: ${newTarget.outputSet}
       """.stripMargin)

    // UDFs to update metrics
    val incrSourceRowCountExpr = makeMetricUpdateUDF("numSourceRows")
    val incrUpdatedCountExpr = makeMetricUpdateUDF("numRowsUpdated")
    val incrNoopCountExpr = makeMetricUpdateUDF("numRowsCopied")

    // Apply left outer join to find matches . We are adding two boolean fields
    // with value `true`, one to each side of the join. Whether this field is null or not after
    // the full outer join, will allow us to identify whether the resultanet joined row was a
    // matched inner result or an unmatched result with null on one side.
    val joinedDF = {
      val sourceDF = Dataset.ofRows(spark, source)
        .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))
      val targetDF = Dataset.ofRows(spark, newTarget)
        .withColumn(TARGET_ROW_PRESENT_COL, lit(true))
      targetDF.join(sourceDF, new Column(condition), "left")
    }

    val joinedPlan = joinedDF.queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      exprs.map { expr => tryResolveReferences(spark)(expr, joinedPlan) }
    }

    def updateOutput(u: MergeIntoUpdateClause): Seq[Expression] = {
      // Generate update expressions and set ROW_DELETED_COL = false
      val exprs = u.resolvedActions.map(_.expr) :+ Literal(false) :+ incrUpdatedCountExpr
      resolveOnJoinedPlan(exprs)
    }

    def clauseCondition(clause: Option[MergeIntoClause]): Option[Expression] = {
      val condExprOption = clause.map(_.condition.getOrElse(Literal(true)))
      resolveOnJoinedPlan(condExprOption.toSeq).headOption
    }

    val joinedRowEncoder = RowEncoder(joinedPlan.schema)
    val outputRowEncoder = RowEncoder(target.schema).resolveAndBind()

    val processor = new JoinedRowProcessor(
      targetRowHasNoMatch = resolveOnJoinedPlan(Seq(col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head,
      sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(col(TARGET_ROW_PRESENT_COL).isNull.expr)).head,
      matchedCondition1 = clauseCondition(Some(updateClause)),
      matchedOutput1 = Some(updateOutput(updateClause)),
      matchedCondition2 = None,
      matchedOutput2 = None,
      notMatchedCondition = None,
      notMatchedOutput = None,
      noopCopyOutput = resolveOnJoinedPlan(target.output :+ Literal(false) :+ incrNoopCountExpr),
      deleteRowOutput = resolveOnJoinedPlan(target.output :+ Literal(true) :+ Literal(true)),
      joinedAttributes = joinedPlan.output,
      joinedRowEncoder = joinedRowEncoder,
      outputRowEncoder = outputRowEncoder)

    val outputDF =
      Dataset.ofRows(spark, joinedPlan).mapPartitions(processor.processPartition)(outputRowEncoder)
    logDebug("writeAllChanges: join output plan:\n" + outputDF.queryExecution)

    // Write to Delta
    val newFiles = deltaTxn.writeFiles(outputDF)
    metrics("numFilesAdded") += newFiles.size
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
    val plan = deltaTxn.deltaLog.createDataFrame(deltaTxn.snapshot, files).queryExecution.analyzed

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
  private def makeMetricUpdateUDF(name: String): Expression = {
    // only capture the needed metric in a local variable
    val metric = metrics(name)
    udf { () => { metric += 1; true }}.asNondeterministic().apply().expr
  }

  private def seqToString(exprs: Seq[Expression]): String = exprs.map(_.sql).mkString("\n\t")
}

case class UpdateDataRows(rows: Long)
case class UpdateDataFiles(files: Long)

case class UpdateStats(
    // Expressions used in UPDATE
    conditionExpr: String,
    updateConditionExpr: String,
    updateExprs: Array[String],

    // Data sizes of source and target at different stages of processing
    source: UpdateDataRows,
    beforeSkipping: UpdateDataFiles,
    afterSkipping: UpdateDataFiles,

    // Data change sizes
    filesRemoved: Long,
    filesAdded: Long,
    rowsCopied: Long,
    rowsUpdated: Long)
