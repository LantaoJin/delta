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

package org.apache.spark.sql.delta.files

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RoundRobinPartitioning, UnknownPartitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

/**
 * To avoid generating too many small files, add an Repartition operator before output
 * to external storage.
 *   1. For bucket tables, do not Repartition since it should partition by bucket columns.
 *   2. For partition tables, Repartition by partition columns.
 *   3. For non-partition and non-bucket table, redistribute by round-robin.
 */
case class EnsureRepartitionForDelta(conf: SQLConf) {
  def apply(plan: SparkPlan, partitionColumns: Seq[Attribute] = Nil): SparkPlan = {
    if (!conf.getConf(SQLConf.AUTO_REPARTITION_FOR_WRITING_ENABLED)) {
      return plan
    }
    plan match {
      case i @ InsertIntoDataSourceExec(_, child, _, _, partitionCols, _) =>
        if (i.requiredChildDistribution.forall {
          case UnspecifiedDistribution => true
          case _ => false }) {
          // To be more strict, we need exclude range partition columns here, but range
          // partition has not been implemented in data source table yet, won't check here.
          val newChild = ensureRepartition(partitionCols, child)
          i.withNewChildren(newChild :: Nil)
        } else {
          // insert into bucketed delta table
          i
        }
      case p =>
        ensureRepartition(partitionColumns, p)
    }
  }

  private def ensureRepartition(partitionCols: Seq[Attribute], child: SparkPlan): SparkPlan = {
    if (partitionCols.nonEmpty) {
      child.outputPartitioning match {
        case HashPartitioning(exs, _) if isSubSet(exs, partitionCols) =>
          child
        case _ =>
          // To avoid writing to too many partitions from a single task, also to avoid
          // CoalescedShuffleReaderExec as possible, which may probably result in too
          // many small files output in a single task, use numShufflePartitions instead
          // of maxNumPostShufflePartitions here
          ShuffleExchangeExec(HashPartitioning(partitionCols,
            conf.numShufflePartitions), child)
      }
    } else {
      child.outputPartitioning match {
        // For operator like CoalesceExec, the outputPartitioning is UnknownPartitioning(a),
        // and a > 0, we should not add shuffle.
        case UnknownPartitioning(a) if a == 0 =>
          ShuffleExchangeExec(
            RoundRobinPartitioning(conf.maxNumPostShufflePartitions), child)
        case _ => child
      }
    }
  }

  private def isSubSet(exs: Seq[Expression], that: Seq[Expression]): Boolean =
    exs.forall(e => that.exists(_.semanticEquals(e)))
}

