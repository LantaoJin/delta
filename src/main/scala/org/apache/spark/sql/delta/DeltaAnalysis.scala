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

package org.apache.spark.sql.delta

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue, caseInsensitiveResolution, withPosition}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Cast, CurrentDate, CurrentTimestamp, EqualTo, Expression, ExtractValue, IsNotNull, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{AnalysisException, SparkSession}

class DeltaAnalysis(spark: SparkSession, conf: SQLConf)
  extends Rule[LogicalPlan] with AnalysisHelper with DeltaLogging {

  private def resolver = spark.sessionState.conf.resolver

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    case u @ UpdateTableStatement(table, assignments, condition, None) if u.resolved =>
      checkTargetTable(table)
      val (columns, values) = assignments.map(a =>
        a.key.asInstanceOf[NamedExpression] -> a.value).unzip
      values.foreach { e =>
        if (SubqueryExpression.hasCorrelatedSubquery(e)) {
          throw DeltaErrors.correlatedSubqueryNotSupportedException("UpdateTable", e)
        }
      }
      val resolvedUpdateCondition = condition.map(resolveExpressionTopDown(_, u))
      DeltaUpdateTable(table, columns, values, resolvedUpdateCondition)

    case u @ UpdateTableStatement(target, assignments, condition, Some(source)) if u.resolved &&
        target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
      val (columns, values) = assignments.map(a =>
        a.key.asInstanceOf[NamedExpression] -> a.value).unzip
      values.foreach { e =>
        if (SubqueryExpression.hasCorrelatedSubquery(e)) {
          throw DeltaErrors.correlatedSubqueryNotSupportedException("UpdateWithJoinTable", e)
        }
      }
      val inferredCondition = condition.flatMap(inferConditions)
      if (inferredCondition.isEmpty && values.forall(_.foldable)) {
        // If join condition is empty and SET expressions are all foldable,
        // skip join and fallback to simple update
        logInfo(s"Update conditions are empty with foldable SET clause, fallback to simple update")
        DeltaUpdateTable(target, columns, values, inferredCondition)
      } else {
        val updateActions = DeltaUpdateTable.toActionFromAssignments(assignments)
        val actions = DeltaMergeIntoUpdateClause(inferredCondition, updateActions)
        source match {
          case _: Join =>
            // if the source contains Join, we should fill the join criteria
            val withFilter = extractPredicatesOnlyInSource(source, inferredCondition.get)
              .map(Filter(_, source)).getOrElse(source)
            val withProject =
              projectionForSource(withFilter, inferredCondition, columns, values)
            UpdateWithJoinTable(
              target, withProject, columns, values, inferredCondition, actions)
          case _ =>
            val withProject =
              projectionForSource(source, inferredCondition, columns, values)
            UpdateWithJoinTable(
              target, withProject, columns, values, inferredCondition, actions)
        }
      }

    case d @ DeleteFromStatement(table, condition, None) if d.resolved =>
      checkTargetTable(table)
      DeltaDelete(table, condition)

    case d @ DeleteFromStatement(target, condition, Some(source)) if d.resolved &&
        target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
      checkTargetTable(target)
      val resolvedDeleteCondition =
        condition.map(resolveExpressionTopDown(_, d)).flatMap(inferConditions)
      val actions = DeltaMergeIntoDeleteClause(resolvedDeleteCondition)
      source match {
        case _: Join =>
          // if the source contains Join, we should fill the join criteria
          val withFilter = extractPredicatesOnlyInSource(source, resolvedDeleteCondition.get)
            .map(Filter(_, source)).getOrElse(source)
          val withProject = projectionForSource(withFilter, resolvedDeleteCondition)
          DeleteWithJoinTable(target, withProject, resolvedDeleteCondition, actions)
        case _ =>
          val withProject = projectionForSource(source, resolvedDeleteCondition)
          DeleteWithJoinTable(target, withProject, resolvedDeleteCondition, actions)
      }

    case m @ MergeIntoTableStatement(target, source, condition, matched, notMatched)
        if m.resolved =>
      checkTargetTable(target)
      val matchedActions = matched.map {
        case update: UpdateAction =>
          DeltaMergeIntoUpdateClause(
            update.condition,
            DeltaMergeIntoClause.toActions(update.assignments))
        case delete: DeleteAction =>
          DeltaMergeIntoDeleteClause(delete.condition)
        case insert =>
          throw new AnalysisException(
            "Insert clauses cannot be part of the WHEN MATCHED clause in MERGE INTO.")
      }
      val notMatchedActions = notMatched.map {
        case insert: InsertAction =>
          DeltaMergeIntoInsertClause(
            insert.condition,
            DeltaMergeIntoClause.toActions(insert.assignments))
        case other =>
          throw new AnalysisException(s"${other.prettyName} clauses cannot be part of the " +
            s"WHEN NOT MATCHED clause in MERGE INTO.")
      }
      // Even if we're merging into a non-Delta target, we will catch it later and throw an
      // exception.
      val deltaMerge =
        DeltaMergeInto(target, source, condition, matchedActions ++ notMatchedActions)

      val deltaMergeResolved =
        DeltaMergeInto.resolveReferences(deltaMerge, conf)(tryResolveReferences(spark) _)

      deltaMergeResolved
  }

  private def resolveExpressionTopDown(e: Expression, q: LogicalPlan): Expression = {
    if (e.resolved) return e
    e match {
//      case f: LambdaFunction if !f.bound => f
      case u @ UnresolvedAttribute(nameParts) =>
        // Leave unchanged if resolution fails. Hopefully will be resolved next round.
        val result =
          withPosition(u) {
            q.resolveChildren(nameParts, resolver)
              .orElse(resolveLiteralFunction(nameParts, u, q))
              .getOrElse(u)
          }
        logDebug(s"Resolving $u to $result")
        result
      case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
        ExtractValue(child, fieldExpr, resolver)
      case _ => e.mapChildren(resolveExpressionTopDown(_, q))
    }
  }

  /**
   * Literal functions do not require the user to specify braces when calling them
   * When an attributes is not resolvable, we try to resolve it as a literal function.
   */
  private def resolveLiteralFunction(
      nameParts: Seq[String],
      attribute: UnresolvedAttribute,
      plan: LogicalPlan): Option[Expression] = {
    if (nameParts.length != 1) return None
    val isNamedExpression = plan match {
      case Aggregate(_, aggregateExpressions, _) => aggregateExpressions.contains(attribute)
      case Project(projectList, _) => projectList.contains(attribute)
      case Window(windowExpressions, _, _, _) => windowExpressions.contains(attribute)
      case _ => false
    }
    val wrapper: Expression => Expression =
      if (isNamedExpression) f => Alias(f, toPrettySQL(f))() else identity
    // support CURRENT_DATE and CURRENT_TIMESTAMP
    val literalFunctions = Seq(CurrentDate(), CurrentTimestamp())
    val name = nameParts.head
    val func = literalFunctions.find(e => caseInsensitiveResolution(e.prettyName, name))
    func.map(wrapper)
  }

  private def checkTargetTable(targetTable: LogicalPlan): Unit = {
    targetTable.collect {
      case View(t, _, _) =>
        throw DeltaErrors.cannotUpdateAViewException(t.identifier)
    }
  }

  /**
   * Join criteria may cross target and source, this method extracts the join criteria
   * and conditions which only related to source.
   *
   * For example:
   * In below query, t1 is target, t2 is source.
   * This method return (t2.id < 100).
   *   Update t1
   *   FROM t1, t2
   *   WHERE t1.id = t2.id and t2.id < 100
   *
   * In below query, t1 is target, t2 join t3 is source.
   * This method return (t2.id = t3.id), (t2.id < 100).
   *   Update t1
   *   FROM t2, t3, t1
   *   WHERE t1.id = t2.id and t2.id = t3.id and t2.id < 100
   *
   * This method will infer and return (t2.id = t3.id), (t2.id < 100).
   *   Update t1
   *   FROM t2, t3, t1
   *   WHERE t1.id = t2.id and t1.id = t3.id and t2.id < 100
   */
  private def extractPredicatesOnlyInSource(
      source: LogicalPlan, predicates: Expression): Option[Expression] = {
    val (predicatesInSourceOnly, predicatesContainsNonSource) =
      splitConjunctivePredicates(predicates).partition {
        case expr: Expression => expr.references.subsetOf(source.outputSet)
        case _ => false
      }
    predicatesInSourceOnly.reduceLeftOption(And)
  }

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * constraints (`a = 5`, `a = b`, `b = 5`)
   */
  private def inferConditions(condition: Expression): Option[Expression] = {
    val constraints = splitConjunctivePredicates(condition).toSet
    var inferredConstraints = constraints
    // IsNotNull should be constructed by `constructIsNotNullConstraints`.
    val predicates = constraints.filterNot(_.isInstanceOf[IsNotNull])
    predicates.foreach {
      case eq @ EqualTo(l @ Cast(_: Expression, _, _, _), r: Expression) =>
        inferredConstraints ++= replaceConstraints(predicates - eq, r, l)
      case eq @ EqualTo(l: Expression, r @ Cast(_: Expression, _, _, _)) =>
        inferredConstraints ++= replaceConstraints(predicates - eq, l, r)
      case eq @ EqualTo(l: Expression, r: Expression) =>
        val candidateConstraints = predicates - eq
        inferredConstraints ++= replaceConstraints(candidateConstraints, l, r)
        inferredConstraints ++= replaceConstraints(candidateConstraints, r, l)
      case _ => // No inference
    }
    // For easy writing unit test, we sort by simpleString
    inferredConstraints.toSeq.sortBy(_.simpleString).reduceLeftOption(And)
  }

  private def projectionForSource(
      source: LogicalPlan,
      condition: Option[Expression],
      columns: Seq[NamedExpression] = Nil,
      values: Seq[Expression] = Nil): LogicalPlan = {
    val attributesInCondition =
      condition.map(splitConjunctivePredicates(_).flatMap(_.references.toSeq)).toSet.flatten
    val attributesInValues =
      values.map(splitConjunctivePredicates(_).flatMap(_.references.toSeq)).toSet.flatten
    val all = attributesInCondition ++ attributesInValues ++ columns.toSet
    val attributesInSource = all.filter(source.outputSet.contains(_)).toSeq
    Project(attributesInSource, source)
  }

  private def replaceConstraints(
      constraints: Set[Expression],
      source: Expression,
      destination: Expression): Set[Expression] = {
    if ((source.resolved && source.foldable) || (destination.resolved && destination.foldable)) {
      constraints
    } else {
      constraints.map(_ transform {
        case e: Expression if e.semanticEquals(source) => destination
      })
    }
  }
}
