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

package io.delta.sql.analysis

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue, caseInsensitiveResolution, withPosition}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Cast, CurrentDate, CurrentTimestamp, EqualTo, Expression, ExtractValue, IsNotNull, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.delta.DeltaErrors

class DeltaSqlResolution(spark: SparkSession) extends Rule[LogicalPlan] {

  private def resolver = spark.sessionState.conf.resolver

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UpdateTableStatement(table, assignments, condition, None)
        if !u.resolved && table.resolved =>
      checkTargetTable(table)
      val resolvedAssignments = resolveAssignments(assignments, u)
      val columns = resolvedAssignments.map(_.key.asInstanceOf[Attribute])
      val values = resolvedAssignments.map(_.value)
      val resolvedUpdateCondition = condition.map(resolveExpressionTopDown(_, u))
      UpdateTable(table, columns, values, resolvedUpdateCondition)

    case u @ UpdateTableStatement(target, assignments, condition, Some(source))
        if !u.resolved && target.resolved && source.resolved &&
          target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
      checkTargetTable(target)
      val resolvedAssignments = resolveAssignments(assignments, u)
      val columns = resolvedAssignments.map(_.key.asInstanceOf[Attribute])
      val values = resolvedAssignments.map(_.value)
      val resolvedUpdateCondition =
        condition.map(resolveExpressionTopDown(_, u)).flatMap(inferredConditions)

      if (resolvedUpdateCondition.isEmpty && values.forall(_.foldable)) {
        // If join condition is empty and SET expressions are all foldable,
        // skip join and fallback to simple update
        logInfo(s"Update conditions are empty with foldable SET clause, fallback to simple update")
        UpdateTable(target, columns, values, resolvedUpdateCondition)
      } else {
        val updateActions = UpdateTable.toActionFromAssignments(resolvedAssignments)
        val actions = DeltaMergeIntoUpdateClause(resolvedUpdateCondition, updateActions)
        source match {
          case _: Join =>
            // if the source contains Join, we should fill the join criteria
            val withFilter = extractPredicatesOnlyInSource(source, resolvedUpdateCondition.get)
              .map(Filter(_, source)).getOrElse(source)
            val withProject =
              projectionForSource(withFilter, resolvedUpdateCondition, columns, values)
            UpdateWithJoinTable(
              target, withProject, columns, values, resolvedUpdateCondition, actions)
          case _ =>
            val withProject =
              projectionForSource(source, resolvedUpdateCondition, columns, values)
            UpdateWithJoinTable(
              target, withProject, columns, values, resolvedUpdateCondition, actions)
        }
      }

    case d @ DeleteFromStatement(table, condition, None)
        if !d.resolved && table.resolved =>
      checkTargetTable(table)
      Delete(table, condition)

    case d @ DeleteFromStatement(target, condition, Some(source))
        if !d.resolved && target.resolved && source.resolved &&
          target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
      checkTargetTable(target)
      val resolvedDeleteCondition =
        condition.map(resolveExpressionTopDown(_, d)).flatMap(inferredConditions)
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

//    case m @ MergeIntoTableStatement(targetTable, sourceTable,
//        mergeCondition, matchedActions, notMatchedActions)
//        if !m.resolved && targetTable.resolved && sourceTable.resolved &&
//          target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
//      val resolvedMergeCondition = resolveExpression(mergeCondition, m)
//      val newMatchedActions = matchedActions.collect {
//        case DeleteAction(deleteCondition) =>
//          val resolvedDeleteCondition = deleteCondition.map(resolveExpression(_, m))
//          MergeIntoDeleteClause(resolvedDeleteCondition)
//        case UpdateAction(updateCondition, assignments) =>
//          val resolvedUpdateCondition = updateCondition.map(resolveExpression(_, m))
//          val resolvedAssignments = resolveAssignments(assignments, m)
//          val updateActions = MergeIntoClause.toActionFromAssignments(resolvedAssignments)
//          MergeIntoUpdateClause(resolvedUpdateCondition, updateActions)
//      }
//      val newNotMatchedActions = notMatchedActions.collectFirst {
//        case InsertAction(insertCondition, assignments) =>
//          val resolvedInsertCondition = insertCondition.map(resolveExpression(_, m))
//          val resolvedAssignments = resolveAssignments(assignments, m)
//          val insertActions = MergeIntoClause.toActionFromAssignments(resolvedAssignments)
//          MergeIntoInsertClause(resolvedInsertCondition, insertActions)
//      }
//      MergeInto(targetTable, sourceTable,
//        resolvedMergeCondition, newMatchedActions, newNotMatchedActions)
  }

  private def resolveAssignments(
      assignments: Seq[Assignment],
      plan: LogicalPlan): Seq[Assignment] = {
    assignments.map { assign =>
      val target = plan match {
        case UpdateTableStatement(target, _, _, _) => target
        case _ => plan
      }
      val resolvedKey = assign.key match {
        case c if !c.resolved => resolveExpressionBottomUp(c, target)
        case o => o
      }
      val resolvedValue = assign.value match {
        case c if !c.resolved => resolveExpressionTopDown(Cast(c, resolvedKey.dataType), plan)
        case o => o
      }
      Assignment(resolvedKey, resolvedValue)
    }
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

  private def resolveExpressionBottomUp(
      expr: Expression,
      plan: LogicalPlan,
      throws: Boolean = false): Expression = {
    if (expr.resolved) return expr
    // Resolve expression in one round.
    // If throws == false or the desired attribute doesn't exist
    // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
    // Else, throw exception.
    try {
      expr transformUp {
        case GetColumnByOrdinal(ordinal, _) => plan.output(ordinal)
        case u @ UnresolvedAttribute(nameParts) =>
          val result =
            withPosition(u) {
              plan.resolve(nameParts, resolver)
                .orElse(resolveLiteralFunction(nameParts, u, plan))
                .getOrElse(u)
            }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldName) if child.resolved =>
          ExtractValue(child, fieldName, resolver)
      }
    } catch {
      case a: AnalysisException if !throws => expr
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
  private def inferredConditions(condition: Expression): Option[Expression] = {
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
    inferredConstraints.reduceLeftOption(And)
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
      destination: Expression): Set[Expression] = constraints.map(_ transform {
    case e: Expression if e.semanticEquals(source) => destination
  })
}
