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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue, caseInsensitiveResolution, withPosition}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Analysis rules for Delta. Currently, these rules enable schema enforcement / evolution with
 * INSERT INTO.
 */
class DeltaAnalysis(session: SparkSession, conf: SQLConf)
  extends Rule[LogicalPlan] with AnalysisHelper with DeltaLogging {

  private def resolver = session.sessionState.conf.resolver

  type CastFunction = (Expression, DataType) => Expression

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // INSERT INTO by ordinal
    case a @ AppendData(DataSourceV2Relation(d: DeltaTableV2, _, _, _, _), query, _, false)
      if query.resolved && needsSchemaAdjustment(d.name(), query, d.schema()) =>
      val projection = normalizeQueryColumns(query, d)
      if (projection != query) {
        a.copy(query = projection)
      } else {
        a
      }

    // INSERT OVERWRITE by ordinal
    case o @ OverwriteByExpression(
    DataSourceV2Relation(d: DeltaTableV2, _, _, _, _), deleteExpr, query, _, false)
      if query.resolved && needsSchemaAdjustment(d.name(), query, d.schema()) =>
      val projection = normalizeQueryColumns(query, d)
      if (projection != query) {
        val aliases = AttributeMap(query.output.zip(projection.output).collect {
          case (l: AttributeReference, r: AttributeReference) if !l.sameRef(r) => (l, r)
        })
        val newDeleteExpr = deleteExpr.transformUp {
          case a: AttributeReference => aliases.getOrElse(a, a)
        }
        o.copy(deleteExpr = newDeleteExpr, query = projection)
      } else {
        o
      }

    // Pull out the partition filter that may be part of the FileIndex. This can happen when someone
    // queries a Delta table such as spark.read.format("delta").load("/some/table/partition=2")
    case l @ DeltaTable(index: TahoeLogFileIndex) if index.partitionFilters.nonEmpty =>
      Filter(
        index.partitionFilters.reduce(And),
        DeltaTableUtils.replaceFileIndex(l, index.copy(partitionFilters = Nil), None))


    // This rule falls back to V1 nodes, since we don't have a V2 reader for Delta right now
    case dsv2 @ DataSourceV2Relation(d: DeltaTableV2, _, _, _, _) =>
      DeltaRelation.fromV2Relation(d, dsv2)

    // DML - TODO: Remove these Delta-specific DML logical plans and use Spark's plans directly

    case d @ DeleteFromTable(table, condition, None) if d.childrenResolved =>
      // rewrites Delta from V2 to V1
      val newTarget = table.transformUp { case DeltaRelation(lr) => lr }
      val indices = newTarget.collect {
        case DeltaFullTable(index) => index
      }
      if (indices.isEmpty) {
        // Not a Delta table at all, do not transform
        d
      } else if (indices.size == 1 && indices(0).deltaLog.snapshot.version > -1) {
        // It is a well-defined Delta table with a schema
        DeltaDelete(newTarget, condition)
      } else {
        // Not a well-defined Delta table
        throw DeltaErrors.notADeltaSourceException("DELETE", Some(d))
      }

    case d @ DeleteFromTable(target, condition, Some(source)) if d.childrenResolved &&
        target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
      // rewrites Delta from V2 to V1
      val newTarget = target.transformUp { case DeltaRelation(lr) => lr }
      val indices = newTarget.collect {
        case DeltaFullTable(index) => index
      }
      if (indices.isEmpty) {
        // Not a Delta table at all, do not transform
        d
      } else if (indices.size == 1 && indices(0).deltaLog.snapshot.version > -1) {
        // It is a well-defined Delta table with a schema

        val resolvedDeleteCondition =
          condition.map(resolveExpressionTopDown(_, d)).flatMap(inferredConditions)
        val actions = DeltaMergeIntoDeleteClause(resolvedDeleteCondition)
        source match {
          case _: Join =>
            // if the source contains Join, we should fill the join criteria
            val withFilter = extractPredicatesOnlyInSource(source, resolvedDeleteCondition.get)
              .map(Filter(_, source)).getOrElse(source)
            val withProject = projectionForSource(withFilter, resolvedDeleteCondition)
            DeleteWithJoinTable(newTarget, withProject, resolvedDeleteCondition, actions)
          case _ =>
            val withProject = projectionForSource(source, resolvedDeleteCondition)
            DeleteWithJoinTable(newTarget, withProject, resolvedDeleteCondition, actions)
        }
      } else {
        // Not a well-defined Delta table
        throw DeltaErrors.notADeltaSourceException("DELETE", Some(d))
      }

    case u @ UpdateTable(table, assignments, condition, None) if u.childrenResolved =>
      val (cols, expressions) = assignments.map(a =>
        a.key.asInstanceOf[NamedExpression] -> a.value).unzip
      // rewrites Delta from V2 to V1
      val newTable = table.transformUp { case DeltaRelation(lr) => lr }
        newTable.collectLeaves().headOption match {
          case Some(DeltaFullTable(index)) =>
          case o =>
            throw DeltaErrors.notADeltaSourceException("UPDATE", o)
        }
      DeltaUpdateTable(newTable, cols, expressions, condition)

    case u @ UpdateTable(target, assignments, condition, Some(source)) if u.childrenResolved &&
        target.outputSet.intersect(source.outputSet).isEmpty =>
      // rewrites Delta from V2 to V1
      val newTarget = target.transformUp { case DeltaRelation(lr) => lr }
      newTarget.collectLeaves().headOption match {
        case Some(DeltaFullTable(index)) =>
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", o)
      }
      val resolvedAssignments = resolveAssignments(assignments, u)
      val columns = resolvedAssignments.map(_.key.asInstanceOf[NamedExpression])
      val values = resolvedAssignments.map(_.value)
      val resolvedUpdateCondition =
        condition.map(resolveExpressionTopDown(_, u)).flatMap(inferredConditions)

      if (resolvedUpdateCondition.isEmpty && values.forall(_.foldable)) {
        // If join condition is empty and SET expressions are all foldable,
        // skip join and fallback to simple update
        logInfo(s"Update conditions are empty with foldable SET clause, fallback to simple update")
        DeltaUpdateTable(newTarget, columns, values, resolvedUpdateCondition)
      } else {
        val updateActions = DeltaUpdateTable.toActionFromAssignments(resolvedAssignments)
        val actions = DeltaMergeIntoUpdateClause(resolvedUpdateCondition, updateActions)
        source match {
          case _: Join =>
            // if the source contains Join, we should fill the join criteria
            val withFilter = extractPredicatesOnlyInSource(source, resolvedUpdateCondition.get)
              .map(Filter(_, source)).getOrElse(source)
            val withProject =
              projectionForSource(withFilter, resolvedUpdateCondition, columns, values)
            UpdateWithJoinTable(
              newTarget, withProject, columns, values, resolvedUpdateCondition, actions)
          case _ =>
            val withProject =
              projectionForSource(source, resolvedUpdateCondition, columns, values)
            UpdateWithJoinTable(
              newTarget, withProject, columns, values, resolvedUpdateCondition, actions)
        }
      }

    case m@MergeIntoTable(target, source, condition, matched, notMatched) if m.childrenResolved =>
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
      // rewrites Delta from V2 to V1
      val newTarget = target.transformUp { case DeltaRelation(lr) => lr }
      // Even if we're merging into a non-Delta target, we will catch it later and throw an
      // exception.
      val deltaMerge =
        DeltaMergeInto(newTarget, source, condition, matchedActions ++ notMatchedActions)

      val deltaMergeResolved =
        DeltaMergeInto.resolveReferences(deltaMerge, conf)(tryResolveReferences(session) _)

      deltaMergeResolved

  }

  /**
   * Performs the schema adjustment by adding UpCasts (which are safe) and Aliases so that we
   * can check if the by-ordinal schema of the insert query matches our Delta table.
   */
  private def normalizeQueryColumns(query: LogicalPlan, target: DeltaTableV2): LogicalPlan = {
    val targetAttrs = target.schema()
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        val expr = (attr.dataType, targetAttr.dataType) match {
          case (s, t) if s == t =>
            attr
          case (s: StructType, t: StructType) if s != t =>
            addCastsToStructs(target.name(), attr, s, t)
          case _ =>
            getCastFunction(attr, targetAttr.dataType)
        }
        Alias(expr, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
      } else {
        attr
      }
    }
    Project(project, query)
  }

  /**
   * With Delta, we ACCEPT_ANY_SCHEMA, meaning that Spark doesn't automatically adjust the schema
   * of INSERT INTO. This allows us to perform better schema enforcement/evolution. Since Spark
   * skips this step, we see if we need to perform any schema adjustment here.
   */
  private def needsSchemaAdjustment(
      tableName: String,
      query: LogicalPlan,
      schema: StructType): Boolean = {
    val output = query.output
    if (output.length < schema.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(tableName, output.length, schema.length)
    }
    // Now we should try our best to match everything that already exists, and leave the rest
    // for schema evolution to WriteIntoDelta
    val existingSchemaOutput = output.take(schema.length)
    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
      !SchemaUtils.isReadCompatible(schema.asNullable, existingSchemaOutput.toStructType)
  }

  // Get cast operation for the level of strictness in the schema a user asked for
  private def getCastFunction: CastFunction = {
    val timeZone = conf.sessionLocalTimeZone
    conf.storeAssignmentPolicy match {
      case SQLConf.StoreAssignmentPolicy.LEGACY => Cast(_, _, Option(timeZone))
      case SQLConf.StoreAssignmentPolicy.ANSI => AnsiCast(_, _, Option(timeZone))
      case SQLConf.StoreAssignmentPolicy.STRICT => UpCast(_, _)
    }
  }

  /**
   * Recursively casts structs in case it contains null types.
   * TODO: Support other complex types like MapType and ArrayType
   */
  private def addCastsToStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType): NamedExpression = {
    if (source.length < target.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(
        tableName, source.length, target.length, Some(parent.qualifiedName))
    }
    val fields = source.zipWithIndex.map {
      case (StructField(name, nested: StructType, _, metadata), i) if i < target.length =>
        target(i).dataType match {
          case t: StructType =>
            val subField = Alias(GetStructField(parent, i, Option(name)), name)(
              explicitMetadata = Option(metadata))
            addCastsToStructs(tableName, subField, nested, t)
          case o =>
            val field = parent.qualifiedName + "." + name
            val targetName = parent.qualifiedName + "." + target(i).name
            throw DeltaErrors.cannotInsertIntoColumn(tableName, field, targetName, o.simpleString)
        }
      case (other, i) if i < target.length =>
        val targetAttr = target(i)
        Alias(
          getCastFunction(GetStructField(parent, i, Option(other.name)), targetAttr.dataType),
          targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))

      case (other, i) =>
        // This is a new column, so leave to schema evolution as is. Do not lose it's name so
        // wrap with an alias
        Alias(
          GetStructField(parent, i, Option(other.name)),
          other.name)(explicitMetadata = Option(other.metadata))
    }
    Alias(CreateStruct(fields), parent.name)(
      parent.exprId, parent.qualifier, Option(parent.metadata))
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

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
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
      case eq @ EqualTo(l @ Cast(_: Expression, _, _), r: Expression) =>
        inferredConstraints ++= replaceConstraints(predicates - eq, r, l)
      case eq @ EqualTo(l: Expression, r @ Cast(_: Expression, _, _)) =>
        inferredConstraints ++= replaceConstraints(predicates - eq, l, r)
      case eq @ EqualTo(l: Expression, r: Expression) =>
        val candidateConstraints = predicates - eq
        inferredConstraints ++= replaceConstraints(candidateConstraints, l, r)
        inferredConstraints ++= replaceConstraints(candidateConstraints, r, l)
      case _ => // No inference
    }
    // For easy writing unit test, we sort by simpleString
    inferredConstraints.toSeq.sortBy(_.simpleString(SQLConf.get.maxToStringFields))
      .reduceLeftOption(And)
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

  private def resolveAssignments(
    assignments: Seq[Assignment],
    plan: LogicalPlan): Seq[Assignment] = {
    assignments.map { assign =>
      val target = plan match {
        case UpdateTable(target, _, _, _) => target
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
}

/** Matchers for dealing with a Delta table. */
object DeltaRelation {
  def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {
    case dsv2 @ DataSourceV2Relation(d: DeltaTableV2, _, _, _, _) => Some(fromV2Relation(d, dsv2))
    case lr @ DeltaTable(_) => Some(lr)
    case _ => None
  }

  def fromV2Relation(d: DeltaTableV2, v2Relation: DataSourceV2Relation): LogicalRelation = {
    val relation = d.toBaseRelation
    LogicalRelation(relation, v2Relation.output, d.catalogTable, isStreaming = false)
  }
}

