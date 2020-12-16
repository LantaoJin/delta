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
import org.apache.spark.sql.catalyst.expressions.SubExprUtils.{containsOuter, stripOuterReferences}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BooleanSimplification
import org.apache.spark.sql.catalyst.plans.LeftAnti
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
  extends Rule[LogicalPlan] with AnalysisHelper with PredicateHelper
    with ConstraintHelper with DeltaLogging {

  private def resolver = session.sessionState.conf.resolver

  type CastFunction = (Expression, DataType) => Expression

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // INSERT INTO by ordinal
    case a @ AppendData(DataSourceV2Relation(d: DeltaTableV2, _, _, _, _), query,
        writeOptions, false)
      if query.resolved && needsSchemaAdjustment(d.name(), query, d.schema(), writeOptions) =>
      val projection = normalizeQueryColumns(query, d)
      if (projection != query) {
        a.copy(query = projection)
      } else {
        a
      }

    // INSERT OVERWRITE by ordinal
    case o @ OverwriteByExpression(
    DataSourceV2Relation(d: DeltaTableV2, _, _, _, _), deleteExpr, query, writeOptions, false)
      if query.resolved && needsSchemaAdjustment(d.name(), query, d.schema(), writeOptions) =>
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
      val newTable = table.transformUp { case DeltaRelation(lr) => lr }
      val indices = newTable.collect {
        case DeltaFullTable(index) => index
      }
      if (indices.isEmpty) {
        // Not a Delta table at all, do not transform
        d
      } else if (indices.size == 1 && indices(0).deltaLog.snapshot.version > -1) {
        // rewrite subquery
        condition.foreach { c =>
          val (rewrite, target, newConditions, source) = rewriteSubquery("DELETE", table, c)
          if (rewrite) {
            return DeleteFromTable(target, newConditions, source)
          }
        }

        // It is a well-defined Delta table with a schema
        DeltaDelete(newTable, condition)
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
          condition.map(resolveExpressionTopDown(_, d)).flatMap(inferConditions)
        resolvedDeleteCondition.foreach(checkCondition(_, "cross tables delete"))

        val actions = DeltaMergeIntoDeleteClause(
          resolvedDeleteCondition.flatMap(extractPredicatesOnlyInTarget(target, _)))
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

    case u @ UpdateTable(table, assignments, condition, None) if u.resolved =>
      // rewrite subquery
      condition.foreach { c =>
        val (rewrite, target, newConditions, source) = rewriteSubquery("UPDATE", table, c)
        if (rewrite) {
          return UpdateTable(target, assignments, newConditions, source)
        }
      }

      val (columns, values) = assignments.map(a =>
        a.key.asInstanceOf[NamedExpression] -> a.value).unzip
      values.foreach { e =>
        if (SubqueryExpression.hasCorrelatedSubquery(e)) {
          throw DeltaErrors.correlatedSubqueryNotSupportedException("UpdateTable", e)
        }
      }
      // rewrites Delta from V2 to V1
      val newTable = table.transformUp { case DeltaRelation(lr) => lr }
      newTable.collectLeaves().headOption match {
        case Some(DeltaFullTable(index)) =>
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", o)
      }
      DeltaUpdateTable(newTable, columns, values, condition)

    case u @ UpdateTable(target, assignments, condition, Some(source)) if u.resolved &&
        target.outputSet.intersect(source.outputSet).isEmpty =>
      // rewrites Delta from V2 to V1
      val newTarget = target.transformUp { case DeltaRelation(lr) => lr }
      newTarget.collectLeaves().headOption match {
        case Some(DeltaFullTable(index)) =>
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", o)
      }
      val (columns, values) = assignments.map(a =>
        a.key.asInstanceOf[NamedExpression] -> a.value).unzip
      values.foreach { e =>
        if (SubqueryExpression.hasCorrelatedSubquery(e)) {
          throw DeltaErrors.correlatedSubqueryNotSupportedException("UpdateWithJoinTable", e)
        }
      }
      val inferredCondition = condition.flatMap(inferConditions)
      inferredCondition.foreach(checkCondition(_, "cross tables update"))

      if (inferredCondition.isEmpty && values.forall(_.foldable)) {
        // If join condition is empty and SET expressions are all foldable,
        // skip join and fallback to simple update
        logInfo(s"Update conditions are empty with foldable SET clause, fallback to simple update")
        DeltaUpdateTable(newTarget, columns, values, inferredCondition)
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
              newTarget, withProject, columns, values, inferredCondition, actions)
          case _ =>
            val withProject =
              projectionForSource(source, inferredCondition, columns, values)
            UpdateWithJoinTable(
              newTarget, withProject, columns, values, inferredCondition, actions)
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
      schema: StructType,
      writeOptions: Map[String, String]): Boolean = {
    val output = query.output
    val columnNames = writeOptions.get("insertColumns").map(_.split(",")).getOrElse(schema.names)
    if (output.length < columnNames.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(tableName, output.length, columnNames.length)
    }
    // Now we should try our best to match everything that already exists, and leave the rest
    // for schema evolution to WriteIntoDelta
    val existingSchemaOutput = output.take(columnNames.length)
    val filteredSchema = StructType(schema.filter(f => columnNames.toSet.contains(f.name)))
    existingSchemaOutput.map(_.name) != columnNames.toSeq ||
      !SchemaUtils.isReadCompatible(filteredSchema.asNullable, existingSchemaOutput.toStructType)
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
  private def inferConditions(condition: Expression): Option[Expression] = {
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
    splitConjunctivePredicates(predicates).filter(_.references.subsetOf(source.outputSet))
      .reduceLeftOption(And)
  }

  private def extractPredicatesOnlyInTarget(
      target: LogicalPlan, predicates: Expression): Option[Expression] = {
    splitConjunctivePredicates(predicates).filter(_.references.subsetOf(target.outputSet))
      .reduceLeftOption(And)
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


  private def dedupSubqueryOnSelfJoin(
      outerPlan: LogicalPlan,
      subplan: LogicalPlan,
      valuesOpt: Option[Seq[Expression]],
      condition: Option[Expression] = None): LogicalPlan = {
    // SPARK-21835: It is possibly that the two sides of the join have conflicting attributes,
    // the produced join then becomes unresolved and break structural integrity. We should
    // de-duplicate conflicting attributes.
    // SPARK-26078: it may also happen that the subquery has conflicting attributes with the outer
    // values. In this case, the resulting join would contain trivially true conditions (eg.
    // id#3 = id#3) which cannot be de-duplicated after. In this method, if there are conflicting
    // attributes in the join condition, the subquery's conflicting attributes are changed using
    // a projection which aliases them and resolves the problem.
    val outerReferences = valuesOpt.map(values =>
      AttributeSet(values.flatMap(_.references))).getOrElse(AttributeSet.empty)
    val outerRefs = outerPlan.outputSet ++ outerReferences
    val duplicates = outerRefs.intersect(subplan.outputSet)
    if (duplicates.nonEmpty) {
      condition.foreach { e =>
        val conflictingAttrs = e.references.intersect(duplicates)
        if (conflictingAttrs.nonEmpty) {
          throw new AnalysisException("Found conflicting attributes " +
            s"${conflictingAttrs.mkString(",")} in the condition joining outer plan:\n  " +
            s"$outerPlan\nand subplan:\n  $subplan")
        }
      }
      val rewrites = AttributeMap(duplicates.map { dup =>
        dup -> Alias(dup, dup.toString)()
      }.toSeq)
      val aliasedExpressions = subplan.output.map { ref =>
        rewrites.getOrElse(ref, ref)
      }
      Project(aliasedExpressions, subplan)
    } else {
      subplan
    }
  }

  /**
   * This is copy from PullupCorrelatedPredicates
   * Returns the correlated predicates and a updated plan that removes the outer references.
   */
  private def pullOutCorrelatedPredicates(
    sub: LogicalPlan,
    outer: Seq[LogicalPlan]): (LogicalPlan, Seq[Expression]) = {
    val predicateMap = scala.collection.mutable.Map.empty[LogicalPlan, Seq[Expression]]

    /** Determine which correlated predicate references are missing from this plan. */
    def missingReferences(p: LogicalPlan): AttributeSet = {
      val localPredicateReferences = p.collect(predicateMap)
        .flatten
        .map(_.references)
        .reduceOption(_ ++ _)
        .getOrElse(AttributeSet.empty)
      localPredicateReferences -- p.outputSet
    }

    // Simplify the predicates before pulling them out.
    val transformed = BooleanSimplification(sub) transformUp {
      case f @ Filter(cond, child) =>
        val (correlated, local) =
          splitConjunctivePredicates(cond).partition(containsOuter)

        // Rewrite the filter without the correlated predicates if any.
        correlated match {
          case Nil => f
          case xs if local.nonEmpty =>
            val newFilter = Filter(local.reduce(And), child)
            predicateMap += newFilter -> xs
            newFilter
          case xs =>
            predicateMap += child -> xs
            child
        }
      case p @ Project(expressions, child) =>
        val referencesToAdd = missingReferences(p)
        if (referencesToAdd.nonEmpty) {
          Project(expressions ++ referencesToAdd, child)
        } else {
          p
        }
      case a @ Aggregate(grouping, expressions, child) =>
        val referencesToAdd = missingReferences(a)
        if (referencesToAdd.nonEmpty) {
          Aggregate(grouping ++ referencesToAdd, expressions ++ referencesToAdd, child)
        } else {
          a
        }
      case p =>
        p
    }

    // Make sure the inner and the outer query attributes do not collide.
    // In case of a collision, change the subquery plan's output to use
    // different attribute by creating alias(s).
    val baseConditions = predicateMap.values.flatten.toSeq
    val (newPlan, newCond) = if (outer.nonEmpty) {
      val outputSet = outer.map(_.outputSet).reduce(_ ++ _)
      val duplicates = transformed.outputSet.intersect(outputSet)
      val (plan, deDuplicatedConditions) = if (duplicates.nonEmpty) {
        val aliasMap = AttributeMap(duplicates.map { dup =>
          dup -> Alias(dup, dup.toString)()
        }.toSeq)
        val aliasedExpressions = transformed.output.map { ref =>
          aliasMap.getOrElse(ref, ref)
        }
        val aliasedProjection = Project(aliasedExpressions, transformed)
        val aliasedConditions = baseConditions.map(_.transform {
          case ref: Attribute => aliasMap.getOrElse(ref, ref).toAttribute
        })
        (aliasedProjection, aliasedConditions)
      } else {
        (transformed, baseConditions)
      }
      (plan, stripOuterReferences(deDuplicatedConditions))
    } else {
      (transformed, stripOuterReferences(baseConditions))
    }
    (newPlan, newCond)
  }

  def getValidCondition(newCond: Seq[Expression], oldCond: Seq[Expression]): Seq[Expression] = {
    if (newCond.isEmpty) oldCond else newCond
  }

  def getJoinCondition(
      conds: Seq[Expression], left: LogicalPlan, right: LogicalPlan): Seq[Expression] = {
    conds.filter(c => c.references.intersect(left.outputSet).nonEmpty &&
      c.references.intersect(right.outputSet).nonEmpty)
  }

  def checkCondition(cond: Expression, conditionName: String): Unit = {
    if (!cond.deterministic) {
      throw DeltaErrors.nonDeterministicNotSupportedException(
        s"$conditionName condition of DELETE operation", cond)
    }
    if (cond.find(_.isInstanceOf[AggregateExpression]).isDefined) {
      throw DeltaErrors.aggsNotSupportedException(
        s"$conditionName condition of DELETE operation", cond)
    }
    if (SubqueryExpression.hasSubquery(cond)) {
      throw DeltaErrors.subqueryNotSupportedException(
        s"$conditionName condition of DELETE operation", cond)
    }
  }

  def rewriteSubquery(op: String, table: LogicalPlan, condition: Expression)
      : (Boolean, LogicalPlan, Option[Expression], Option[LogicalPlan]) = {
    val flattenPredicates = flattenToDisjunctivePredicates(condition)
    val (dnfWithSubquery, dnfWithoutSubquery) =
      flattenPredicates.partition(SubqueryExpression.hasSubquery)
    if (dnfWithSubquery.isEmpty) {
      return (false, table, None, None)
    }
    if (dnfWithSubquery.length > 1) {
      throw DeltaErrors.moreThanOneSubqueryNotSupportedException(op)
    }
    val predicates = splitConjunctivePredicates(dnfWithSubquery.head)
    if (predicates.exists { e =>
      // non correlate scalar subquery
      e.find {
        case s: ScalarSubquery => s.children.isEmpty
        case _ => false
      }.isDefined }) {
      return (false, table, None, None)
    }
    if (predicates.exists { p =>
      // IN or correlated sub-query are supported only
      SubqueryExpression.hasSubquery(p) &&
        !SubqueryExpression.hasInOrCorrelatedExistsSubquery(p)
    }) {
      throw DeltaErrors.InOrUncorrelatedSubquerySupportedOnlyException(op)
    }
    val (cnfWithSubquery, cnfWithoutSubquery) =
      predicates.partition(SubqueryExpression.hasInOrCorrelatedExistsSubquery)
    if (cnfWithSubquery.nonEmpty) {
      // handle in or correlated sub-query
      if (cnfWithSubquery.length > 1) {
        throw DeltaErrors.moreThanOneSubqueryNotSupportedException(op)
      }

      val correlatedSubquery = getCorrelatedSubquery(table, cnfWithSubquery)
      correlatedSubquery match {
        case ScalarSubquery(sub, children, _) if children.nonEmpty =>
          (true, table, ((children ++ cnfWithoutSubquery).reduceLeftOption(And)
            ++ dnfWithoutSubquery.reduceLeftOption(Or)).reduceLeftOption(Or), Some(sub))
        case Exists(sub, children, _) if children.nonEmpty =>
          (true, table, ((children ++ cnfWithoutSubquery).reduceLeftOption(And)
            ++ dnfWithoutSubquery.reduceLeftOption(Or)).reduceLeftOption(Or), Some(sub))
        case ListQuery(sub, children, _, _) if children.nonEmpty =>
          (true, table, ((children ++ cnfWithoutSubquery).reduceLeftOption(And)
            ++ dnfWithoutSubquery.reduceLeftOption(Or)).reduceLeftOption(Or), Some(sub))
        case InSubquery(values, ListQuery(sub, children, _, _)) if children.nonEmpty =>
          val newSub = dedupSubqueryOnSelfJoin(table, sub, Some(values))
          val inConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val subJoinConditions = getJoinCondition(children, table, sub)
          (true, table,
            ((inConditions ++ subJoinConditions ++ cnfWithoutSubquery).reduceLeftOption(And)
            ++ dnfWithoutSubquery.reduceLeftOption(Or)).reduceLeftOption(Or), Some(sub))
        case InSubquery(values, ListQuery(sub, children, _, _)) =>
          val newSub = dedupSubqueryOnSelfJoin(table, sub, Some(values))
          val inConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val subJoinConditions = getJoinCondition(children, table, sub)
          (true, table,
            ((inConditions ++ subJoinConditions ++ cnfWithoutSubquery).reduceLeftOption(And)
            ++ dnfWithoutSubquery.reduceLeftOption(Or)).reduceLeftOption(Or), Some(newSub))
        case Not(InSubquery(values, ListQuery(sub, _, _, _))) =>
          val newSub = dedupSubqueryOnSelfJoin(table, sub, Some(values))
          val joinConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val find = newSub.find {
            case Filter(cond, _) =>
              val constraints = splitConjunctivePredicates(cond)
              val exprs = constraints.flatMap(inferIsNotNullConstraints).collect {
                case IsNotNull(Cast(expr, _, _)) => expr
                case IsNotNull(expr) => expr
              }
              newSub.output.intersect(exprs).nonEmpty
            case _ => false
          }
          if (find.isEmpty) {
            throw DeltaErrors.NotInWithoutIsNotNullNotSupportedException(op)
          }
          val newSource = Join(table, newSub,
            LeftAnti, joinConditions.reduceLeftOption(And), JoinHint.NONE)

          // generate a newTable which with new exprId
          val newTable = table.transformUp {
            case ds @ DataSourceV2Relation(_, output, _, _, _) =>
              val newOutput = output.map { a => a.withExprId(NamedExpression.newExprId) }
              ds.copy(output = newOutput)
          }
          // coordinately, the join condition should use the new exprId
          val newJoinConditions = values.map { expr =>
            expr transformDown {
              case a: AttributeReference => newTable.output.find(_.name == a.name).getOrElse(a)
            }
          }.zip(values).map(EqualTo.tupled)
          val newDnfWithoutSubquery = dnfWithoutSubquery.map { expr =>
            expr.transformDown { case a: AttributeReference =>
              newTable.output.find(_.name == a.name).getOrElse(a)
            }
          }.reduceLeftOption(Or)
          (true, newTable,
            ((newJoinConditions ++ cnfWithoutSubquery).reduceLeftOption(And)
              ++ newDnfWithoutSubquery).reduceLeftOption(Or), Some(newSource))
        case Not(Exists(sub, children, _)) if children.nonEmpty =>
          val newSource = Join(table, sub,
            LeftAnti, children.reduceLeftOption(And), JoinHint.NONE)
          // generate a newTable which with new exprId
          val newTable = table.transformUp {
            case ds @ DataSourceV2Relation(_, output, _, _, _) =>
              val newOutput = output.map { a => a.withExprId(NamedExpression.newExprId) }
              ds.copy(output = newOutput)
          }
          val joinConditions = getJoinCondition(children, table, sub)

          // coordinately, the join condition should use the new exprId
          val newJoinConditions = (joinConditions.map { cond =>
            cond transformDown {
              case c: AttributeReference if sub.output.exists {
                case a: AttributeReference => a.sameRef(c)
              } => newTable.output.find(_.name == c.name).getOrElse(c)
            }
          } ++ cnfWithoutSubquery).reduceLeftOption(And)
          val newDnfWithoutSubquery = dnfWithoutSubquery.map { expr =>
            expr.transformDown { case a: AttributeReference =>
              newTable.output.find(_.name == a.name).getOrElse(a)
            }
          }.reduceLeftOption(Or)
          (true, newTable,
            (newJoinConditions ++ newDnfWithoutSubquery).reduceLeftOption(Or), Some(newSource))
        case _ =>
          throw DeltaErrors.subqueryNotSupportedException(op, correlatedSubquery)
      }
    } else {
      (false, table, None, None)
    }
  }

  private def getCorrelatedSubquery(table: LogicalPlan, withSubquery: Seq[Expression]) = {
    val outerPlans = table.children
    val correlatedSubquery = withSubquery.head transformDown {
      case ScalarSubquery(sub, children, exprId) if children.nonEmpty =>
        val (newPlan, newCond) = pullOutCorrelatedPredicates(sub, outerPlans)
        ScalarSubquery(newPlan, getValidCondition(newCond, children), exprId)
      case Exists(sub, children, exprId) if children.nonEmpty =>
        val (newPlan, newCond) = pullOutCorrelatedPredicates(sub, outerPlans)
        Exists(newPlan, getValidCondition(newCond, children), exprId)
      case ListQuery(sub, children, exprId, childOutputs) if children.nonEmpty =>
        val (newPlan, newCond) = pullOutCorrelatedPredicates(sub, outerPlans)
        ListQuery(newPlan, getValidCondition(newCond, children), exprId, childOutputs)
    }
    correlatedSubquery
  }

  /**
   * Infer the Attribute-specific IsNotNull constraints from the null intolerant child expressions
   * of constraints.
   */
  private def inferIsNotNullConstraints(constraint: Expression): Seq[Expression] =
    constraint match {
      // When the root is IsNotNull, we can push IsNotNull through the child null intolerant
      // expressions
      case IsNotNull(expr) => scanNullIntolerantAttribute(expr).map(IsNotNull(_))
      // Constraints always return true for all the inputs. That means, null will never be returned.
      // Thus, we can infer `IsNotNull(constraint)`, and also push IsNotNull through the child
      // null intolerant expressions.
      case _ => scanNullIntolerantAttribute(constraint).map(IsNotNull(_))
    }

  /**
   * Recursively explores the expressions which are null intolerant and returns all attributes
   * in these expressions.
   */
  private def scanNullIntolerantAttribute(expr: Expression): Seq[Attribute] = expr match {
    case a: Attribute => Seq(a)
    case _: NullIntolerant => expr.children.flatMap(scanNullIntolerantAttribute)
    case _ => Seq.empty[Attribute]
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

