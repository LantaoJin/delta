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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

class DeltaSubQuerySuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest with SQLTestUtils {

  test("Or in where") {
    withTable("source", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (2, 20), (3, 30)")
      sql("CREATE TABLE target (a int, b int) USING delta")
      sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
      sql(
        """
          |DELETE t
          |FROM target t, source s
          |WHERE t.a = s.a AND s.a % 2 =1 OR t.b = 2 OR t.b = 5
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(4, 4) :: Nil)
    }
  }

  test("Or and subquery in where") {
    withTable("source", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (2, 20), (3, 30)")
      sql("CREATE TABLE target (a int, b int) USING delta")
      sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")

      sql(
        """
          |DELETE FROM target t
          |WHERE t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 1) OR t.b = 2
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(4, 4) :: Row(5, 5) :: Nil)
      sql("INSERT INTO target VALUES (2, 2)")
      sql(
        """
          |UPDATE target t
          |SET t.b = 0
          |WHERE t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 0) OR t.b = 5
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(2, 0) :: Row(4, 4) :: Row(5, 0) :: Nil)
    }
  }

  test("More than one subqueries with Or in where") {
    withTable("source", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (2, 20), (3, 30)")
      sql("CREATE TABLE target (a int, b int) USING delta")
      sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
      val e = intercept[AnalysisException] {
        sql(
          """
            |DELETE FROM target t
            |WHERE t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 1)
            |  OR t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 0)
            |""".stripMargin)
      }.getMessage()
      assert(e.contains("More than one sub-queries are not supported"))
    }
  }

  test("IN sub-query in where") {
    withTable("source", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (2, 20), (3, 30)")
      sql("CREATE TABLE target (a int, b int) USING delta")
      sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
      val df1 = sql(
        """
          |SELECT * FROM target t
          |WHERE t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 1)
          |""".stripMargin)
      checkAnswer(df1, Row(1, 1) :: Row(3, 3) :: Nil)
      sql(
        """
          |DELETE FROM target t
          |WHERE t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 1)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(2, 2) :: Row(4, 4) :: Row(5, 5) :: Nil)
      sql(
        """
          |UPDATE target t
          |SET t.b = 0
          |WHERE t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 0)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(2, 0) :: Row(4, 4) :: Row(5, 5) :: Nil)
    }
  }

  test("NOT IN sub-query in where") {
    withTable("source", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (3, 30), (4, 40), (7, 70)")
      sql("CREATE TABLE target (a int, b int) USING delta")
      sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
      sql(
        """
          |DELETE FROM target t
          |WHERE t.a NOT IN (SELECT s.a FROM source s WHERE s.a IS NOT NULL)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(1, 1) :: Row(3, 3) :: Row(4, 4) :: Nil)
      sql("INSERT INTO target VALUES (2, 2), (5, 5)")
      sql(
        """
          |DELETE FROM target t
          |WHERE t.a NOT IN (SELECT s.a FROM source s WHERE s.a % 2 = 1)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(1, 1) :: Row(3, 3) :: Nil)
      sql(
        """
          |UPDATE target t
          |SET t.b = 0
          |WHERE t.a NOT IN (SELECT s.a FROM source s WHERE s.a % 2 = 0)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(1, 0) :: Row(3, 0) :: Nil)
    }
  }

  test("EXISTS sub-query in where") {
    withTable("source", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (2, 20), (3, 30)")
      sql("CREATE TABLE target (a int, b int) USING delta")
      sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
      val df2 = sql(
        """
          |SELECT * FROM target t
          |WHERE EXISTS (SELECT * FROM source s WHERE t.a = s.a AND s.a % 2 = 1)
          |""".stripMargin)
      checkAnswer(df2, Row(1, 1) :: Row(3, 3) :: Nil)
      sql(
        """
          |DELETE FROM target t
          |WHERE EXISTS (SELECT * FROM source s WHERE t.a = s.a AND s.a % 2 = 1)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(2, 2) :: Row(4, 4) :: Row(5, 5) :: Nil)
      sql(
        """
          |UPDATE target t
          |SET t.b = 0
          |WHERE EXISTS (SELECT * FROM source s WHERE t.a = s.a AND s.a % 2 = 0)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(2, 0) :: Row(4, 4) :: Row(5, 5) :: Nil)
    }
  }

  test("NOT EXISTS sub-query in where") {
    withTable("source", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (3, 30), (4, 40), (7, 70)")
      sql("CREATE TABLE target (a int, b int) USING delta")
      sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
      sql(
        """
          |DELETE FROM target t
          |WHERE NOT EXISTS (SELECT * FROM source s WHERE t.a = s.a)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(1, 1) :: Row(3, 3) :: Row(4, 4) :: Nil)
      sql("INSERT INTO target VALUES (2, 2), (5, 5)")
      sql(
        """
          |DELETE FROM target t
          |WHERE NOT EXISTS (SELECT * FROM source s WHERE t.a = s.a AND s.a % 2 = 1)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(1, 1) :: Row(3, 3) :: Nil)
      sql(
        """
          |UPDATE target t
          |SET t.b = 0
          |WHERE NOT EXISTS (SELECT * FROM source s WHERE t.a = s.a AND s.a % 2 = 0)
          |""".stripMargin)
      checkAnswer(spark.table("target"), Row(1, 0) :: Row(3, 0) :: Nil)
    }
  }

  test("Uncorrelated sub-query in where") {
    withTable("source", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (2, 20), (3, 30)")
      sql("CREATE TABLE target USING delta AS SELECT * FROM source")
      val df3 = sql(
        """
          |SELECT * FROM target t
          |WHERE t.a = (SELECT max(s.a) FROM source s)
          |""".stripMargin)
      checkAnswer(df3, Row(3, 30) :: Nil)
      val e = intercept[AnalysisException] {
        sql(
          """
            |DELETE FROM target t
            |WHERE t.a = (SELECT max(s.a) FROM source s)
            |""".stripMargin)
      }.getMessage()
      assert(e.contains("In or uncorrelated subquery is supported only in"))
      val e2 = intercept[AnalysisException] {
        sql(
          """
            |UPDATE target t
            |SET t.b = 0
            |WHERE t.a = (SELECT max(s.a) FROM source s)
            |""".stripMargin)
      }.getMessage()
      assert(e2.contains("In or uncorrelated subquery is supported only in"))
    }
  }


  test("cross table update/delete with sub-query in where is not supported") {
    withTable("source", "source2", "target") {
      sql("CREATE TABLE source(a int, b int) USING parquet")
      sql("INSERT INTO source values (1, 10), (3, 30), (4, 40), (7, 70)")
      sql("CREATE TABLE source2(a int, b int) USING parquet")
      sql("INSERT INTO source2 values (1, 10), (3, 30), (4, 40), (7, 70)")
      sql("CREATE TABLE target (a int, b int) USING delta")
      sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
      val e = intercept[AnalysisException] {
        sql(
          """
            |DELETE t
            |FROM target t, source2 s2
            |WHERE t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 1) AND t.a = s2.a
            |""".stripMargin)
      }.getMessage()
      assert(e.contains("Subqueries are not supported in the cross tables"))
      val e2 = intercept[AnalysisException] {
        sql(
          """
            |UPDATE t
            |FROM target t, source2 s2
            |SET t.b = s2.b
            |WHERE t.a IN (SELECT s.a FROM source s WHERE s.a % 2 = 0) AND t.a = s2.a
            |""".stripMargin)
      }.getMessage()
      assert(e2.contains("Subqueries are not supported in the cross tables"))
    }
  }

  def checkNestedInSubquery(where: String, source: String, supported: Boolean = true): Unit = {
    withTable("target") {
      sql(s"CREATE TABLE target USING delta AS SELECT * FROM $source")
      if (supported) {
        val beforeDF =
          sql(
            s"""
               |SELECT * FROM target
               |$where
               |""".stripMargin)
         beforeDF.show(false)
        val before = beforeDF.collect()
        sql(
          s"""
             |DELETE FROM target
             |$where
             |""".stripMargin)
        val afterDF = spark.table("target")
         afterDF.show(false)
        val after = afterDF.collect()
        checkAnswer(spark.table(source), before ++ after)
      } else {
        val e = intercept[AnalysisException] {
          sql(
            s"""
               |DELETE FROM target
               |$where
               |""".stripMargin)
        }.getMessage()
        assert(e.contains("not supported"))
      }
    }
  }

  test("Nested IN subqueries in where") {
    // scalastyle:off
    sql(
      """
        |create temporary view t1 as select * from values
        |  ("t1a", 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:00:00.000', date '2014-04-04'),
        |  ("t1b", 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
        |  ("t1a", 16S, 12, 21L, float(15.0), 20D, 20E2BD, timestamp '2014-06-04 01:02:00.001', date '2014-06-04'),
        |  ("t1a", 16S, 12, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
        |  ("t1c", 8S, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.001', date '2014-05-05'),
        |  ("t1d", null, 16, 22L, float(17.0), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', null),
        |  ("t1d", null, 16, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.001', null),
        |  ("t1e", 10S, null, 25L, float(17.0), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-04'),
        |  ("t1e", 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.001', date '2014-09-04'),
        |  ("t1d", 10S, null, 12L, float(17.0), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
        |  ("t1a", 6S, 8, 10L, float(15.0), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.001', date '2014-04-04'),
        |  ("t1e", 10S, null, 19L, float(17.0), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04')
        |  as t1(t1a, t1b, t1c, t1d, t1e, t1f, t1g, t1h, t1i);
        |""".stripMargin)
    sql(
      """
        |create temporary view t2 as select * from values
        |  ("t2a", 6S, 12, 14L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
        |  ("t1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
        |  ("t1b", 8S, 16, 119L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
        |  ("t1c", 12S, 16, 219L, float(17), 25D, 26E2BD, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
        |  ("t1b", null, 16, 319L, float(17), 25D, 26E2BD, timestamp '2017-05-04 01:01:00.000', null),
        |  ("t2e", 8S, null, 419L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
        |  ("t1f", 19S, null, 519L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
        |  ("t1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
        |  ("t1b", 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
        |  ("t1c", 12S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
        |  ("t1e", 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
        |  ("t1f", 19S, null, 19L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
        |  ("t1b", null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:01:00.000', null)
        |  as t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i);
        |""".stripMargin)
    sql(
      """
        |create temporary view t3 as select * from values
        |  ("t3a", 6S, 12, 110L, float(15), 20D, 20E2BD, timestamp '2014-04-04 01:02:00.000', date '2014-04-04'),
        |  ("t3a", 6S, 12, 10L, float(15), 20D, 20E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
        |  ("t1b", 10S, 12, 219L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
        |  ("t1b", 10S, 12, 19L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
        |  ("t1b", 8S, 16, 319L, float(17), 25D, 26E2BD, timestamp '2014-06-04 01:02:00.000', date '2014-06-04'),
        |  ("t1b", 8S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-07-04 01:02:00.000', date '2014-07-04'),
        |  ("t3c", 17S, 16, 519L, float(17), 25D, 26E2BD, timestamp '2014-08-04 01:02:00.000', date '2014-08-04'),
        |  ("t3c", 17S, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-09-04 01:02:00.000', date '2014-09-05'),
        |  ("t1b", null, 16, 419L, float(17), 25D, 26E2BD, timestamp '2014-10-04 01:02:00.000', null),
        |  ("t1b", null, 16, 19L, float(17), 25D, 26E2BD, timestamp '2014-11-04 01:02:00.000', null),
        |  ("t3b", 8S, null, 719L, float(17), 25D, 26E2BD, timestamp '2014-05-04 01:02:00.000', date '2014-05-04'),
        |  ("t3b", 8S, null, 19L, float(17), 25D, 26E2BD, timestamp '2015-05-04 01:02:00.000', date '2015-05-04')
        |  as t3(t3a, t3b, t3c, t3d, t3e, t3f, t3g, t3h, t3i);
        |""".stripMargin)
    // scalastyle:on

    val where1 =
      """
        |WHERE t1a IN (SELECT t2a
        |               FROM   t2)
        |""".stripMargin
    val where2 =
      """
        |WHERE t1b IN (SELECT t2b
        |               FROM   t2
        |               WHERE  t1a = t2a)
        |""".stripMargin
    val where3 =
      """
        |WHERE t1c IN (SELECT t2b
        |               FROM   t2
        |               WHERE  t1a != t2a)
        |""".stripMargin
    val where4 =
      """
        |WHERE t1c IN (SELECT t2b
        |               FROM   t2
        |               WHERE  t1a = t2a
        |                       OR t1b > t2b)
        |""".stripMargin
    val where5 =
      """
        |WHERE t1c IN (SELECT t2b
        |               FROM   t2
        |               WHERE  t2i IN (SELECT t3i
        |                              FROM   t3
        |                              WHERE  t2c = t3c))
        |""".stripMargin
    val where6 =
      """
        |WHERE t1c IN (SELECT t2b
        |               FROM   t2
        |               WHERE  t2a IN (SELECT t3a
        |                              FROM   t3
        |                              WHERE  t2c = t3c
        |                                     AND t2b IS NOT NULL))
        |""".stripMargin
    Seq(where1, where2, where3, where4, where5, where6)
      .foreach(checkNestedInSubquery(_, "t1"))
  }

  test("More than one subquery in where") {
    sql(
      """
        |CREATE TEMPORARY VIEW S1 AS SELECT * FROM VALUES
        |  (null, null), (4, 4), (5, 5), (8, 8), (9, 9), (11, 11) AS s1(a, b)
        |""".stripMargin)
    sql(
      """
        |CREATE TEMPORARY VIEW S2 AS SELECT * FROM VALUES
        |  (7, 7), (8, 8), (11, 11), (null, null) AS s2(c, d)
        |""".stripMargin)
    val where1 =
      """
        |WHERE a IN (SELECT c FROM s2) AND a IN (10, 11, 12)
        |""".stripMargin
    val where2 =
      """
        |WHERE a IN (SELECT c FROM s2) AND b IN (10, 11, 12)
        |""".stripMargin
    val where3 =
      """
        |WHERE a IN (SELECT c FROM s2) AND b IN (SELECT d FROM s2)
        |""".stripMargin
    Seq(where1, where2)
      .foreach(checkNestedInSubquery(_, "s1"))
    Seq(where3)
      .foreach(checkNestedInSubquery(_, "s1", supported = false))
  }

  test("NOT IN subqueries in where") {
    sql(
      """
        |CREATE TEMPORARY VIEW S1 AS SELECT * FROM VALUES
        |  (null, null), (4, 4), (5, 5), (8, 8), (9, 9), (11, 11) AS s1(a, b)
        |""".stripMargin)
    sql(
      """
        |CREATE TEMPORARY VIEW S2 AS SELECT * FROM VALUES
        |  (7, 7), (8, 8), (11, 11), (null, null) AS s2(c, d)
        |""".stripMargin)
    val where11 =
      """
        |WHERE NOT (a NOT IN (SELECT c
        |                      FROM s2));
        |""".stripMargin
    val where12 =
      """
        |WHERE NOT (a > 5
        |            OR a IN (SELECT c
        |                     FROM s2))
        |""".stripMargin
    val where13 =
      """
        |WHERE NOT (a > 5
        |            OR a IN (SELECT c
        |                     FROM s2 WHERE c IS NOT NULL))
        |""".stripMargin
    val where14 =
      """
        |WHERE NOT (a > 5
        |            OR a NOT IN (SELECT c
        |                         FROM s2))
        |""".stripMargin
    val where15 =
      """
        |WHERE NOT (a > 5
        |            AND a IN (SELECT c
        |                      FROM s2))
        |""".stripMargin
    val where16 =
      """
        |WHERE NOT (a > 5
        |            AND a IN (SELECT c
        |                      FROM s2 WHERE c IS NOT NULL))
        |""".stripMargin
    val where17 =
      """
        |WHERE NOT (a > 5
        |            AND a NOT IN (SELECT c
        |                          FROM   s2))
        |""".stripMargin
    Seq(where11, where13, where14, where16, where17)
      .foreach(checkNestedInSubquery(_, "s1"))
    Seq(where12, where15)
      .foreach(checkNestedInSubquery(_, "s1", supported = false))
  }

  test("Nested NOT IN subqueries in where") {
    sql(
      """
        |CREATE TEMPORARY VIEW EMP AS SELECT * FROM VALUES
        |  (100, "emp 1", 10),
        |  (200, "emp 2", NULL),
        |  (300, "emp 3", 20),
        |  (400, "emp 4", 30),
        |  (500, "emp 5", NULL),
        |  (600, "emp 6", 100),
        |  (800, "emp 8", 70)
        |AS EMP(id, emp_name, dept_id)
        |""".stripMargin)
    sql(
      """
        |CREATE TEMPORARY VIEW DEPT AS SELECT * FROM VALUES
        |  (10, "dept 1", "CA"),
        |  (20, "dept 2", "NY"),
        |  (30, "dept 3", "TX"),
        |  (40, "dept 4 - unassigned", "OR"),
        |  (50, "dept 5 - unassigned", "NJ"),
        |  (70, "dept 7", "FL")
        |AS DEPT(dept_id, dept_name, state)
        |""".stripMargin)
    sql(
      """
        |CREATE TEMPORARY VIEW BONUS AS SELECT * FROM VALUES
        |  ("emp 1", 10.00D),
        |  ("emp 1", 20.00D),
        |  ("emp 2", 300.00D),
        |  ("emp 2", 100.00D),
        |  ("emp 3", 300.00D),
        |  ("emp 4", 100.00D),
        |  ("emp 5", 1000.00D),
        |  ("emp 6 - no dept", 500.00D)
        |AS BONUS(emp_name, bonus_amt)
        |""".stripMargin)
    sql(
      """
        |CREATE TEMPORARY VIEW ADDRESS AS SELECT * FROM VALUES
        |  (100, "emp 1", "addr1"),
        |  (200, null, "addr2"),
        |  (null, "emp 3", "addr3"),
        |  (null, null, "addr4"),
        |  (600, "emp 6", "addr6"),
        |  (800, "emp 8", "addr8")
        |AS ADDRESS(id, emp_name, address)
        |""".stripMargin)
    val where1 =
      """
        |WHERE id = 600
        |       OR id = 500
        |       OR dept_id NOT IN (SELECT dept_id
        |                          FROM   emp)
        |""".stripMargin
    val where2 =
      """
        |WHERE id = 800
        |       OR (dept_id IS NOT NULL
        |           AND dept_id NOT IN (SELECT dept_id
        |                                FROM   emp))
        |""".stripMargin
    val where3 =
      """
        |WHERE id = 100
        |       OR dept_id NOT IN (SELECT dept_id
        |                           FROM   emp
        |                           WHERE dept_id IS NOT NULL)
        |""".stripMargin
    val where4 =
      """
        |WHERE id = 200
        |       OR (dept_id IS NOT NULL
        |       AND dept_id + 100 NOT IN (SELECT dept_id
        |                           FROM   emp
        |                           WHERE dept_id IS NOT NULL))
        |""".stripMargin
    val where5 = // Multiple subqueries with disjunctive
      """
        |WHERE emp_name IN (SELECT emp_name
        |                    FROM   bonus)
        |        OR (dept_id IS NOT NULL
        |            AND dept_id NOT IN (SELECT dept_id
        |                                FROM   dept))
        |""".stripMargin
    val where6 = // Multiple subqueries with disjunctive
      """
        |WHERE EXISTS (SELECT emp_name
        |               FROM   bonus
        |               WHERE  target.emp_name = bonus.emp_name)
        |       OR (dept_id IS NOT NULL
        |           AND dept_id NOT IN (SELECT dept_id
        |                               FROM   dept))
        |""".stripMargin
    val where7 =
      """
        |WHERE dept_id = 10
        |OR (id, emp_name) NOT IN (SELECT id, emp_name FROM address)
        |""".stripMargin
    val where8 =
      """
        |WHERE dept_id = 10
        |        OR (( id, emp_name ) NOT IN (SELECT id,
        |                                             emp_name
        |                                      FROM   address
        |                                      WHERE  id IS NOT NULL
        |                                             AND emp_name IS NOT NULL)
        |        AND id > 400 )
        |""".stripMargin
    val where9 =
      """
        |WHERE dept_id = 10
        |       OR emp_name NOT IN (SELECT emp_name
        |                                  FROM   address
        |                                  WHERE  id IS NOT NULL
        |                                  AND emp_name IS NOT NULL
        |                                  AND target.id = address.id)
        |""".stripMargin
    val where10 = // Multiple subqueries with disjunctive
      """
        |WHERE id NOT IN (SELECT id
        |                         FROM   address
        |                         WHERE  id IS NOT NULL
        |                         AND emp_name IS NOT NULL
        |                         AND id >= 400)
        |       OR emp_name NOT IN (SELECT emp_name
        |                                  FROM   address
        |                                  WHERE  id IS NOT NULL
        |                                  AND emp_name IS NOT NULL
        |                                  AND target.id = address.id
        |                                  AND id < 400)
        |""".stripMargin
    Seq(where3, where4, where8, where9)
      .foreach(checkNestedInSubquery(_, "emp"))
    Seq(where1, where2, where5, where6, where7, where10)
      .foreach(checkNestedInSubquery(_, "emp", supported = false))
  }
}
