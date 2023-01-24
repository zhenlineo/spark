/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect.client

import java.io.File
import java.net.URLClassLoader
import java.util.regex.Pattern

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.lib.MiMaLib
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite
import org.apache.spark.sql.connect.client.util.IntegrationTestUtils._

/**
 * This test requires the following artifacts built before running the tests:
 * {{{
 *     spark-sql
 *     spark-connect-client-jvm
 * }}}
 * To build the above artifact, use e.g. `sbt package` or `mvn clean install -DskipTests`.
 *
 * When debugging this test, if any changes to the client API, the client jar need to be built
 * before re-running the test. An example workflow with SBT for this test:
 *   1. Compatibility test reported an unexpected client API change.
 *   1. Fix the wrong client API.
 *   1. Build the client jar: `sbt package`
 *   1. Run the test again: `sbt "testOnly
 *      org.apache.spark.sql.connect.client.CompatibilitySuite"`
 */
class CompatibilitySuite extends AnyFunSuite { // scalastyle:ignore funsuite

  private lazy val clientJar: File =
    findJar(
      "connector/connect/client/jvm",
      "spark-connect-client-jvm-assembly",
      "spark-connect-client-jvm")

  private lazy val sqlJar: File = findJar("sql/core", "spark-sql", "spark-sql")

  test("compatibility mima tests") {
    val mima = new MiMaLib(Seq(clientJar, sqlJar))
    val allProblems = mima.collectProblems(sqlJar, clientJar, List.empty)
    val includedRules = Seq(
      IncludeByName("org.apache.spark.sql.Column"),
      IncludeByName("org.apache.spark.sql.Dataset"),
      IncludeByName("org.apache.spark.sql.DataFrame"),
      IncludeByName("org.apache.spark.sql.SparkSession")) ++ includeImplementedMethods(clientJar)
    val excludeRules = Seq(
      // Filter unsupported rules:
      // Two sql method overloads are marked experimental in the API.
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession.sql"),
      ProblemFilters.exclude[Problem]("org.sparkproject.*"),
      ProblemFilters.exclude[Problem]("org.apache.spark.connect.proto.*"))
    val problems = allProblems
      .filter { p =>
        includedRules.exists(rule => rule(p))
      }
      .filter { p =>
        excludeRules.forall(rule => rule(p))
      }

    if (problems.nonEmpty) {
      fail(
        s"\nComparing client jar: $clientJar\nand sql jar: $sqlJar\n" +
          problems.map(p => p.description("client")).mkString("\n"))
    }
  }

  test("compatibility API tests: Dataset") {
    val clientClassLoader: URLClassLoader = new URLClassLoader(Seq(clientJar.toURI.toURL).toArray)
    val sqlClassLoader: URLClassLoader = new URLClassLoader(Seq(sqlJar.toURI.toURL).toArray)

    val clientClass = clientClassLoader.loadClass("org.apache.spark.sql.Dataset")
    val sqlClass = sqlClassLoader.loadClass("org.apache.spark.sql.Dataset")

    val newMethods = clientClass.getMethods
    val oldMethods = sqlClass.getMethods

    // For now we simply check the new methods is a subset of the old methods.
    newMethods
      .map(m => m.toString)
      .foreach(method => {
        assert(oldMethods.map(m => m.toString).contains(method))
      })
  }

  /**
   * Find all methods that are implemented in the client jar. Once all major methods are
   * implemented we can switch to include all methods under the class using ".*" e.g.
   * "org.apache.spark.sql.Dataset.*"
   */
  private def includeImplementedMethods(clientJar: File): Seq[IncludeByName] = {
    val clsNames = Seq(
      "org.apache.spark.sql.Column",
      // Mute compatibility check on the Dataset methods, as too many overload methods are missing.
      //  "org.apache.spark.sql.Dataset",
      "org.apache.spark.sql.SparkSession")

    val clientClassLoader: URLClassLoader = new URLClassLoader(Seq(clientJar.toURI.toURL).toArray)
    clsNames
      .flatMap { clsName =>
        val cls = clientClassLoader.loadClass(clsName)
        // all distinct method names
        cls.getMethods.map(m => s"$clsName.${m.getName}").toSet
      }
      .map { fullName =>
        IncludeByName(fullName)
      }
  }

  private case class IncludeByName(name: String) extends ProblemFilter {
    private[this] val pattern =
      Pattern.compile(name.split("\\*", -1).map(Pattern.quote).mkString(".*"))

    override def apply(problem: Problem): Boolean = {
      pattern.matcher(problem.matchName.getOrElse("")).matches
    }
  }
}
