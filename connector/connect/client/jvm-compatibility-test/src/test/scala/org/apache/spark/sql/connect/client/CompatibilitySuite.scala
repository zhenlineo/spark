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

import com.typesafe.tools.mima.core.{Problem, ProblemFilter, ProblemFilters}
import com.typesafe.tools.mima.lib.MiMaLib
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class CompatibilitySuite extends AnyFunSuite { // scalastyle:ignore funsuite
  val clientJarName = "spark-connect-client-jvm"
  val sqlJarName = "spark-sql"
  val version = "2.12-3.4.0-SNAPSHOT"
  // When running with IDE, the jars are not in the classpath.
  // For the convenience of testing, we fall back to read the jars built by maven.
  // Note: Remember to run "mvn package" when the jar files are not present or changed when running
  // test via IDE.
  // The fall-back paths to the maven jars when running from the IDE.
  val sbtClientJarPath = s"connector/connect/client/jvm/target/${clientJarName}_$version.jar"
  val sbtSqlJarPath = s"sql/core/target/spark-sql_$version.jar"

  test("compatibility mima tests") {
    val clientJar = findJar(clientJarName, sbtClientJarPath)
    assert(clientJar.exists(), clientJar.getAbsolutePath)

    val sqlJar = findJar(sqlJarName, sbtSqlJarPath)
    assert(sqlJar.exists(), sqlJar.getAbsolutePath)

    val mima = new MiMaLib(Nil)
    val allProblems = mima.collectProblems(sqlJar, clientJar, List.empty)
    val includedRules = Seq(
      // For now we only compare a few classes
      IncludeByName("org.apache.spark.sql.Dataset"),
      IncludeByName("org.apache.spark.sql.Dataset$"),
      IncludeByName("org.apache.spark.sql.Dataset.*"),

      IncludeByName("org.apache.spark.sql.SparkSession"),
      IncludeByName("org.apache.spark.sql.SparkSession$"),
      IncludeByName("org.apache.spark.sql.SparkSession.*")
    )
    val excludeRules = Seq(
      // Filter unimplemented classes
      ProblemFilters.exclude[Problem]("org.apache.spark.sql.SparkSession*")
    )
    val problems = allProblems.filter { p =>
      includedRules.exists(rule => rule(p))
    }.filter { p =>
      excludeRules.forall(rule => rule(p))
    }

    // Mima consider Dataset an internal class due to private[sql] modifier on the object.
    // Thus there is no check on the methods in the Dataset class
    assert(problems.isEmpty, s"\nComparing client jar: $clientJar\nand sql jar: $sqlJar\n" +
      problems.map(p => p.toString).mkString("\n"))
  }

  test("compatibility API tests: Dataset") {
    // Read the client compatibility jar from the classpath
    val clientJar = findJar(clientJarName, sbtClientJarPath)
    assert(clientJar.exists(), clientJar.getAbsolutePath)

    val sqlJar = findJar(sqlJarName, sbtSqlJarPath)
    assert(sqlJar.exists(), sqlJar.getAbsolutePath)

    val clientClassLoader: URLClassLoader =
      new URLClassLoader(Seq(clientJar.toURI.toURL).toArray)

    val sqlClassLoader: URLClassLoader =
      new URLClassLoader(Seq(clientJar.toURI.toURL).toArray)

    // scalastyle:off classforname
    val clientClass = Class.forName(
      "org.apache.spark.sql.Dataset",
      true,
      clientClassLoader)
    val sqlClass = Class.forName(
      "org.apache.spark.sql.Dataset",
      true,
      sqlClassLoader)
    // scalastyle:on classforname

    val newMethods = clientClass.getMethods
    val oldMethods = sqlClass.getMethods

    // For now the new methods is a subset of the old methods,
    // but in the future, the check should be the opposite
    newMethods.map(m => m.toString).foreach(method => {
      assert(oldMethods.map(m => m.toString).contains(method))
    })
  }

  private def findJar(name: String, fallback: String): File = {
    // Find the jar first in the classpath
    val classpath = System.getProperty("java.class.path")
    val jars = classpath.split(":")
    val clientJar = jars.filter(jar => jar.contains(name))

    if(clientJar.nonEmpty) {
      new File(clientJar(0))
    }

    // Fallback to load the jar directly in the artifacts
    // For running tests in Intellij.
    new File(fallback)
  }

  private case class IncludeByName(name: String) extends ProblemFilter {
    private[this] val pattern = Pattern.compile(
      name.split("\\*", -1).map(Pattern.quote).mkString(".*")
    )

    override def apply(problem: Problem): Boolean = {
      pattern.matcher(problem.matchName.getOrElse("")).matches
    }
  }
}
