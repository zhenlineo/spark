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

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.plugin.MimaKeys.{mimaBinaryIssueFilters, mimaFailOnNoPrevious, mimaPreviousArtifacts}
import java.io.FileNotFoundException
import java.net.URLClassLoader
import java.util.regex.Pattern
import sbt._
import sbt.Keys.{baseDirectory, version}
import sbt.internal.inc.ScalaInstance
import scala.io.Source

// Used for spark connect client API compatibility check with sql API.
// previous artifact: spark-sql.jar
// current artifact: spark-connect-client-jvm.jar
object ClientMimaBuild {
  def mimaSettings: Seq[Setting[_]] = {

    val organization = "org.apache.spark"
    val preId = "spark-sql"

    Seq(
      mimaFailOnNoPrevious := true,
      mimaPreviousArtifacts := Set(organization %% preId % version.value),
      mimaBinaryIssueFilters ++= Seq(IncludeAny( Seq(
        IncludeByName("org.apache.spark.sql.Dataset"),
        IncludeByName("org.apache.spark.sql.SparkSession"),
        IncludeByName("org.apache.spark.sql.DataFrame"),
        IncludeByName("org.apache.spark.sql.Column"),

      ) ++ includeMethods(new File(baseDirectory.value, "resources/api.txt"))
      )) ++ notSupported()
    )
  }

  // We start with include filters for as the supported methods is limited.
  // When the API is stable, we should switch to exclude filters.
  private def includeMethods(file: File): Seq[ProblemFilter] = {
    if (!file.exists()) {
      throw new FileNotFoundException(file.getAbsolutePath)
    }
    val source = Source.fromFile(file)
    source.getLines().map(method => {
      IncludeByName(method)
    }).toSeq
  }

  // I cannot use classLoader inside SBT?
  // Use a class loader to automatically include the implemented methods in the MiMa check
  private def includeMethods(
      className: String,
      file: File, scalaInstance: ScalaInstance): Seq[ProblemFilter] = {
    if (!file.exists()) {
      throw new FileNotFoundException("Cannot found the jar: " + file.getAbsolutePath)
    }
//    def disassembled = Process(s"javap -cp ${file.getAbsolutePath} $className").!!
    val loader = new URLClassLoader(Array(file.toURI.toURL), scalaInstance.loaderLibraryOnly)
//    val resource = loader.getResource(className.replace(".", "/") + ".class")
//    if (resource == null) {
//      throw new RuntimeException(disassembled)
//    }
    val cls = loader.loadClass(className)
    cls.getMethods.map(m => m.getName).map(n => IncludeByName(s"$className.$n"))
  }

  private def notSupported(): Seq[ProblemFilter] = {
    Seq(
      // private[sql] methods
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Dataset.ofRows"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Dataset.DATASET_ID_TAG"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Dataset.COL_POS_KEY"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Dataset.DATASET_ID_KEY"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Dataset.curId"),
    )
  }

  /**
   * Define an include filter for MiMa result. This filter accepts a sequence of include
   * filters and keep the result that matches any of the filters in the sequence.
   */
  private case class IncludeAny(includeFilters: Seq[ProblemFilter]) extends ProblemFilter {
    override def apply(problem: Problem): Boolean = {
      includeFilters.exists(rule => rule(problem))
    }
  }

  /**
   * Define an include filter: Given a [[Problem]], it returns True if the filter pattern matches.
   *
   * @param name the matching pattern. e.g.:
   * class - "org.apache.spark.sql.Dataset"
   * companion class - "org.apache.spark.sql.Dataset$"
   * All methods in the class - "org.apache.spark.sql.Dataset.*"
   */
  private case class IncludeByName(name: String) extends ProblemFilter {
    private[this] val pattern = Pattern.compile(
      name.split("\\*", -1).map(Pattern.quote).mkString(".*")
    )

    override def apply(problem: Problem): Boolean = {
      pattern.matcher(problem.matchName.getOrElse("")).matches
    }
  }
}
