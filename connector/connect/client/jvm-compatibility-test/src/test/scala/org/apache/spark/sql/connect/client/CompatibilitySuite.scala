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

import org.apache.spark.SparkFunSuite

import com.typesafe.tools.mima.lib.MiMaLib

// Pros: The test is both accessible by SBT and Maven
// Cons: Added a new module just for testing, will we end up release a empty module?
class CompatibilitySuite extends SparkFunSuite {
  val clientJarName = "spark-connect-client-jvm-compatibility"

  test("compatibility mima tests") {
    // Read the client compatibility jar from the classpath
    val classpath = System.getProperty("java.class.path")
    val jars = classpath.split(":")
    val clientJar = jars.filter(jar => jar.contains(clientJarName))
    assert(clientJar.length == 1, "Classpath:\n" + jars.mkString("\n"))
    val sqlJar = jars.filter(jar => jar.contains("spark-sql"))
    assert(sqlJar.length == 1, "Classpath:\n" + jars.mkString("\n"))

    val cp = Seq(clientJar(0), sqlJar(0)).map(jar => new File(jar))
    val mima = new MiMaLib(cp)
    val problems = mima.collectProblems(cp(0), cp(1), List.empty)
    throw new RuntimeException(problems.map(p => p.toString).mkString("\n"))
  }

  test("compatibility API tests") {
    // Read the client compatibility jar from the classpath
    val classpath = System.getProperty("java.class.path")
    val jars = classpath.split(":")
    val clientJar = jars.filter(jar => jar.contains(clientJarName))
    assert(clientJar.length == 1)
    val classLoader: URLClassLoader =
      new URLClassLoader(Seq(new File(clientJar(0)).toURI.toURL).toArray)

    // Do something with the loaded jar
    // scalastyle:off classforname
    val classToLoad = Class.forName(
      "org.apache.spark.sql.Dataset",
      true,
      classLoader)
    // scalastyle:on classforname
    val methods = classToLoad.getMethods
    assert(methods.map(m => m.toString).contains(
      "public org.apache.spark.sql.Row[] org.apache.spark.sql.Dataset.collect()"
    ))
  }
}