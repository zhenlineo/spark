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
package org.apache.spark.sql

import java.io.{BufferedWriter, File, FileNotFoundException, FileWriter}

import scala.io.Source

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class ClassLoadingSuite extends AnyFunSuite { // scalastyle:ignore funsuite
  test("test loading the correct Dataset class") {
    val ds = new Dataset[Int](null)
    assert(ds.version.contains("Spark Connect JVM Client"))
  }

  test("test loading the correct SparkSession class") {
    val session = new SparkSession()
    assert(session.version.contains("Spark Connect JVM Client"))
  }

  test("assert no new methods") {
    val clsLoader = Thread.currentThread().getContextClassLoader
    val methods = GenerateGoldenFile.clsNames
      .flatMap(clsName => {
        val cls = clsLoader.loadClass(clsName)
        cls.getMethods.map(m => s"$clsName.${m.getName}").toSet
      })
      .toSet

    val source = Source.fromFile(GenerateGoldenFile.goldenFile)
    try {
      val readMethods = source.getLines.toSet
      assert(readMethods == methods)
    } finally {
      source.close()
    }
  }
}

// This class can be run via IntelliJ to update the `client/jvm/resources/api.txt` file
object GenerateGoldenFile {

  val clsNames = Seq(
    "org.apache.spark.sql.Dataset",
    "org.apache.spark.sql.SparkSession",
    "org.apache.spark.sql.Column")
  private val filename = "resources/api.txt"
  private val parent = "connector/connect/client/jvm"

  def goldenFile: File = {
    var file = new File(parent, filename)
    if (!file.exists()) {
      file = new File(filename)
      if (!file.exists()) {
        throw new FileNotFoundException(file.getAbsolutePath)
      }
    }
    file
  }

  def main(args: Array[String]): Unit = {
    val file = goldenFile
    if (file.exists()) {
      file.delete()
    }
    val writer = new BufferedWriter(new FileWriter(file))
    try {
      val clsLoader = Thread.currentThread().getContextClassLoader
      clsNames.foreach { clsName =>
        val cls = clsLoader.loadClass(clsName)
        val methodSet: Set[String] = cls.getMethods.map(m => m.getName).toSet
        methodSet.foreach { methodName =>
          writer.write(s"$clsName.$methodName\n")
        }
      }
    } finally {
      writer.close()
    }
  }
}
