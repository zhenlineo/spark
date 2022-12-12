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

trait APICompatibilityBase extends SparkFunSuite {
    def jarFile: File
    val classLoader: URLClassLoader = new URLClassLoader(
        Seq(jarFile.toURI.toURL).toArray, this.getClass.getClassLoader)
}


class APICompatibilitySuite extends SparkFunSuite {
    val shadedJar = new File(
        "/Users/zhen.li/code/spark/connector/connect-client/" +
                "target/spark-connect-client_2.12-3.4.0-SNAPSHOT-compatibility.jar")
    val classLoader: URLClassLoader = new URLClassLoader(
        Seq(shadedJar.toURI.toURL).toArray, this.getClass.getClassLoader)

    test("compatibility API tests") {
        val classToLoad = Class.forName(
            "org.apache.spark.sql.SparkSession",
            true,
            classLoader)
        val range = classToLoad.getDeclaredMethod("range")
        //        val session = new SparkSession()
        val session = classToLoad.getConstructor().newInstance()
        //        val ds = session.range(10)
        val ds = range.invoke(session, Int.box(10))
//        ds.select(new Column().as("col"))
    }
}
