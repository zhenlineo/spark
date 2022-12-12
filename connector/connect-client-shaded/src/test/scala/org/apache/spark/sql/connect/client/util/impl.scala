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
package org.apache.spark.sql.connect.client.util

import java.util.HashMap

import org.apache.spark.SparkContext
import org.mockito.Mockito.mock

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
 * The classes in this file define the "real" Spark SQL 4.0 API.
 * In order to ensure we test the API compatibility of Spark SQL API and Spark Connect Scala Client,
 * we wrap the two implmentations behind a common API.
 *
 * @param classLoader
 */
class SparkSessionImpl(val classLoader: ClassLoader)
        extends SparkSession(mock(classOf[SparkContext]), new HashMap[String, String]()) {
    // scalastyle:off classforname
    val classToLoad: Class[_] = Class.forName(
        "org.apache.spark.sql.SparkSession",
        true,
        classLoader)
    // scalastyle:on classforname
    val sessionClass: Any = classToLoad.getConstructor().newInstance()

    override def range(n: Long): Dataset[java.lang.Long] = {
        val range = classToLoad.getDeclaredMethod("range")
        range.invoke(sessionClass, Long.box(n)).asInstanceOf[Dataset[java.lang.Long]]
    }

    override def table(name: String): DataFrame = {
        val table = classToLoad.getDeclaredMethod("table")
        table.invoke(sessionClass, name).asInstanceOf[DataFrame]
    }
}

class ColumnImpl(val classLoader: ClassLoader) extends Column("") {
    // scalastyle:off classforname
    val classToLoad: Class[_] = Class.forName(
        "org.apache.spark.sql.Column",
        true,
        classLoader)
    // scalastyle:on classforname
    val columnClass: Any = classToLoad.getConstructor().newInstance()

    override def as(alias: String): Column = {
        val as = classToLoad.getDeclaredMethod("as")
        as.invoke(columnClass, alias).asInstanceOf[Column]
    }

    override def and(other: Column): Column = {
        val and = classToLoad.getDeclaredMethod("as")
        and.invoke(columnClass, other).asInstanceOf[Column]
    }
}
