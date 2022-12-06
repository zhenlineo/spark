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

import org.apache.spark.sql.api.{Column => ColumnAPI}
import org.apache.spark.sql.api.{Dataset => DatasetAPI}
import org.apache.spark.sql.api.{SparkSession => SparkSessionAPI}
import org.apache.spark.sql.api.{Row => RowAPI}

class SparkSession extends SparkSessionAPI {
    override type DS[_] = Dataset[_]
    override def range(n: Long): Dataset[java.lang.Long] = new Dataset[java.lang.Long](this)
    override def table(name: String): Dataset[Row] = new Dataset[Row](this)
}

class Column extends ColumnAPI {
    override type COL = Column
    override def as(alias: String): Column = new Column
    override def and(other: Column): Column = new Column
}

class Dataset[T](override val sparkSession: SparkSession) extends DatasetAPI[SparkSession, T] {
    override type COL = Column
    override type DS[_] = Dataset[_]

    override def as(alias: String): Dataset[T] = new Dataset[T](sparkSession)
    override def select(cols: Column*): Dataset[Row] = new Dataset[Row](sparkSession)
}

class Row extends RowAPI {
}
