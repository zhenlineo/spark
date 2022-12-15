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

// The impl in this file is just a placeholder for the real classes. We use them to setup the basic
// shading and test tools.
class SparkSession {
    def range(n: Long): Dataset[java.lang.Long] = new Dataset[java.lang.Long](this)
    def table(name: String): Dataset[Row] = new Dataset[Row](this)
}

class Column {
    def as(alias: String): Column = new Column
    def and(other: Column): Column = new Column
}

class Dataset[T](val sparkSession: SparkSession) {
    def as(alias: String): Dataset[T] = new Dataset[T](sparkSession)
    def select(cols: Column*): Dataset[Row] = new Dataset[Row](sparkSession)
}

// This is a stub for sql.Row, We use this for now until the sql/api refactoring is done.
class Row {
}
