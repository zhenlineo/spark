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

class SparkSession {
  def range(n: Long): Dataset[java.lang.Long] = new Dataset[java.lang.Long](this)
  def table(name: String): DataFrame = new Dataset[Row](this)

  private[sql] def version: String = "Spark Connect JVM Client"
}

class Column {
  def as(alias: String): Column = new Column
  def and(other: Column): Column = new Column
}

class Dataset[T](val sparkSession: SparkSession) {
  def as[U: Encoder]: Dataset[U] = new Dataset[U](sparkSession)
  def select(cols: Column*): DataFrame = new Dataset[Row](sparkSession)

  private[sql] def version: String = "Spark Connect JVM Client"
}
