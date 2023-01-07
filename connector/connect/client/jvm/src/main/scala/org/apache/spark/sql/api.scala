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

import java.io.Closeable
import java.lang

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.Metadata

class SparkSession extends Serializable with Closeable with Logging {
  def range(n: Long): Dataset[java.lang.Long] = new Dataset[java.lang.Long](this)
  def range(start: Long, end: Long): Dataset[java.lang.Long] = new Dataset[lang.Long](this)
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    new Dataset[lang.Long](this)
  }
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
    new Dataset[lang.Long](this)
  }
  def table(name: String): DataFrame = new Dataset[Row](this)

  private[sql] def version: String = "Spark Connect JVM Client"

  override def close(): Unit = {}
}

class Column extends Logging {
  def as(alias: String): Column = new Column
  def as(aliases: Seq[String]): Column = new Column
  def as(aliases: Array[String]): Column = new Column
  def as(alias: Symbol): Column = new Column
  def as(alias: String, metadata: Metadata): Column = new Column
  def as[U: Encoder]: TypedColumn[Any, U] = new TypedColumn[Any, U]()

  def and(other: Column): Column = new Column
}

class TypedColumn[-T, U] extends Column {}

class Dataset[T](val sparkSession: SparkSession) extends Serializable {
  def as(alias: String): Dataset[T] = new Dataset[T](sparkSession)
  def as(alias: Symbol): Dataset[T] = new Dataset[T](sparkSession)
  def as[U: Encoder]: Dataset[U] = new Dataset[U](sparkSession)

  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = new Dataset[Row](sparkSession)
  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame = new Dataset[Row](sparkSession)
  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] = new Dataset[U1](sparkSession)
  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] = {
    new Dataset[(U1, U2)](sparkSession)
  }
  def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] = new Dataset[(U1, U2, U3)](sparkSession)

  def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] =
    new Dataset[(U1, U2, U3, U4)](sparkSession)

  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] =
    new Dataset[(U1, U2, U3, U4, U5)](sparkSession)

  private[sql] def version: String = "Spark Connect JVM Client"
}
