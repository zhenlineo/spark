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
package org.apache.spark.sql.api

trait SparkSession {

    def range(n: Long): Dataset[java.lang.Long]

    def table(name: String): Dataset[_ <: Row]
}

trait Column {
    // TODO figure out a way to tie this to the self type.
    type COL <: Column

    def as(alias: String): COL

    def and(other: COL): COL
}


trait Dataset[T] {
    type COL <: Column

    def sparkSession: SparkSession

    def as(alias: String): Dataset[T]

    def select(cols: COL*): Dataset[_ <: Row]
}

// A placeholder for the sql.Row class to test out shading.
trait Row {
}
