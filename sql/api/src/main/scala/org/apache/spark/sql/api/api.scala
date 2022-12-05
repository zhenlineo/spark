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

import scala.language.higherKinds

trait SparkSession {
    type DS[_] <: Dataset[_ >: this.type <: SparkSession, _]
    def range(n: Long): DS[java.lang.Long]
    def table(name: String): DS[_ <: Row]
}

trait Column {
    type COL >: this.type <: Column
    def as(alias: String): COL
    def and(other: COL): COL
}


trait Dataset[S <: SparkSession, T] {
    type COL <: Column
//    type S <: SparkSession
    type DS[_] >: this.type <: Dataset[S, _]
    def sparkSession: S
    def as(alias: String): DS[T]
    def select(cols: COL*): DS[_ <: Row]
}

// A placeholder for the sql.Row class to test out shading.
trait Row {
}
