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
package org.apache.spark.sql.connect

import java.lang

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.api.{SparkSession => SparkSessionAPI}

class APITestSuite extends SparkFunSuite {
    test("compatibility API tests") {
        val session: SparkSession = new SparkSession()
        val ds : Dataset[java.lang.Long] = session.range(10)
        ds.select(new Column().as("col"))
    }

    test("API tests") {
        val session: SparkSessionAPI = new SparkSession()
        val ds: session.DS[lang.Long] = session.range(10)
    }
}
