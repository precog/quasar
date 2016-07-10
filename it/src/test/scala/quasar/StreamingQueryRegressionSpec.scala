/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.fs.ADir
import quasar.regression._
import quasar.sql.Sql

import scala.None

import eu.timepit.refined.auto._
import matryoshka.{Fix}

class StreamingQueryRegressionSpec
  extends QueryRegressionTest[FileSystemIO](QueryRegressionTest.externalFS) {

  val suiteName = "Streaming Queries"

  def queryResults(expr: Fix[Sql], vars: Variables, basePath: ADir) =
    fsQ.evaluateQuery(expr, vars, basePath, 0L, None)
}
