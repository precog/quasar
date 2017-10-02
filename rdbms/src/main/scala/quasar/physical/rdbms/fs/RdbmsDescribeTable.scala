/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.rdbms.fs

import slamdata.Predef._
import quasar.physical.rdbms.common.{CustomSchema, Schema, TableName, TablePath}

import doobie.imports.ConnectionIO

trait RdbmsDescribeTable {

  def findChildSchemas(parent: Schema): ConnectionIO[Vector[CustomSchema]]
  def findChildTables(schema: Schema): ConnectionIO[Vector[TableName]]
  def schemaExists(schema: Schema): ConnectionIO[Boolean]
  def tableExists(tablePath: TablePath): ConnectionIO[Boolean]
}
