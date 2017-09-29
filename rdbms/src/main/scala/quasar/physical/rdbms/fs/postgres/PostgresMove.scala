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

package quasar.physical.rdbms.fs.postgres

import slamdata.Predef._
import quasar.physical.rdbms.common.{CustomSchema, Schema, TableName, TablePath}
import quasar.physical.rdbms.fs.RdbmsMove
import quasar.physical.rdbms.common.TablePath._

import doobie.imports.ConnectionIO
import doobie.syntax.string._
import doobie.util.fragment.Fragment
import scalaz._
import Scalaz._

trait PostgresMove extends RdbmsMove {

  override def dropTableIfExists(table: TablePath): ConnectionIO[Unit] =
    (fr"DROP TABLE IF EXISTS" ++ Fragment.const(table.shows)).update.run.void

  override def moveTableToSchema(table: TablePath, dst: Schema): ConnectionIO[TablePath] =
    (fr"ALTER TABLE" ++ Fragment.const(table.shows) ++ fr"SET SCHEMA" ++ Fragment.const(
      dst.shows)).update.run.map(_ => table.copy(schema = dst))

  override def renameTable(table: TablePath, newName: TableName): ConnectionIO[TablePath] =
    (fr"ALTER TABLE" ++ Fragment.const(table.shows) ++ fr"RENAME TO" ++ Fragment.const(
      newName.shows)).update.run.map(_ => table.copy(table = newName))

  override def renameSchema(schema: CustomSchema, newName: CustomSchema): ConnectionIO[Unit] =
    (fr"ALTER SCHEMA" ++ Fragment.const(schema.shows) ++ fr"RENAME TO" ++ Fragment.const(
      newName.shows)).update.run.void

}
