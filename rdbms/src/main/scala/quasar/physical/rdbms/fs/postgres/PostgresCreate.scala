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
import quasar.physical.rdbms.fs.RdbmsCreate
import quasar.physical.rdbms.common._
import quasar.physical.rdbms.model._

import doobie.syntax.string._
import doobie.free.connection.ConnectionIO
import doobie.util.fragment.Fragment
import scalaz._
import Scalaz._

trait PostgresCreate extends RdbmsCreate {

  override def createSchema(schema: Schema): ConnectionIO[Unit] = {
    (schema :: schema.parents).traverse { s =>
      if (s.isRoot)
        ().point[ConnectionIO]
      else
        (fr"CREATE SCHEMA IF NOT EXISTS" ++ Fragment.const(s.shows)).update.run.void
    }.void
  }

  override def createTable(tablePath: TablePath, model: TableModel): ConnectionIO[Unit] =
    (createSchema(tablePath.schema) *> (fr"CREATE TABLE IF NOT EXISTS" ++ Fragment
      .const(tablePath.shows) ++ fr"(" ++ modelToColumns(model) ++ fr")").update.run).void

  def modelToColumns(model: TableModel): Fragment = {
    model match {
      case JsonTable => Fragment.const("data jsonb NOT NULL")
      case ColumnarTable(cols) =>
        cols.toList.map(c => Fragment.const(s"${c.name} ${c.tpe.mapToStringName}")).intercalate(fr",")
    }
  }

  override def alterTable(tablePath: TablePath, cols: Set[AlterColumn]): ConnectionIO[Unit] = {
    if (cols.isEmpty)
      ().point[ConnectionIO]
    else {
      (fr"ALTER TABLE" ++ Fragment.const(tablePath.shows) ++
        cols.map {
          case AddColumn(name, tpe) => Fragment.const(s"ADD COLUMN $name ${tpe.mapToStringName}")
          case ModifyColumn(name, tpe) => Fragment.const(s"MODIFY COLUMN $name ${tpe.mapToStringName}")
        }.toList.intercalate(fr",")).update.run.void
    }
  }
}
