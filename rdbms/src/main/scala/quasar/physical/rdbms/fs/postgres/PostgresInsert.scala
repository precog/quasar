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
import quasar.Data
import quasar.fs.FileSystemError
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.fs.RdbmsInsert

import doobie.free.connection.ConnectionIO
import doobie.imports.{Meta, Update}
import doobie.syntax.string._
import doobie.util.fragment.Fragment
import scalaz.std.vector._
import scalaz.syntax.show._

trait PostgresInsert extends RdbmsInsert {

  implicit def dataMeta: Meta[Data]

  def batchInsert(
      dbPath: TablePath,
      chunk: Vector[Data]
  ): ConnectionIO[Vector[FileSystemError]] = {

    val fQuery = fr"insert into " ++ Fragment.const(dbPath.shows) ++ fr"(data) values(?::JSON)"

    Update[Data](fQuery.update.sql).updateMany(chunk).map(_ => Vector.empty[FileSystemError])
  }
}
