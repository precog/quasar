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
import quasar.physical.rdbms.model._
import doobie.imports._

import scalaz._
import Scalaz._

trait PostgresInsert extends RdbmsInsert {

  implicit def dataMeta: Meta[Data]

  def toColValues(cols: Set[ColumnDesc])(
      row: Data)(implicit formatter: DataFormatter): FileSystemError \/ Map[String, String] = {
    row match {
      case Data.Obj(fields) =>
        fields.toVector
          .filter(f => cols.exists(_.name === f._1))
          .map {
            case (n, v) =>
              (n, formatter(n, v))
          }
          .toMap
          .right
      case _ =>
        FileSystemError
          .unsupportedOperation(
            s"Unexpected data row $row not matching model $cols")
          .left
    }
  }

  def buildQuery(chunk: Vector[Data],
                 cols: Set[ColumnDesc],
                 dbPath: TablePath): FileSystemError \/ Vector[Fragment] = {
    val insertIntoTable = fr"insert into " ++ Fragment.const(dbPath.shows)

    chunk.traverse(toColValues(cols)).map { insertColVectors =>
      insertColVectors.map { colMap =>

        val colNamesFragment =
          insertIntoTable ++ fr"(" ++ Fragment.const(
            colMap.keys.mkString(",")) ++
            fr")"

        val colValsFragment =
              fr"values (" ++
          colMap.values.toList
            .map(v => Fragment.const(v))
            .intercalate(fr",") ++ fr")"

        colNamesFragment ++ colValsFragment
      }
    }
  }

  def batchInsert(
      dbPath: TablePath,
      chunk: Vector[Data],
      model: TableModel
  ): ConnectionIO[Vector[FileSystemError]] = {

    model match {
      case JsonTable =>
        val fQuery = fr"(data) values(?::JSON)"
        Update[Data](fQuery.update.sql)
          .updateMany(chunk)
          .map(_ => Vector.empty[FileSystemError])
      case ColumnarTable(cols) =>
        buildQuery(chunk, cols, dbPath)
          .traverse(_.foldMap(_.update.run.void))
          .map {
            case -\/(err) => Vector(err)
            case _        => Vector.empty
          }
    }
  }
}
