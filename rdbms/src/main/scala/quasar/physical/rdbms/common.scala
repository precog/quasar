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

package quasar.physical.rdbms

import slamdata.Predef._
import pathy.Path
import pathy.Path.DirName
import quasar.contrib.pathy.{ADir, AFile}
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo

import monocle.Prism
import scalaz._
import Scalaz._

object common {

  final case class Config(connInfo: JdbcConnectionInfo)

  sealed trait Schema

  final case object DefaultSchema extends Schema

  final case class CustomSchema(name: String) extends Schema

  object Schema {
    def lastDirName(schema: Schema): DirName = {
      schema match {
        case DefaultSchema => DirName("")
        case CustomSchema(name) =>
          val lastSeparatorIndex = name.lastIndexOf(TablePath.Separator)
          val startIndex =
            if (lastSeparatorIndex > 0) lastSeparatorIndex + TablePath.Separator.length else 0
          DirName(name.substring(startIndex))
      }
    }

    val default = Prism.partial[Schema, DefaultSchema.type] {
      case DefaultSchema => DefaultSchema
    }(scala.Predef.identity)

    val custom = Prism.partial[Schema, CustomSchema] {
      case c: CustomSchema => c
    }(scala.Predef.identity)

    implicit val showCustomSchema: Show[CustomSchema] = Show.shows(_.name.toLowerCase)
  }

  final case class TableName(name: String) extends AnyVal
  final case class TablePath(schema: Schema, table: TableName)

  object TablePath {

    val Separator = "__c_"
    val SeparatorRegex = "__c_"

    def dirToSchema(dir: ADir): Schema = {
      Path
        .flatten(None, None, None, Some(_), Some(_), dir)
        .toIList
        .unite
        .intercalate(Separator) match {
        case "" => DefaultSchema
        case pathStr => CustomSchema(pathStr)
      }
    }

    def create(file: AFile): TablePath = {
      val filename = Path.fileName(file).value
      val schema = Path.parentDir(file).map(dirToSchema).getOrElse(DefaultSchema)
      new TablePath(schema, TableName(filename))
    }

    implicit val showTableName: Show[TableName] =
      Show.shows(_.name.toLowerCase)

    implicit val showPath: Show[TablePath] = Show.shows { tp =>
      tp.schema match {
        case DefaultSchema => tp.table.shows
        case s: CustomSchema => s"${s.shows}.${tp.table.shows}"
      }
    }
  }
}
