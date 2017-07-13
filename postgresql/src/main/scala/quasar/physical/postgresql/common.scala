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

package quasar.physical.postgresql

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.fs._

import doobie.imports._
import pathy.Path, Path._
import scalaz._, Scalaz._

object common {

  final case class DbTable(db: String, table: String)

  def dbTableFromPath(f: APath): FileSystemError \/ DbTable =
    Path.flatten(None, None, None, Some(_), Some(_), f)
      .toIList.unite.uncons(
        FileSystemError.pathErr(PathError.invalidPath(f, "no database specified")).left,
        (h, t) => DbTable(h, t.intercalate("/")).right)

  def pathSegmentsFromPrefix(
    tableNamePathPrefix: String, tableNamePaths: List[String]
  ): Set[PathSegment] =
    tableNamePaths
      .map(_.stripPrefix(tableNamePathPrefix).stripPrefix("/").split("/").toList)
      .collect {
        case h :: Nil => FileName(h).right
        case h :: _   => DirName(h).left
      }.toSet

  def tableExists[S[_]](tableName: String): ConnectionIO[Boolean] =
    sql"""select table_name from information_schema.tables
          where table_name = $tableName"""
          .query[String].list.map(_.nonEmpty)

  def tablesWithPrefix(tableNamePrefix: String): ConnectionIO[List[String]] = {
    // NB: Order important to avoid double-escaping
    // TODO: Duplicated from Quasar Advanced. Relocate to a common location.
    val patternChars = List("\\\\", "_", "%")

    val patternEscaped =
      patternChars.foldLeft(tableNamePrefix)((s, c) => s.replaceAll(c, s"\\\\$c"))

    sql"""select table_name from information_schema.tables
          where table_name like ${patternEscaped + "%"}"""
          .query[String].list
  }

}
