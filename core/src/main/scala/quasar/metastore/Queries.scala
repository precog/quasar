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

package quasar.metastore

import slamdata.Predef._

import quasar.contrib.pathy.{ADir, APath}
import quasar.fs.mount.{MountConfig, MountType}, MountConfig.FileSystemConfig

import doobie.imports._
import pathy.Path, Path._

/** Raw query and update terms, which may be `checked` against a DB schema
  * without requiring or affecting any data.
  */
trait Queries {
  val fsMounts: Query0[(APath, FileSystemConfig)] =
    sql"SELECT path, type, connectionUri FROM Mounts WHERE type != 'view'".query[(APath, FileSystemConfig)]

  def mountsHavingPrefix(dir: ADir): Query0[(APath, MountType)] = {
    val patternChars = List("\\\\", "_", "%") // NB: Order is important here to avoid double-escaping
    val patternEscaped = patternChars.foldLeft(posixCodec.printPath(dir))((s, c) =>
                           s.replaceAll(c, s"\\\\$c"))
    sql"SELECT path, type FROM Mounts WHERE path LIKE ${patternEscaped + "_%"}"
      .query[(APath, MountType)]
  }

  def lookupMountType(path: APath): Query0[MountType] =
    sql"SELECT type FROM Mounts WHERE path = ${refineType(path)}".query[MountType]

  def lookupMountConfig(path: APath): Query0[Mount] =
    sql"SELECT type, connectionUri FROM Mounts WHERE path = ${refineType(path)}".query[Mount]

  def insertMount(path: APath, cfg: MountConfig): Update0 = {
    val mnt = Mount.fromMountConfig(cfg)
    sql"""INSERT INTO Mounts (path, type, connectionUri)
          VALUES (${refineType(path)}, ${mnt.`type`}, ${mnt.uri.value})
          """.update
  }

  def deleteMount(path: APath): Update0 =
    sql"DELETE FROM Mounts where path = ${refineType(path)}".update
}

object Queries extends Queries
