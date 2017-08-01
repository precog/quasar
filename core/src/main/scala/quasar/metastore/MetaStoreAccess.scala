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
import quasar.fs.mount.{MountConfig, MountType, MountingError}, MountConfig.FileSystemConfig
import quasar.db._

import doobie.imports._
import scalaz._, Scalaz._

/** Operations that access the meta-store via doobie, all wrapped in ConnectionIO
  */
trait MetaStoreAccess {

  //--- Mounts ---
  val fsMounts: ConnectionIO[Map[APath, FileSystemConfig]] =
    Queries.fsMounts.list.map(_.toMap)

  def mountsHavingPrefix(dir: ADir): ConnectionIO[Map[APath, MountType]] =
    Queries.mountsHavingPrefix(dir).list.map(_.toMap)

  def lookupMountType(path: APath): ConnectionIO[Option[MountType]] =
    Queries.lookupMountType(path).option

  def lookupMountConfig(path: APath): ConnectionIO[Option[MountingError \/ MountConfig]] =
    Queries.lookupMountConfig(path).option ∘ (_ ∘ (Mount.toMountConfig))

  def insertMount(path: APath, cfg: MountConfig): ConnectionIO[Unit] =
    runOneRowUpdate(Queries.insertMount(path, cfg))

  def deleteMount(path: APath): NotFoundErrT[ConnectionIO, Unit] =
    runOneRowUpdateOpt(Queries.deleteMount(path)) toRight NotFound

  // NB: H2 stores everything in upper, PosgreSQL in lower, so need to ignore
  // case here.
  def tableExists(name: String): ConnectionIO[Boolean] =
    sql"""select true from information_schema.tables
          where lower(table_schema) = lower('public')
          and lower(table_name) = lower($name)"""
      .query[Boolean].option.map(_.getOrElse(false))

  /** Fail if the update doesn't modify exactly one row. */
  def runOneRowUpdate(update: Update0): ConnectionIO[Unit] =
    runOneRowUpdateOpt(update) getOrElseF connFail("no matching row")

  /** Return `None` if the update doesn't modify exactly one row. */
  def runOneRowUpdateOpt(update: Update0): OptionT[ConnectionIO, Unit] =
    OptionT[ConnectionIO, Unit](update.run flatMap {
      case 0 => none.point[ConnectionIO]
      case 1 => some(()).point[ConnectionIO]
      case _ => connFail("found multiple matching rows")
    })
}

object MetaStoreAccess extends MetaStoreAccess
