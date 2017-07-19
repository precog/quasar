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
import quasar.fs.mount._, MountConfig._, MountType._
import quasar.sql

import scalaz._, Scalaz._

final case class Mount(`type`: MountType, uri: ConnectionUri)

object Mount {
  val fromMountConfig: MountConfig => Mount = {
    case ViewConfig(query, vars) =>
      Mount(ViewMount, viewCfgAsUri(query, vars))
    case ModuleConfig(statements) =>
      Mount(ModuleMount, stmtsAsSqlUri(statements))
    case FileSystemConfig(typ, uri) =>
      Mount(FileSystemMount(typ), uri)
  }

  val toMountConfig: Mount => MountingError \/ MountConfig = {
    case Mount(ViewMount, uri) =>
      viewCfgFromUri(uri).fold(
        e => MountingError.invalidMount(
          ViewMount,
          s"error while obtaining a view config for ${uri.value}, $e").left,
        viewConfig(_).right)
    case Mount(ModuleMount, uri) =>
      sql.fixParser.parseModule(uri.value).fold(
        e => MountingError.invalidMount(
          ModuleMount,
          s"error while obtaining a mount config for ${uri.value}, ${e.message}").left,
        moduleConfig(_).right)
    case Mount(FileSystemMount(fsType), uri) =>
      fileSystemConfig(fsType, uri).right
  }
}
