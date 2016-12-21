/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.fs.mount

import quasar.Predef.{Some, None}
import quasar.Variables
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.fs.FileSystemType
import quasar.sql.Sql

import matryoshka.data.Fix
import monocle.Prism

sealed trait MountRequest {
  import MountRequest._

  def path: APath =
    this match {
      case MountView(f, _, _)       => f
      case MountFileSystem(d, _, _) => d
    }

  def toConfig: MountConfig =
    this match {
      case MountView(_, q, vs)      => MountConfig.viewConfig(q, vs)
      case MountFileSystem(_, t, u) => MountConfig.fileSystemConfig(t, u)
    }
}

object MountRequest {
  final case class MountView private[mount] (
    file: AFile,
    query: Fix[Sql],
    vars: Variables
  ) extends MountRequest

  final case class MountFileSystem private[mount] (
    dir: ADir,
    typ: FileSystemType,
    uri: ConnectionUri
  ) extends MountRequest

  val mountView = Prism[MountRequest, (AFile, Fix[Sql], Variables)] {
    case MountView(f, q, vs) => Some((f, q, vs))
    case _                   => None
  } ((MountView(_, _, _)).tupled)

  val mountFileSystem = Prism[MountRequest, (ADir, FileSystemType, ConnectionUri)] {
    case MountFileSystem(d, t, u) => Some((d, t, u))
    case _                        => None
  } ((MountFileSystem(_, _, _)).tupled)
}
