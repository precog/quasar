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
import quasar.sql.Expr
import quasar.fs.{AFile, ADir, APath, FileSystemType}
import quasar.fp.prism._

import monocle.Prism

sealed trait MountRequest {
  import MountRequest._

  def path: APath =
    this match {
      case MountView(f, _, _)       => f
      case MountFileSystem(d, _, _) => d
    }

  def toConfig: MountConfig2 =
    this match {
      case MountView(_, q, vs)      => MountConfig2.viewConfig(q, vs)
      case MountFileSystem(_, t, u) => MountConfig2.fileSystemConfig(t, u)
    }
}

object MountRequest {
  final case class MountView private[mount] (
    file: AFile,
    query: Expr,
    vars: Variables
  ) extends MountRequest

  final case class MountFileSystem private[mount] (
    dir: ADir,
    typ: FileSystemType,
    uri: ConnectionUri
  ) extends MountRequest

  val mountView = Prism[MountRequest, (AFile, Expr, Variables)] {
    case MountView(f, q, vs) => Some((f, q, vs))
    case _                   => None
  } ((MountView(_, _, _)).tupled)

  val mountFileSystem = Prism[MountRequest, (ADir, FileSystemType, ConnectionUri)] {
    case MountFileSystem(d, t, u) => Some((d, t, u))
    case _                        => None
  } ((MountFileSystem(_, _, _)).tupled)
}
