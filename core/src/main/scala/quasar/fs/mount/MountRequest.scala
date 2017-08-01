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

package quasar.fs.mount

import slamdata.Predef.List
import quasar.Variables
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.fs.FileSystemType
import quasar.sql.{ScopedExpr, Sql, Statement}

import matryoshka.data.Fix
import monocle.Prism

sealed abstract class MountRequest {
  import MountRequest._

  def path: APath =
    this match {
      case MountView(f, _, _)       => f
      case MountFileSystem(d, _, _) => d
      case MountModule(d, _)        => d
    }

  def toConfig: MountConfig =
    this match {
      case MountView(_, q, vs)      => MountConfig.viewConfig(q, vs)
      case MountFileSystem(_, t, u) => MountConfig.fileSystemConfig(t, u)
      case MountModule(_, s)        => MountConfig.moduleConfig(s)
    }
}

object MountRequest {
  final case class MountView private[mount] (
    file: AFile,
    scopedExpr: ScopedExpr[Fix[Sql]],
    vars: Variables
  ) extends MountRequest

  final case class MountFileSystem private[mount] (
    dir: ADir,
    typ: FileSystemType,
    uri: ConnectionUri
  ) extends MountRequest

  final case class MountModule private[mount] (
    dir: ADir,
    statements: List[Statement[Fix[Sql]]]
  ) extends MountRequest

  val mountView = Prism.partial[MountRequest, (AFile, ScopedExpr[Fix[Sql]], Variables)] {
    case MountView(f, q, vs) => (f, q, vs)
  } ((MountView(_, _, _)).tupled)

  val mountFileSystem = Prism.partial[MountRequest, (ADir, FileSystemType, ConnectionUri)] {
    case MountFileSystem(d, t, u) => (d, t, u)
  } ((MountFileSystem(_, _, _)).tupled)

  val mountModule = Prism.partial[MountRequest, (ADir, List[Statement[Fix[Sql]]])] {
    case MountModule(d, s) => (d, s)
  } ((MountModule(_, _)).tupled)
}
