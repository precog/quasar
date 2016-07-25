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

package quasar.physical.postgresql

import quasar.fs._
import quasar.fs.mount.FileSystemDef, FileSystemDef.DefErrT

import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object fs {
  val FsType = FileSystemType("postgresql")

  def definition[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S):
      FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        FileSystemDef.DefinitionResult[Free[S, ?]](
          interpretFileSystem(
            Empty.queryFile[Free[S, ?]],
            Empty.readFile[Free[S, ?]],
            Empty.writeFile[Free[S, ?]],
            Empty.manageFile[Free[S, ?]]),
          Free.point(())).point[DefErrT[Free[S, ?], ?]]
    }
}
