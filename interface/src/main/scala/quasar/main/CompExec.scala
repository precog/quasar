/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.main

import quasar.common.{PhaseResultT, PhaseResultW}
import quasar.compile.SemanticErrsT
import quasar.fp.{liftMT, pointNT}
import quasar.fs.{FileSystemErrT, QueryFile}

import scalaz.{~>, Hoist, Monad}
import scalaz.std.vector._

class CompExec[F[_]: Monad] extends QueryFile.Transforms[F] {
  type H[A] = SemanticErrsT[G, A]
  type CompileM[A] = SemanticErrsT[PhaseResultW, A]
  type CompExecM[A] = FileSystemErrT[H, A]

  val execToCompExec: ExecM ~> CompExecM =
      Hoist[FileSystemErrT].hoist[G, H](liftMT[G, SemanticErrsT])

  val compToCompExec: CompileM ~> CompExecM = {
    val hoistW: PhaseResultW ~> G = Hoist[PhaseResultT].hoist(pointNT[F])
    val hoistC: CompileM ~> H     = Hoist[SemanticErrsT].hoist(hoistW)
    liftMT[H, FileSystemErrT] compose hoistC
  }

  val toCompExec: F ~> CompExecM =
    execToCompExec compose toExec
}

object CompExec {
  def apply[F[_]: Monad]: CompExec[F] =
    new CompExec[F]
}
