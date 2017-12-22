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

package quasar.effect

import slamdata.Predef._
import quasar.fp._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

sealed abstract class Write[W, A]

object Write {
  final case class Tell[W](w: W) extends Write[W, Unit]

  final class Ops[W, S[_] <: ACopK](implicit S: Write[W, ?] :<<: S) extends LiftedOps[Write[W, ?], S] {
    def tell(w: W): FreeS[Unit] = lift(Tell(w))
  }

  object Ops {
    implicit def apply[W, S[_] <: ACopK](implicit S: Write[W, ?] :<<: S): Ops[W, S] = new Ops[W, S]
  }

  def fromTaskRef[W: Semigroup](tr: TaskRef[W]): Write[W, ?] ~> Task =
    λ[Write[W, ?] ~> Task] {
      case Tell(w)   => tr.modify(_  ⊹ w).void
    }
}
