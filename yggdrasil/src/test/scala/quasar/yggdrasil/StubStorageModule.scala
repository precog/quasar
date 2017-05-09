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

package quasar.yggdrasil

import quasar.blueeyes._
import quasar.precog.common._
import scalaz._

trait StubProjectionModule[M[+_], Block] extends ProjectionModule[M, Block] { self =>
  implicit def M: Monad[M]

  protected def projections: Map[Path, Projection]

  class ProjectionCompanion extends ProjectionCompanionLike[M] {
    def apply(path: Path) = M.point(projections.get(path))
  }
}
