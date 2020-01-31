/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.qsu
package minimizers

import slamdata.Predef._
import quasar.common.effect.NameGenerator
import quasar.qscript.MonadPlannerErr

import scalaz.Monad

trait Minimizer[T[_[_]]] extends QSUTTypes[T] {
  import MinimizeAutoJoins.MinStateM

  type P

  def couldApplyTo(candidates: List[QSUGraph]): Boolean

  /** Returns what is essentially a lens focused on the first ancestor of `qgraph`
    * _not_ applicable to this `Minimizer`. The function component describes how
    * to rebuild the semantic equivalent of `qgraph` given a (possibly) new ancestor
    * and `FreeMap` to apply to its output.
    */
  def extract[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, P, ?[_]]](
      qgraph: QSUGraph)
      : Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])]

  /** The first component of the tuple is the rewrite target on any provenance
    * association, i.e. the semantic equivalent of the input. The second component
    * is the root of the resulting graph.
    */
  def apply[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, P, ?[_]]](
      qgraph: QSUGraph,
      singleSource: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int])
      : G[Option[(QSUGraph, QSUGraph)]]
}

object Minimizer {
  type Aux[T[_[_]], P0] = Minimizer[T] { type P = P0 }
}
