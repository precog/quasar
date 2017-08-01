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

package quasar.mimir

import quasar.qscript.{MapFuncCore, MapFuncDerived}

import matryoshka.{AlgebraM, BirecursiveT, RecursiveT}
import matryoshka.{AlgebraM, RecursiveT}
import scalaz.{Applicative, Coproduct, Monad}

abstract class MapFuncPlanner[T[_[_]], F[_]: Applicative, MF[_]] {
  def plan(cake: Precog): PlanApplicator[cake.type]

  // this class is necessary so the [A] param can be to the right of the (cake) param
  abstract class PlanApplicator[P <: Precog](val cake: P) {
    import cake.trans._

    def apply[A <: SourceType](id: cake.trans.TransSpec[A]): AlgebraM[F, MF, TransSpec[A]]
  }
}

object MapFuncPlanner {
  def apply[T[_[_]], F[_], MF[_]]
    (implicit ev: MapFuncPlanner[T, F, MF]): MapFuncPlanner[T, F, MF] =
    ev

  implicit def coproduct[T[_[_]], F[_]: Applicative, G[_], H[_]]
    (implicit G: MapFuncPlanner[T, F, G], H: MapFuncPlanner[T, F, H])
      : MapFuncPlanner[T, F, Coproduct[G, H, ?]] =
    new MapFuncPlanner[T, F, Coproduct[G, H, ?]] {
      def plan(cake: Precog): PlanApplicator[cake.type] =
        new PlanApplicatorCoproduct(cake)

      final class PlanApplicatorCoproduct[P <: Precog](override val cake: P)
        extends PlanApplicator[P](cake) {
        import cake.trans._

        def apply[A <: SourceType](id: cake.trans.TransSpec[A]): AlgebraM[F, Coproduct[G, H, ?], TransSpec[A]] =
          _.run.fold(G.plan(cake).apply(id), H.plan(cake).apply(id))
      }

    }

  implicit def mapFuncCore[T[_[_]]: RecursiveT, F[_]: Applicative]
    : MapFuncPlanner[T, F, MapFuncCore[T, ?]] =
    new MapFuncCorePlanner[T, F]

  implicit def mapFuncDerived[T[_[_]]: BirecursiveT, F[_]: Monad]
    : MapFuncPlanner[T, F, MapFuncDerived[T, ?]] =
    new MapFuncDerivedPlanner[T, F](mapFuncCore)
}
