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
import scalaz.{Applicative, Coproduct, Monad}

abstract class MapFuncPlanner[T[_[_]], F[_]: Applicative, QS[_]] {
  def plan(cake: Precog): AlgebraM[F, QS, cake.trans.TransSpec1]
}

object MapFuncPlanner {
  def apply[T[_[_]], F[_], QS[_]]
    (implicit ev: MapFuncPlanner[T, F, QS]): MapFuncPlanner[T, F, QS] =
    ev

  implicit def coproduct[T[_[_]], F[_]: Applicative, G[_], H[_]]
    (implicit G: MapFuncPlanner[T, F, G], H: MapFuncPlanner[T, F, H])
      : MapFuncPlanner[T, F, Coproduct[G, H, ?]] =
    new MapFuncPlanner[T, F, Coproduct[G, H, ?]] {
      def plan(cake: Precog): AlgebraM[F, Coproduct[G, H, ?], cake.trans.TransSpec1] =
        _.run.fold(G.plan(cake), H.plan(cake))
    }

  implicit def mapFuncCore[T[_[_]]: RecursiveT, F[_]: Applicative]
    : MapFuncPlanner[T, F, MapFuncCore[T, ?]] =
    new MapFuncCorePlanner[T, F]

  implicit def mapFuncDerived[T[_[_]]: BirecursiveT, F[_]: Monad]
    : MapFuncPlanner[T, F, MapFuncDerived[T, ?]] =
    new MapFuncDerivedPlanner[T, F](mapFuncCore)
}
