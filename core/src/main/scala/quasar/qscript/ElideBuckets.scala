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

package quasar.qscript

import quasar.Predef._

import scalaz._
import simulacrum.typeclass

@typeclass trait ElideBuckets[F[_]] {
  type IT[G[_]]

  type InnerPure = IT[QScriptPure[IT, ?]]

  def purify: F[InnerPure] => QScriptPure[IT, InnerPure]
  // #1: F[_]](implicit QSC :<: F, )
  // #2: transformations work across Free
}

object ElideBuckets extends ElideBucketsInstances {
  type Aux[T[_[_]], F[_]] = ElideBuckets[F] { type IT[G[_]] = T[G] }
}

trait ElideBucketsInstances0 {
  implicit def inject[T[_[_]], F[_]](implicit F: F :<: QScriptPure[T, ?]):
      ElideBuckets.Aux[T, F] =
    new ElideBuckets[F] {
      type IT[K[_]] = T[K]

      def purify: F[InnerPure] => QScriptPure[IT, InnerPure] = F.inj
    }
}

trait ElideBucketsInstances extends ElideBucketsInstances0 {
  implicit def coproduct[T[_[_]], F[_], G[_]](
    implicit F: ElideBuckets.Aux[T, F], G: ElideBuckets.Aux[T, G]):
      ElideBuckets.Aux[T, Coproduct[F, G, ?]] =
    new ElideBuckets[Coproduct[F, G, ?]] {
      type IT[K[_]] = T[K]
  
      def purify: Coproduct[F, G, InnerPure] => QScriptPure[IT, InnerPure] =
        _.run.fold(F.purify, G.purify)
    }
}
