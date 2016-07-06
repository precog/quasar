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

import simulacrum.typeclass
import scalaz._

@typeclass trait Bucketable[F[_]] {
  type IT[G[_]]

  type Inner = IT[QScriptInternal[IT, ?]]

  // TODO use matryoshka.instances.fixedpoint.Nat
  def digForBucket: F[Inner] => StateT[QScriptBucket[IT, Inner] \/ ?, Int, Inner]
}

object Bucketable {
  type Aux[T[_[_]], F[_]] = Bucketable[F] { type IT[G[_]] = T[G] }

  implicit def coproduct[T[_[_]], F[_], G[_]](
    implicit F: Bucketable.Aux[T, F], G: Bucketable.Aux[T, G]):
      Bucketable.Aux[T, Coproduct[F, G, ?]] =
    new Bucketable[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]

      def digForBucket:
          Coproduct[F, G, Inner] => StateT[QScriptBucket[T, Inner] \/ ?, Int, Inner] =
        _.run.fold(F.digForBucket, G.digForBucket)
    }
}
