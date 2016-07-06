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
import quasar.fp._
import quasar.qscript.MapFunc._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import matryoshka.patterns._
import scalaz._
import simulacrum.typeclass

@typeclass trait Normalizable[F[_]] {
  def normalize: F ~> F

  // NB: This is overly complicated due to the use of Free instead of Mu[CoEnv]
  def normalizeMapFunc[T[_[_]]: Recursive: Corecursive: EqualT, A](fm: Free[MapFunc[T, ?], A]):
      Free[MapFunc[T, ?], A] =
    fm.ana[Mu, CoEnv[A, MapFunc[T, ?], ?]](CoEnv.freeIso[A, MapFunc[T, ?]].reverseGet)
      .transCata[CoMF[T, A, ?]](repeatedly(MapFunc.normalize))
      .cata(CoEnv.freeIso[A, MapFunc[T, ?]].get)
}

trait NormalizableInstances0 {
  /** This case matches _everything_. I.e., if something _isn’t_ normalizable,
    * then normalization is identity.
    */
  implicit def default[F[_]]: Normalizable[F] = new Normalizable[F] {
    def normalize = new (F ~> F) {
      def apply[A](sp: F[A]) = sp
    }
  }
}

trait NormalizableInstances extends NormalizableInstances0 {
  implicit def coproduct[F[_], G[_]](
    implicit F: Normalizable[F], G: Normalizable[G]):
      Normalizable[Coproduct[F, G, ?]] =
    new Normalizable[Coproduct[F, G, ?]] {
      def normalize = new (Coproduct[F, G, ?] ~> Coproduct[F, G, ?]) {
        def apply[A](sp: Coproduct[F, G, A]) =
          Coproduct(sp.run.bimap(F.normalize(_), G.normalize(_)))
      }
    }
}

object Normalizable extends NormalizableInstances
