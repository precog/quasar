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

package quasar.evaluate

import quasar.api.ResourceError.ReadError

import scalaz.{\/, Contravariant, Functor, Profunctor}
import scalaz.syntax.functor._

/** Represents the ability to evaluate QScript over potentially many sources. */
trait QueryFederation[T[_[_]], F[_], S, R] {
  def evaluateFederated(q: FederatedQuery[T, S]): F[ReadError \/ R]
}

object QueryFederation extends QueryFederationInstances {
  def apply[T[_[_]], F[_], S, R](
      f: FederatedQuery[T, S] => F[ReadError \/ R])
      : QueryFederation[T, F, S, R] =
    new QueryFederation[T, F, S, R] {
      def evaluateFederated(q: FederatedQuery[T, S]) = f(q)
    }
}

sealed abstract class QueryFederationInstances {
  implicit def contravariant[T[_[_]], F[_], R]: Contravariant[QueryFederation[T, F, ?, R]] =
    new Contravariant[QueryFederation[T, F, ?, R]] {
      def contramap[A, B](fa: QueryFederation[T, F, A, R])(f: B => A) =
        QueryFederation(fq => fa.evaluateFederated(fq map f))
    }

  implicit def functor[T[_[_]], F[_]: Functor, S]: Functor[QueryFederation[T, F, S, ?]] =
    new Functor[QueryFederation[T, F, S, ?]] {
      def map[A, B](fa: QueryFederation[T, F, S, A])(f: A => B) =
        QueryFederation(fq => fa.evaluateFederated(fq).map(_.map(f)))
    }

  implicit def profunctor[T[_[_]], F[_]: Functor]: Profunctor[QueryFederation[T, F, ?, ?]] =
    new Profunctor[QueryFederation[T, F, ?, ?]] {
      def mapfst[A, B, C](fa: QueryFederation[T, F, A, B])(f: C => A) =
        contravariant[T, F, B].contramap(fa)(f)

      def mapsnd[A, B, C](fa: QueryFederation[T, F, A, B])(f: B => C) =
        functor[T, F, A].map(fa)(f)
    }
}
