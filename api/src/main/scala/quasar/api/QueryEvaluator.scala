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

package quasar.api

import quasar.higher.HFunctor

import scalaz.{~>, Applicative, Contravariant, Functor, Profunctor, Strong, ProChoice}
import scalaz.syntax.applicative._
import scalaz.syntax.either._

import QueryEvaluator.mapEval

trait QueryEvaluator[F[_], Q, R] {
  /** Returns the results of evaluating the given query. */
  def evaluate(query: Q): F[R]
}

object QueryEvaluator extends QueryEvaluatorInstances {
  def apply[F[_], Q, R](f: Q => F[R]): QueryEvaluator[F, Q, R] =
    new QueryEvaluator[F, Q, R] { def evaluate(q: Q) = f(q) }

  /** A `QueryEvaluator` derived from `underlying` by transforming its
    * `evaluate` function.
    */
  object mapEval {
    def apply[F[_], Q, R](underlying: QueryEvaluator[F, Q, R])
        : PartiallyApplied[F, Q, R] =
      new PartiallyApplied(underlying)

    final class PartiallyApplied[F[_], Q, R](u: QueryEvaluator[F, Q, R]) {
      def apply[S, T](f: (Q => F[R]) => (S => F[T])): QueryEvaluator[F, S, T] =
        new QueryEvaluator[F, S, T] {
          def evaluate(s: S): F[T] = f(u.evaluate)(s)
        }
    }
  }
}

sealed abstract class QueryEvaluatorInstances extends QueryEvaluatorInstances0 {
  implicit def contravariant[F[_], R]: Contravariant[QueryEvaluator[F, ?, R]] =
    new Contravariant[QueryEvaluator[F, ?, R]] {
      def contramap[A, B](fa: QueryEvaluator[F, A, R])(f: B => A) =
        mapEval(fa)(_ compose f)
    }

  implicit def functor[F[_]: Functor, Q]: Functor[QueryEvaluator[F, Q, ?]] =
    new Functor[QueryEvaluator[F, Q, ?]] {
      def map[A, B](fa: QueryEvaluator[F, Q, A])(f: A => B) =
        mapEval(fa)(_ andThen (_.map(f)))
    }

  implicit def hfunctor[Q, R]: HFunctor[QueryEvaluator[?[_], Q, R]] =
    new HFunctor[QueryEvaluator[?[_], Q, R]] {
      def hmap[A[_], B[_]](fa: QueryEvaluator[A, Q, R])(f: A ~> B) =
        new QueryEvaluator[B, Q, R] {
          def evaluate(query: Q) = f(fa.evaluate(query))
        }
    }

  implicit def proChoice[F[_]: Applicative]: ProChoice[QueryEvaluator[F, ?, ?]] =
    new QueryEvaluatorProfunctor[F] with ProChoice[QueryEvaluator[F, ?, ?]] {
      def left[A, B, C](fa: QueryEvaluator[F, A, B]) =
        mapEval(fa) { eval => { q =>
          q.fold(
            a => eval(a).map(_.left[C]),
            c => c.right[B].point[F])
        }}

      def right[A, B, C](fa: QueryEvaluator[F, A, B]) =
        mapEval(fa) { eval => {q =>
          q.fold(
            c => c.left[B].point[F],
            a => eval(a).map(_.right[C]))
        }}
    }
}

sealed abstract class QueryEvaluatorInstances0 {
  implicit def strong[F[_]: Functor]: Strong[QueryEvaluator[F, ?, ?]] =
    new QueryEvaluatorProfunctor[F] with Strong[QueryEvaluator[F, ?, ?]] {
      def first[A, B, C](fa: QueryEvaluator[F, A, B]) =
        mapEval(fa) { eval => {
          case (a, c) => eval(a) strengthR c
        }}

      def second[A, B, C](fa: QueryEvaluator[F, A, B]) =
        mapEval(fa) { eval => {
          case (c, a) => eval(a) strengthL c
        }}
    }
}

private[api] abstract class QueryEvaluatorProfunctor[F[_]: Functor]
    extends Profunctor[QueryEvaluator[F, ?, ?]] {

  def mapfst[A, B, C](fa: QueryEvaluator[F, A, B])(f: C => A) =
    Contravariant[QueryEvaluator[F, ?, B]].contramap(fa)(f)

  def mapsnd[A, B, C](fa: QueryEvaluator[F, A, B])(f: B => C) =
    Functor[QueryEvaluator[F, A, ?]].map(fa)(f)
}
