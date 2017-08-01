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

package quasar.contrib

import slamdata.Predef._

import _root_.monocle.{Getter, Iso}
import _root_.matryoshka._
import _root_.matryoshka.patterns.EnvT
import _root_.scalaz._, Scalaz._

package object matryoshka {
  /** Chains multiple transformations together, each of which can fail to change
    * anything.
    */
  def applyTransforms[A](first: A => Option[A], rest: (A => Option[A])*)
      : A => Option[A] =
    rest.foldLeft(
      first)(
      (prev, next) => x => prev(x).fold(next(x))(orOriginal(next)(_).some))

  def envT[E, W[_], A](e: E, wa: W[A]): EnvT[E, W, A] =
    EnvT((e, wa))

  def envTIso[E, W[_], A]: Iso[EnvT[E, W, A], (E, W[A])] =
    Iso((_: EnvT[E, W, A]).runEnvT)(EnvT(_))

  def project[T, F[_]: Functor](implicit T: Recursive.Aux[T, F]): Getter[T, F[T]] =
    Getter(T.project(_))

  /** Make a partial endomorphism total by returning the argument when undefined. */
  def totally[A](pf: PartialFunction[A, A]): A => A =
    orOriginal(pf.lift)

  /** Derive a recursive instance over the functor transformed by EnvT by forgetting the annotation. */
  def forgetRecursive[T, E, F[_]](implicit T: Recursive.Aux[T, EnvT[E, F, ?]]): Recursive.Aux[T, F] =
    new Recursive[T] {
      type Base[B] = F[B]

      def project(t: T)(implicit BF: Functor[Base]) =
        T.project(t).lower
    }

  /** Derive a corecursive instance over the functor transformed by EnvT using the zero of the annotation monoid. */
  def rememberCorecursive[T, E: Monoid, F[_]](implicit T: Corecursive.Aux[T, EnvT[E, F, ?]]): Corecursive.Aux[T, F] =
    new Corecursive[T] {
      type Base[B] = F[B]

      def embed(ft: Base[T])(implicit BF: Functor[Base]) =
        T.embed(envT(∅[E], ft))
    }

  implicit def delayOrder[F[_], A](implicit F: Delay[Order, F], A: Order[A]): Order[F[A]] =
    F(A)

  implicit def coproductOrder[F[_], G[_]](implicit F: Delay[Order, F], G: Delay[Order, G]): Delay[Order, Coproduct[F, G, ?]] =
    new Delay[Order, Coproduct[F, G, ?]] {
      def apply[A](ord: Order[A]): Order[Coproduct[F, G, A]] = {
        implicit val ordA: Order[A] = ord
        Order.orderBy((_: Coproduct[F, G, A]).run)
      }
    }
}
