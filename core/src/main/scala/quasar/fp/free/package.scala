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

package quasar.fp

import quasar.Predef._

import scalaz._
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.concurrent.Task

package object free {
  type Coproduct3[F[_], G[_], H[_], A] = Coproduct[F, Coproduct[G, H, ?], A]
  type Coproduct4[F[_], G[_], H[_], I[_], A] = Coproduct[F, Coproduct3[G, H, I, ?], A]
  type Coproduct5[F[_], G[_], H[_], I[_], J[_], A] = Coproduct[F, Coproduct4[G, H, I, J, ?], A]
  type Coproduct6[F[_], G[_], H[_], I[_], J[_], K[_], A] = Coproduct[F, Coproduct5[G, H, I, J, K, ?], A]

  /** Given `F[_]` and `G[_]` such that `F :<: G`, lifts a natural transformation
    * `F ~> F` to `G ~> G`.
    */
  def injectedNT[F[_], G[_]](f: F ~> F)(implicit G: F :<: G): G ~> G =
    new (G ~> G) {
      def apply[A](ga: G[A]) = G.prj(ga).fold(ga)(fa => G.inj(f(fa)))
    }

  def restrict[M[_], S[_], T[_]](f: T ~> M)(implicit S: Coyoneda[S, ?] :<: T) =
    new (S ~> M) {
      def apply[A](fa: S[A]): M[A] = f(S.inj(Coyoneda.lift(fa)))
    }

  def flatMapSNT[S[_]: Functor, T[_]](f: S ~> Free[T, ?]): Free[S, ?] ~> Free[T, ?] =
    new (Free[S, ?] ~> Free[T, ?]) {
      def apply[A](fa: Free[S, A]) = fa.flatMapSuspension(f)
    }

  def foldMapNT[F[_]: Functor, G[_]: Monad](f: F ~> G) = new (Free[F, ?] ~> G) {
    def apply[A](fa: Free[F, A]): G[A] =
      fa.foldMap(f)
  }

  def mapSNT[S[_]: Functor, T[_]: Functor](f: S ~> T): Free[S, ?] ~> Free[T, ?] =
    new (Free[S, ?] ~> Free[T, ?]) {
      def apply[A](fa: Free[S, A]) = fa.mapSuspension(f)
    }

  /** Given `F[_]` and `S[_]` such that `F :<: S`, returns a natural
    * transformation, `S ~> G`, where `f` is used to transform an `F[_]` and `g`
    * used otherwise.
    */
  def transformIn[F[_], S[_], G[_]: Functor](f: F ~> G, g: S ~> G)(implicit S: F :<: S): S ~> G =
    new (S ~> G) {
      def apply[A](sa: S[A]) = S.prj(sa).fold(g(sa))(f)
    }

  def interpret2[F[_], G[_], M[_]](f: F ~> M, g: G ~> M): Coproduct[F, G, ?] ~> M =
    new (Coproduct[F, G, ?] ~> M) {
      def apply[A](fa: Coproduct[F, G, A]) =
        fa.run.fold(f, g)
    }

  def interpret3[F[_], G[_], H[_], M[_]](f: F ~> M, g: G ~> M, h: H ~> M): Coproduct3[F, G, H, ?] ~> M =
    new (Coproduct3[F, G, H, ?] ~> M) {
      def apply[A](fa: Coproduct3[F, G, H, A]) =
        fa.run.fold(f, interpret2(g, h)(_))
    }

  def interpret4[F[_], G[_], H[_], I[_], M[_]](f: F ~> M, g: G ~> M, h: H ~> M, i: I ~> M): Coproduct4[F, G, H, I, ?] ~> M =
    new (Coproduct4[F, G, H, I, ?] ~> M) {
      def apply[A](fa: Coproduct4[F, G, H, I, A]) =
        fa.run.fold(f, interpret3(g, h, i)(_))
    }

  def interpret5[F[_], G[_], H[_], I[_], J[_], M[_]](f: F ~> M, g: G ~> M, h: H ~> M, i: I ~> M, j: J ~> M): Coproduct5[F, G, H, I, J, ?] ~> M =
    new (Coproduct5[F, G, H, I, J, ?] ~> M) {
      def apply[A](fa: Coproduct5[F, G, H, I, J, A]) =
        fa.run.fold(f, interpret4(g, h, i, j)(_))
    }

  def interpret6[F[_], G[_], H[_], I[_], J[_], K[_], M[_]](f: F ~> M, g: G ~> M, h: H ~> M, i: I ~> M, j: J ~> M, k: K ~> M): Coproduct6[F, G, H, I, J, K, ?] ~> M =
    new (Coproduct6[F, G, H, I, J, K, ?] ~> M) {
      def apply[A](fa: Coproduct6[F, G, H, I, J, K, A]) =
        fa.run.fold(f, interpret5(g, h, i, j, k)(_))
    }

  /** A `Catchable` instance for `Free[S, ?]` when `Task` can be injected into `S`. */
  implicit def freeCatchable[S[_]: Functor](implicit S: Task :<: S): Catchable[Free[S, ?]] =
    new Catchable[Free[S, ?]] {
      type G[A] = Free[S, A]
      private val injFT: Task ~> G = injectFT[Task, S]
      private val lftFT: S ~> G = liftFT[S]

      def attempt[A](fa: Free[S, A]): Free[S, Throwable \/ A] =
        injFT(Task.delay(fa.resume match {
          case \/-(a) =>
            a.right[Throwable].point[G]

          case -\/(sa) => S.prj(sa) match {
            case Some(t) =>
              injFT(t.attempt) flatMap {
                case -\/(t)   => t.left[A].point[G]
                case \/-(fa0) => attempt(fa0)
              }

            case None =>
              lftFT(sa).flatMap(attempt)
          }
        }).attempt map {
          case \/-(a) => a
          case -\/(t) => t.left[A].point[G]
        }).join

      def fail[A](t: Throwable): Free[S, A] =
        injFT(Task.fail(t))
    }
}
