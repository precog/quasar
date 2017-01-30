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

package quasar.effect

import quasar.Predef._
import quasar.contrib.scalaz._

import scalaz.{Failure => _, _}
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.std.option._

/** Provides the ability to indicate a computation has failed.
  *
  * @tparam E the reason/error describing why the computation failed
  */
sealed trait Failure[E, A]

object Failure {
  final case class Fail[E, A](e: E) extends Failure[E, A]

  final class Ops[E, S[_]](implicit S: Failure[E, ?] :<: S)
    extends LiftedOps[Failure[E, ?], S] {

    def attempt[A](fa: F[A]): F[E \/ A] =
      fa.foldMap(attempt0).run

    def fail[A](e: E): F[A] =
      lift(Fail(e))

    def onFinish[A](fa: F[A], f: Option[E] => F[Unit]): F[A] =
      attempt(fa).flatMap(_.fold(
        e => f(Some(e)) *> fail(e),
        a => f(None)    as a))

    def onFail[A](fa: F[A], f: E => F[Unit]): F[A] =
      onFinish(fa, _.cata(f,().pure[F]))

    def recover[A](fa: F[A], f: E => F[A]): F[A] =
      attempt(fa).flatMap(_.fold(f, _.point[F]))

    def unattempt_[A](fa: E \/ A): F[A] =
      unattempt(fa.point[F])

    def unattempt[A](fa: F[E \/ A]): F[A] =
      fa.flatMap(_.fold(fail, _.point[F]))

    val unattemptT: EitherT[F, E, ?] ~> F = new (EitherT[F, E, ?] ~> F) {
      def apply[A](v: EitherT[F, E, A]): F[A] = unattempt(v.run)
    }

    implicit val monadError: MonadError[F, E] =
      new MonadError[F, E] {
        def raiseError[A](e: E) = fail(e)
        def handleError[A](fa: F[A])(f: E => F[A]) = recover(fa, f)
        def point[A](a: => A) = Free.pure(a)
        def bind[A, B](fa: F[A])(f: A => F[B]) = fa flatMap f
      }

    ////

    private type Err[A]      = Failure[E, A]
    private type G[A]        = EitherT[F, E, A]
    private type GT[X[_], A] = EitherT[X, E, A]
    private type GE[A, B]    = EitherT[F, A, B]

    private val attemptE: Err ~> G = new (Err ~> G) {
      val err = MonadError[G, E]
      def apply[A](ea: Err[A]) = ea match {
        case Fail(e) => err.raiseError[A](e)
      }
    }

    private val attempt0: S ~> G = new (S ~> G) {
      def apply[A](sa: S[A]) =
        S.prj(sa) match {
          case Some(err) => attemptE(err)
          case None      => Free.liftF(sa).liftM[GT]
        }
    }
  }

  object Ops {
    implicit def apply[E, S[_]](implicit S: Failure[E, ?] :<: S): Ops[E, S] =
      new Ops[E, S]
  }

  def mapError[D, E](f: D => E): Failure[D, ?] ~> Failure[E, ?] =
    new (Failure[D, ?] ~> Failure[E, ?]) {
      def apply[A](fa: Failure[D, A]) = fa match {
        case Fail(d) => Fail(f(d))
      }
    }

  def toError[F[_], E](implicit F: MonadError[F, E]): Failure[E, ?] ~> F =
    new (Failure[E, ?] ~> F) {
      def apply[A](fa: Failure[E, A]) = fa match {
        case Fail(e) => F.raiseError(e)
      }
    }

  def toCatchable[F[_], E <: Throwable](implicit C: Catchable[F]): Failure[E, ?] ~> F =
    new (Failure[E, ?] ~> F) {
      def apply[A](fa: Failure[E, A]) = fa match {
        case Fail(e) => C.fail(e)
      }
    }

  def toRuntimeError[F[_]: Catchable, E: Show]: Failure[E, ?] ~> F =
    toCatchable[F, RuntimeException]
      .compose[Failure[E, ?]](mapError(e => new RuntimeException(e.shows)))

  def monadError_[E, S[_]](implicit O: Ops[E, S]): MonadError_[Free[S, ?], E] =
    new MonadError_[Free[S, ?], E] {
      def raiseError[A](e: E) = O.fail(e)
      def handleError[A](fa: Free[S, A])(f: E => Free[S, A]) = O.recover(fa, f)
    }
}
