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

package quasar.effect

import slamdata.Predef._
import quasar.contrib.scalaz._

import scalaz.{Failure => _, _}
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.std.option._

/** Provides the ability to indicate a computation has failed.
  *
  * @tparam E the reason/error describing why the computation failed
  */
sealed abstract class Failure[E, A]

object Failure {
  final case class Fail[E, A](e: E) extends Failure[E, A]

  final class Ops[E, S[_]](implicit S: Failure[E, ?] :<: S)
    extends LiftedOps[Failure[E, ?], S] {

    def attempt[A](fa: FreeS[A]): FreeS[E \/ A] =
      fa.foldMap(attempt0).run

    def fail[A](e: E): FreeS[A] =
      lift(Fail(e))

    def onFinish[A](fa: FreeS[A], f: Option[E] => FreeS[Unit]): FreeS[A] =
      attempt(fa).flatMap(_.fold(
        e => f(Some(e)) *> fail(e),
        a => f(None)    as a))

    def onFail[A](fa: FreeS[A], f: E => FreeS[Unit]): FreeS[A] =
      onFinish(fa, _.cata(f,().pure[FreeS]))

    def recover[A](fa: FreeS[A], f: E => FreeS[A]): FreeS[A] =
      attempt(fa).flatMap(_.fold(f, _.point[FreeS]))

    def unattempt_[A](fa: E \/ A): FreeS[A] =
      unattempt(fa.point[FreeS])

    def unattempt[A](fa: FreeS[E \/ A]): FreeS[A] =
      fa.flatMap(_.fold(fail, _.point[FreeS]))

    val unattemptT: EitherT[FreeS, E, ?] ~> FreeS = new (EitherT[FreeS, E, ?] ~> FreeS) {
      def apply[A](v: EitherT[FreeS, E, A]): FreeS[A] = unattempt(v.run)
    }

    implicit val monadError: MonadError[FreeS, E] =
      new MonadError[FreeS, E] {
        def raiseError[A](e: E) = fail(e)
        def handleError[A](fa: FreeS[A])(f: E => FreeS[A]) = recover(fa, f)
        def point[A](a: => A) = Free.pure(a)
        def bind[A, B](fa: FreeS[A])(f: A => FreeS[B]) = fa flatMap f
      }

    ////

    private type Err[A]      = Failure[E, A]
    private type G[A]        = EitherT[FreeS, E, A]
    private type GT[X[_], A] = EitherT[X, E, A]
    private type GE[A, B]    = EitherT[FreeS, A, B]

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
    λ[Failure[D, ?] ~> Failure[E, ?]]{ case Fail(d) => Fail(f(d))}

  def toError[F[_], E](implicit F: MonadError[F, E]): Failure[E, ?] ~> F =
    λ[Failure[E, ?] ~> F]{ case Fail(e) => F.raiseError(e)}

  def toCatchable[F[_], E <: Throwable](implicit C: Catchable[F]): Failure[E, ?] ~> F =
    λ[Failure[E, ?] ~> F]{ case Fail(e) => C.fail(e)}

  def toRuntimeError[F[_]: Catchable, E: Show]: Failure[E, ?] ~> F =
    toCatchable[F, RuntimeException]
      .compose[Failure[E, ?]](mapError(e => new RuntimeException(e.shows)))

  def monadError_[E, S[_]](implicit O: Ops[E, S]): MonadError_[Free[S, ?], E] =
    new MonadError_[Free[S, ?], E] {
      def raiseError[A](e: E) = O.fail(e)
      def handleError[A](fa: Free[S, A])(f: E => Free[S, A]) = O.recover(fa, f)
    }
}
