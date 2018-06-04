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

package quasar.contrib.scalaz

import slamdata.Predef._

import java.lang.{Throwable, RuntimeException}

import quasar.fp.{:<<:, ACopK}
import quasar.fp.free.{injectNT, projectNT}

import scalaz._, Scalaz._, Leibniz.===
import scalaz.concurrent.Task

trait CatchableInstances extends CatchableInstances0 {
  implicit def copKinjectableTaskCatchable[S[a] <: ACopK[a]](implicit I: Task :<<: S): Catchable[Free[S, ?]] =
    catchable.freeCatchable[Task, S](I.inj, I.prj)
}

trait CatchableInstances0 {
  implicit def injectableTaskCatchable[S[_]](implicit I: Task :<: S): Catchable[Free[S, ?]] =
    catchable.freeCatchable[Task, S](injectNT[Task, S], projectNT[Task, S])
}

final class CatchableOps[F[_], A] private[scalaz] (self: F[A])(implicit F0: Catchable[F]) {
  /** Reify thrown non-fatal exceptions as a value. */
  def attemptNonFatal(implicit F: Monad[F]): F[Throwable \/ A] =
    new CatchableOps(self map (_.right[Throwable])).handleNonFatal {
      case th => th.left[A]
    }

  /** Ensures `f` is sequenced after `fa`, whether the latter succeeded or not.
    *
    * Useful for releasing resources that may have been acquired in order to
    * produce `fa`.
    */
  def ensuring(f: Option[Throwable] => F[Unit])(implicit F: Bind[F]): F[A] =
    F0.attempt(self) flatMap {
      case -\/(t) => f(some(t)) *> F0.fail(t)
      case \/-(a) => f(none)    as a
    }

  /** Handle caught exceptions using the given partial function, reraising any
    * unhandled exceptions.
    */
  def handle[B >: A](pf: PartialFunction[Throwable, B])(implicit F: Monad[F]): F[B] =
    handleWith[B](pf andThen (F.point(_)))

  /** Handle caught non-fatal exceptions using the given partial function,
    * reraising any unhandled exceptions.
    */
  def handleNonFatal[B >: A](pf: PartialFunction[Throwable, B])(implicit F: Monad[F]): F[B] =
    handleNonFatalWith[B](pf andThen (F.point(_)))

  /** Handle caught non-fatal exceptions using the given effectful partial
    * function, reraising any unhandled exceptions.
    */
  def handleNonFatalWith[B >: A](pf: PartialFunction[Throwable, F[B]])(implicit F: Monad[F]): F[B] =
    handleWith[B](nonFatal andThen pf)

  /** Handle caught exceptions using the given effectful partial function,
    * reraising any unhandled exceptions.
    */
  def handleWith[B >: A](pf: PartialFunction[Throwable, F[B]])(implicit F: Monad[F]): F[B] =
    F0.attempt(self) flatMap {
      case -\/(t) => pf.lift(t) getOrElse F0.fail(t)
      case \/-(a) => F.point(a)
    }

  ////

  private val nonFatal: PartialFunction[Throwable, Throwable] = {
    case scala.util.control.NonFatal(t) => t
  }
}

final class CatchableOfDisjunctionOps[F[_], A, B] private[scalaz] (self: F[A \/ B])(implicit F: Catchable[F]) {
  def unattempt(implicit M: Monad[F], T: A === Throwable): F[B] =
    self flatMap (_.fold(a => F.fail[B](T(a)), M.point(_)))

  def unattemptRuntime(implicit M: Monad[F], S: Show[A]): F[B] =
    new CatchableOfDisjunctionOps(
      self map (_.leftMap[Throwable](a => new RuntimeException(a.shows)))
    ).unattempt
}

trait ToCatchableOps {
  implicit def toCatchableOps[F[_]: Catchable, A](self: F[A]): CatchableOps[F, A] =
    new CatchableOps(self)

  implicit def toCatchableOfDisjunctionOps[F[_]: Catchable, A, B](self: F[A \/ B]): CatchableOfDisjunctionOps[F, A, B] =
    new CatchableOfDisjunctionOps(self)
}

object catchable extends CatchableInstances with ToCatchableOps {
  def freeCatchable[F[_], S[_]](
    inject: F ~> S,
    project: S ~> λ[a => Option[F[a]]]
  )(implicit F: Catchable[F]): Catchable[Free[S, ?]] =
    new Catchable[Free[S, ?]] {
      type ExceptT[X[_], A] = EitherT[X, Throwable, A]

      private val attemptT: S ~> ExceptT[Free[S, ?], ?] =
        λ[S ~> ExceptT[Free[S, ?], ?]](sa =>
          EitherT(project(sa).fold(
            Free.liftF(sa) map (_.right[Throwable]))
            { case fa => Free.liftF(inject(F.attempt(fa))) }))

      def attempt[A](fa: Free[S, A]) =
        fa.foldMap(attemptT).run

      def fail[A](t: Throwable) =
        Free.liftF(inject(F.fail[A](t)))
    }
}
