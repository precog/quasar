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

package quasar.contrib.scalaz

import slamdata.Predef._

import scalaz._, Scalaz._
import scalaz.Liskov._

/** A version of MonadError that doesn't extend Monad to avoid ambiguous implicits
  * in the presence of multiple "mtl" constraints.
  */
trait MonadError_[F[_], E] {
  def raiseError[A](e: E): F[A]
  def handleError[A](fa: F[A])(f: E => F[A]): F[A]

  def attempt[A](fa: F[A])(implicit F: Applicative[F]): F[E \/ A] =
    handleError(fa map (_.right[E]))(_.left[A].point[F])

  /** Ensures `f` is sequenced after `fa`, whether the latter succeeded or not.
    *
    * Useful for releasing resources that may have been acquired in order to
    * produce `fa`.
    */
  def ensuring[A](fa: F[A])(f: Option[E] => F[Unit])(implicit F: Monad[F]): F[A] =
    attempt(fa) flatMap {
      case -\/(e) => f(some(e)) *> raiseError(e)
      case \/-(a) => f(none)    as a
    }

  def handle[A](fa: F[A])(pf: PartialFunction[E, A])(implicit F: Applicative[F]): F[A] =
    handleWith(fa)(pf andThen (_.point[F]))

  def handleWith[A](fa: F[A])(pf: PartialFunction[E, F[A]]): F[A] =
    handleError(fa)(e => pf.lift(e) getOrElse raiseError(e))

  def unattempt[A](fa: F[E \/ A])(implicit F: Monad[F]): F[A] =
    fa >>= (_.fold(raiseError[A] _, _.point[F]))
}

object MonadError_ extends MonadError_Instances {
  def apply[F[_], E](implicit F: MonadError_[F, E]): MonadError_[F, E] = F
}

sealed abstract class MonadError_Instances extends MonadError_Instances0 {
  implicit def kleisliMonadError_[F[_], E, R](implicit F: MonadError_[F, E]): MonadError_[Kleisli[F, R, ?], E] =
    new MonadError_[Kleisli[F, R, ?], E] {
      def raiseError[A](e: E) =
        Kleisli(_ => F.raiseError(e))

      def handleError[A](fa: Kleisli[F, R, A])(f: E => Kleisli[F, R, A]) =
        Kleisli(r => F.handleError(fa.run(r))(e => f(e).run(r)))
    }

  implicit def writerTMonadError_[F[_]: Functor, W: Monoid, E](implicit E: MonadError_[F, E]): MonadError_[WriterT[F, W, ?], E] =
    new MonadError_[WriterT[F, W, ?], E] {
      def raiseError[A](e: E) =
        WriterT(E.raiseError[A](e) strengthL mzero[W])

      def handleError[A](fa: WriterT[F, W, A])(f: E => WriterT[F, W, A]) =
        WriterT(E.handleError(fa.run)(e => f(e).run))
    }

  implicit def eitherTInnerMonadError_[F[_]: Functor, E1, E2](implicit E: MonadError_[F, E1]): MonadError_[EitherT[F, E2, ?], E1] =
    new MonadError_[EitherT[F, E2, ?], E1] {
      def raiseError[A](e: E1) =
        EitherT(E.raiseError[A](e) map (_.right[E2]))

      def handleError[A](fa: EitherT[F, E2, A])(f: E1 => EitherT[F, E2, A]) =
        EitherT(E.handleError(fa.run)(e1 => f(e1).run))
    }

  implicit def stateTMonadError_[F[_]: Monad, E, S](implicit F: MonadError_[F, E]): MonadError_[StateT[F, S, ?], E] =
    new MonadError_[StateT[F, S, ?], E] {
      def handleError[A](fa: StateT[F, S, A])(f: E => StateT[F, S, A]) =
        StateT(s => F.handleError(fa.run(s))(f(_).run(s)))

      def raiseError[A](e: E) =
        StateT(_ => F.raiseError[(S, A)](e))
    }
}

sealed abstract class MonadError_Instances0 {
  implicit def monadErrorNoMonad[F[_], E](implicit F: MonadError[F, E]): MonadError_[F, E] =
    new MonadError_[F, E] {
      def raiseError[A](e: E): F[A] = F.raiseError(e)
      def handleError[A](fa: F[A])(f: E => F[A]): F[A] = F.handleError(fa)(f)
    }
}

final class MonadError_Ops[F[_], E, A] private[scalaz] (self: F[A])(implicit F0: MonadError_[F, E]) {
  final def handleError(f: E => F[A]): F[A] =
    F0.handleError(self)(f)

  def attempt(implicit F: Applicative[F]): F[E \/ A] =
    F0.attempt(self)

  def ensuring(f: Option[E] => F[Unit])(implicit F: Monad[F]): F[A] =
    F0.ensuring(self)(f)

  def handle(pf: PartialFunction[E, A])(implicit F: Applicative[F]): F[A] =
    F0.handle(self)(pf)

  def handleWith(pf: PartialFunction[E, F[A]]): F[A] =
    F0.handleWith(self)(pf)

  def unattempt[B](implicit ev: A <~< (E \/ B), M: Monad[F]): F[B] =
    F0.unattempt(self.map(ev(_)))
}
