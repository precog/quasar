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

package quasar

import slamdata.Predef.{Throwable, Unit}
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.κ

import scalaz.{Applicative, Functor, Monad, Monoid, Semigroup, Zip}
import scalaz.syntax.monad._
import scalaz.syntax.monoid._

/** Represents a value associated with a contextual resource that must be
  * disposed of in order to avoid resource leaks.
  *
  * One _must_ either consume the value with `apply`, in which case the
  * resource will automatically be disposed of, or explicitly dispose of
  * the resource.
  */
final class Disposable[F[_], A](
    protected val value: A,
    val dispose: F[Unit]) {

  def apply[B](f: A => F[B])(implicit F0: Monad[F], F1: MonadError_[F, Throwable]): F[B] =
    F1.ensuring(f(value))(κ(dispose))

  def ap[B](other: => Disposable[F, A => B])(
      implicit
      F0: Monad[F],
      F1: MonadError_[F, Throwable])
      : Disposable[F, B] =
    other.zip(this) map {
      case (f, a) => f(a)
    }

  def map[B](f: A => B): Disposable[F, B] =
    Disposable(f(value), dispose)

  def mappend(other: => Disposable[F, A])(
      implicit
      F0: Monad[F],
      F1: MonadError_[F, Throwable],
      A: Semigroup[A])
      : Disposable[F, A] =
    zip(other) map {
      case (a, b) => A.append(a, b)
    }

  def zip[B](b: Disposable[F, B])(
      implicit
      F0: Monad[F],
      F1: MonadError_[F, Throwable])
      : Disposable[F, (A, B)] =
    Disposable((value, b.value), F1.ensuring(dispose)(κ(b.dispose)))
}

object Disposable extends DisposableInstances {
  def apply[F[_], A](a: A, release: F[Unit]): Disposable[F, A] =
    new Disposable(a, release)
}

sealed abstract class DisposableInstances extends DisposableInstances0 {
  implicit def applicative[F[_]: Monad: MonadError_[?[_], Throwable]]: Applicative[Disposable[F, ?]] =
    new Applicative[Disposable[F, ?]] {
      def ap[A, B](fa: => Disposable[F, A])(ff: => Disposable[F, A => B]) =
        fa ap ff

      def point[A](a: => A) =
        Disposable(a, ().point[F])
    }

  implicit def monoid[F[_]: Monad: MonadError_[?[_], Throwable], A: Monoid]: Monoid[Disposable[F, A]] =
    Monoid.instance(_ mappend _, Disposable(mzero[A], ().point[F]))

  implicit def zip[F[_]: Monad: MonadError_[?[_], Throwable]]: Zip[Disposable[F, ?]] =
    new Zip[Disposable[F, ?]] {
      def zip[A, B](da: => Disposable[F, A], db: => Disposable[F, B]) =
        da.zip(db)
    }
}

sealed abstract class DisposableInstances0 {
  implicit def functor[F[_]]: Functor[Disposable[F, ?]] =
    new Functor[Disposable[F, ?]] {
      def map[A, B](da: Disposable[F, A])(f: A => B) =
        da map f
    }

  implicit def semigroup[F[_]: Monad: MonadError_[?[_], Throwable], A: Semigroup]: Semigroup[Disposable[F, A]] =
    Semigroup.instance(_ mappend _)
}
