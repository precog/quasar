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

package quasar.contrib.cats.effect

import slamdata.Predef._

import cats.data.StateT
import cats.effect._
import cats.kernel.Monoid

object stateT {
  // we are depending on this implicit which was removed here https://github.com/typelevel/cats-effect/pull/291
  implicit def catsStateTEffect[F[_]: Effect, S: Monoid]: Effect[StateT[F, S, ?]] =
    new StateTEffect[F, S] { def F = Effect[F]; def S = Monoid[S] }

  private[effect] trait StateTEffect[F[_], S] extends Effect[StateT[F, S, ?]] {
    protected def F: Effect[F]
    protected def S: Monoid[S]
    private val asyncStateT = Async.catsStateTAsync[F, S](F)

    override def pure[A](x: A): StateT[F, S, A] = asyncStateT.pure(x)

    override def handleErrorWith[A](
      fa: StateT[F, S, A])(
      f: Throwable => StateT[F, S, A])
        : StateT[F, S, A] =
      asyncStateT.handleErrorWith(fa)(f)

    override def raiseError[A](e: Throwable): StateT[F, S, A] =
      asyncStateT.raiseError(e)

    override def async[A](k: (Either[Throwable,A] => Unit) => Unit): StateT[F, S, A] =
      asyncStateT.async(k)

    override def asyncF[A](k: (Either[Throwable,A] => Unit) => StateT[F, S, Unit]): StateT[F, S, A] =
      asyncStateT.asyncF(k)

    override def bracketCase[A, B](
      acquire: StateT[F, S, A])(
      use: A => StateT[F, S, B])(
      release: (A, ExitCase[Throwable]) => StateT[F, S, Unit])
        : StateT[F, S, B] =
      asyncStateT.bracketCase(acquire)(use)(release)

    override def flatMap[A, B](fa: StateT[F, S, A])(f: A => StateT[F, S, B]): StateT[F, S, B] =
      asyncStateT.flatMap(fa)(f)

    override def tailRecM[A, B](a: A)(f: A => StateT[F, S, Either[A,B]]): StateT[F, S, B] =
      asyncStateT.tailRecM(a)(f)

    override def suspend[A](thunk: => StateT[F, S, A]): StateT[F, S, A] =
      asyncStateT.suspend(thunk)

    override def runAsync[A](fa: StateT[F, S, A])(cb: Either[Throwable, A] => IO[Unit]): SyncIO[Unit] =
      F.runAsync(fa.runA(S.empty)(F))(cb)

    override def toIO[A](fa: StateT[F, S, A]): IO[A] =
      F.toIO(fa.runA(S.empty)(F))
  }
}
