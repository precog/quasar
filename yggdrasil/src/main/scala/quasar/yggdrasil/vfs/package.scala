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

package quasar.yggdrasil

import slamdata.Predef.Throwable
import quasar.contrib.iota.{:<<:, ACopK}
import quasar.fp.free

import scala.concurrent.ExecutionContext

import cats.StackSafeMonad
import cats.effect.{ContextShift, ExitCase, IO, Sync}
import iotaz.{CopK, TNilK}
import iotaz.TListK.:::
import scalaz.{EitherT, Free, ~>}
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.std.either._

package object vfs {
  type POSIX[A] = Free[POSIXOp, A]
  type POSIXWithIOCopK[A] = CopK[POSIXOp ::: IO ::: TNilK, A]
  type POSIXWithIO[A] = Free[POSIXWithIOCopK, A]

  // this is needed kind of a lot
  private[vfs] implicit def syncForS[S[a] <: ACopK[a]](implicit I: IO :<<: S): Sync[Free[S, ?]] =
    new Sync[Free[S, ?]] with StackSafeMonad[Free[S, ?]] {
      type ExceptT[X[_], A] = EitherT[X, Throwable, A]

      private val attemptT: S ~> ExceptT[Free[S, ?], ?] =
        λ[S ~> ExceptT[Free[S, ?], ?]] { sa =>
          EitherT(I.prj(sa).fold(
            Free.liftF(sa) map (_.right[Throwable]))(
            ioa => Free.liftF(I(ioa.attempt.map(_.disjunction)))))
        }


      def bracketCase[A, B](acquire: Free[S, A])(use: A => Free[S, B])(release: (A, ExitCase[Throwable]) => Free[S, Unit]): Free[S, B] =
        for {
          a <- acquire
          r <- use(a).foldMap(attemptT).run
          b <- r.fold(
            t => release(a, ExitCase.error(t)) *> raiseError[B](t),
            b => release(a, ExitCase.complete).as(b))
        } yield b

      def suspend[A](thunk: => Free[S, A]): Free[S, A] =
        delay(thunk).join

      override def delay[A](a: => A): Free[S, A] =
        Free.liftF(I(IO(a)))

      def raiseError[A](err: Throwable): Free[S, A] =
        free.lift(IO.raiseError[A](err)).intoCopK[S]

      def handleErrorWith[A](fa: Free[S, A])(f: Throwable => Free[S, A]): Free[S, A] =
        fa.foldMap(attemptT).run.flatMap(_.fold(f, Free.pure(_)))

      def pure[A](a: A): Free[S, A] =
        Free.pure[S, A](a)

      def flatMap[A, B](fa: Free[S, A])(f: A => Free[S, B]): Free[S, B] =
        fa.flatMap(f)
    }

  implicit def contextShiftForS[S[a] <: ACopK[a]](
    implicit
    cs: ContextShift[IO],
    I: IO :<<: S)
      : ContextShift[Free[S, ?]] =
    new ContextShift[Free[S, ?]] {
      def shift: Free[S, Unit] = Free.liftF(I(cs.shift))

      def evalOn[A](ctx: ExecutionContext)(fa: Free[S, A]): Free[S, A] = {
        fa.mapSuspension(
          λ[S ~> S](ga => I.prj(ga).fold(ga)(ioa => I(cs.evalOn(ctx)(ioa)))))
      }
    }
}
