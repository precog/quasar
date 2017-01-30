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

import scalaz._, concurrent.Task
import scalaz.syntax.monad._
import simulacrum.typeclass

/** Monad with effect-capturing unit.
  *
  * Cribbed from [doobie](http://github.com/tpolecat/doobie)
  */
@typeclass
trait Capture[F[_]] {
  def delay[A](a: => A): F[A]
  def apply[A](a: => A): F[A] = delay(a)
}

object Capture extends CaptureInstances {
  def forTrans[F[_]: Monad: Capture, T[_[_], _]: MonadTrans]: Capture[T[F, ?]] =
    new Capture[T[F, ?]] { def delay[A](a: => A) = Capture[F].delay(a).liftM[T] }

  def forInjectable[F[_], S[_]](implicit F: Capture[F], I: F :<: S): Capture[Free[S, ?]] =
    new Capture[Free[S, ?]] {
      def delay[A](a: => A) = Free.liftF(I(F.delay(a)))
    }
}

sealed abstract class CaptureInstances {
  implicit val taskCapture: Capture[Task] =
    new Capture[Task] {
      def delay[A](a: => A) = Task.delay(a)
    }

  implicit def injectTaskCapture[S[_]](implicit I: Task :<: S): Capture[Free[S, ?]] =
    Capture.forInjectable[Task, S]

  implicit def eitherTCapture[F[_]: Monad: Capture, E]: Capture[EitherT[F, E, ?]] =
    Capture.forTrans[F, EitherT[?[_], E, ?]]

  implicit def readerTCapture[F[_]: Monad: Capture, R]: Capture[ReaderT[F, R, ?]] =
    Capture.forTrans[F, ReaderT[?[_], R, ?]]

  implicit def stateTCapture[F[_]: Monad: Capture, S]: Capture[StateT[F, S, ?]] =
    Capture.forTrans[F, StateT[?[_], S, ?]]

  implicit def writerTCapture[F[_]: Monad: Capture, W: Monoid]: Capture[WriterT[F, W, ?]] =
    Capture.forTrans[F, WriterT[?[_], W, ?]]
}
