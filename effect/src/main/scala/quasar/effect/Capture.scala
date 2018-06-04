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

package quasar.effect

import slamdata.Predef._
import quasar.fp.{:<<:, ACopK}

import scalaz._, concurrent.Task
import scalaz.syntax.monad._

/** Monad with effect-capturing unit.
  *
  * Cribbed from [doobie](http://github.com/tpolecat/doobie)
  */
trait Capture[F[_]] {
  /** Captures the effect of producing `A`, including any exceptions that may
    * be thrown.
    */
  def capture[A](a: => A): F[A]

  /** Alias for `capture`. */
  def apply[A](a: => A): F[A] =
    capture(a)

  /** Construct a failed computation described by the given `Throwable`. */
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def fail[A](t: Throwable): F[A] =
    capture(throw t)

  def unattempt[A](fa: F[Throwable \/ A])(implicit F: Monad[F]): F[A] =
    fa >>= (_.fold(fail, F.point(_)))
}

object Capture extends CaptureInstances {
  def apply[F[_]](implicit F: Capture[F]): Capture[F] = F

  def forTrans[F[_]: Monad: Capture, T[_[_], _]: MonadTrans]: Capture[T[F, ?]] =
    new TransCapture[F, T]
}

sealed abstract class CaptureInstances extends CaptureInstances0 {
  implicit val taskCapture: Capture[Task] =
    new TaskCapture

  implicit def freeCaptureCopK[F[_]: Capture, S[a] <: ACopK[a]](implicit I: F :<<: S): Capture[Free[S, ?]] =
    new FreeCapture[F, S](I.inj)
}

sealed abstract class CaptureInstances0 extends CaptureInstances1 {

  implicit def freeCapture[F[_]: Capture, S[_]](implicit I: F :<: S): Capture[Free[S, ?]] =
    new FreeCapture[F, S](I)

}

sealed abstract class CaptureInstances1 {
  implicit def eitherTCapture[F[_]: Monad: Capture, E]: Capture[EitherT[F, E, ?]] =
    new TransCapture[F, EitherT[?[_], E, ?]]

  implicit def readerTCapture[F[_]: Monad: Capture, R]: Capture[ReaderT[F, R, ?]] =
    new TransCapture[F, ReaderT[?[_], R, ?]]

  implicit def stateTCapture[F[_]: Monad: Capture, S]: Capture[StateT[F, S, ?]] =
    new TransCapture[F, StateT[?[_], S, ?]]

  implicit def writerTCapture[F[_]: Monad: Capture, W: Monoid]: Capture[WriterT[F, W, ?]] =
    new TransCapture[F, WriterT[?[_], W, ?]]
}

private[effect] class TaskCapture extends Capture[Task] {
  def capture[A](a: => A) = Task.delay(a)
}

private[effect] class TransCapture[F[_]: Monad: Capture, T[_[_], _]: MonadTrans]
  extends Capture[T[F, ?]] {
  def capture[A](a: => A) = Capture[F].capture(a).liftM[T]
}

private[effect] class FreeCapture[F[_], S[_]](I: F ~> S)(implicit F: Capture[F])
  extends Capture[Free[S, ?]] {
  def capture[A](a: => A) = Free.liftF(I(F.capture(a)))
}
