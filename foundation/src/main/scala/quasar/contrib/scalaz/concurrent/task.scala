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

package quasar.contrib.scalaz.concurrent

import slamdata.Predef.{PartialFunction, Throwable, Unit}

import scala.util.{Either, Left, Right}
import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.{Effect, ExitCase, IO}
import cats.StackSafeMonad
import scalaz.{\/, -\/, \/-}
import scalaz.concurrent.{Future, Task}

object task {
  /** Cribbed from https://github.com/ChristopherDavenport/scalaz-task-effect
    *
    * TODO: Switch back to scalaz-task-effect once testing issues are sorted and
    *       PR has been accepted.
    */
  implicit val taskEffect: Effect[Task] = new Effect[Task] with StackSafeMonad[Task] {
    // Members declared in cats.Applicative
    def pure[A](x: A): Task[A] = Task.now(x)

    // Members declared in cats.ApplicativeError
    def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] =
      fa.handleWith(functionToPartial(f))

    def raiseError[A](e: Throwable): Task[A] = Task.fail(e)

    // Members declared in cats.effect.Bracket
    def bracketCase[A, B](acq: Task[A])(use: A => Task[B])(rel: (A, ExitCase[Throwable]) => Task[Unit]): Task[B] =
      acq flatMap { a =>
        use(a).onFinish(err =>
          rel(a, err.fold(ExitCase.complete[Throwable])(ExitCase.error)))
      }

    // Members declared in cats.effect.Async
    // In order to comply with `repeatedCallbackIgnored` law
    // on async, a custom AtomicBoolean is required to ignore
    // second callbacks.
    def async[A](k: (Either[Throwable, A] => Unit) => Unit): Task[A] =
      Task.async(singleUseCallback(k))

    def asyncF[A](k: (Either[Throwable, A] => Unit) => Task[Unit]): Task[A] =
      Task.async(cb => singleUseCallback(k)(cb).unsafePerformSync)

    // Members declared in cats.effect.Effect

    /** runAsync takes the final callback to something that
     * summarizes the effects in an IO[Unit] as such this
     * takes the Task and executes the internal IO callback
     * into the task asynchronous execution all delayed
     * within the outer IO, discarding any error that might
     * occur
      **/
    def runAsync[A](fa: Task[A])(cb: Either[Throwable, A] => IO[Unit]): IO[Unit] =
      IO {
        fa.unsafePerformAsync { disjunction =>
          cb(disjunction.toEither).unsafeRunAsync(_ => ())
        }
      }

    def runSyncStep[A](fa: Task[A]): IO[Either[Task[A], A]] =
      IO(fa.get match {
        case Future.Now(-\/(_)) => Left(fa)
        case other => other.step match {
          case Future.Now(\/-(a)) => Right(a)
          case other              => Left(new Task(other))
        }
      })

    override def toIO[A](fa: Task[A]): IO[A] =
      IO.async(cb => fa.unsafePerformAsync(d => cb(d.toEither)))

    // Members declared in cats.FlatMap
    def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)

    // Members declared in cats.effect.Sync
    def suspend[A](thunk: => Task[A]): Task[A] = Task.suspend(thunk)
  }

  private def functionToPartial[A, B](f: A => B): PartialFunction[A, B] = _ match {
    case a => f(a)
  }

  private def singleUseCallback[A, B](
      f: (Either[Throwable, A] => Unit) => B)
      : (Throwable \/ A => Unit) => B = { registered =>
    val a = new AtomicBoolean(true)
    f(e => if (a.getAndSet(false)) { registered(\/.fromEither(e)) } else ())
  }
}
