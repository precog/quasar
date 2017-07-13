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

package quasar

import slamdata.Predef._

import java.util.concurrent.atomic.AtomicLong
import scalaz._, Scalaz._
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream._

sealed abstract class TaskResource[A] {
  /** Get an instance of the resource, first acquiring it if necessary. */
  def get: Task[A]

  /** Release the cached instance, if it was ever acquired. */
  def release: Task[Unit]
}

object TaskResource {
  /** "Memo-ized" resource, given an effect to create it and an effect to
    * dispose of it. Effects are processed asynchronously using the provided
    * Strategy.
    * @param acquire Executed exactly once for any number of `get` calls as
    *   long as `release` is not called.
    * @param release0 Executed exactly once for any number of `release`
    *   calls as long as `get` is not called.
    */
  def apply[A](acquire: Task[A], execStrat: Strategy)(release0: A => Task[Unit]): Task[TaskResource[A]] = Task delay {
    val counter = new AtomicLong(0)
    def nextId: Task[Long] = Task.delay(counter.getAndIncrement)

    sealed abstract class RsrcState
    case object Start extends RsrcState
    case class Acquiring(id: Long) extends RsrcState
    case class Acquired(a: A) extends RsrcState
    case class Failed(t: Throwable) extends RsrcState

    val signal = async.signalOf[RsrcState](Start)

    def orFail[B]: Option[B] => Task[B] = _.cata(Task.now, Task.fail(Cause.Terminated(Cause.End)))

    /** The current value of the signal, or fail if it hasn't been initialized
      * (and ours is initialized when it's created.) This is the behavior you
      * presumably want for `Signal.get`, but that method actually uses the
      * `discrete` stream.
      */
    val getCurrent: Task[RsrcState] =
      signal.continuous.take(1).runLast.flatMap(orFail)

    /** Asynchronously wait for the signal to contain the resource, which
      * will have been acquired by another thread, or fail if that thread failed.
      */
    val awaitAcquired: Task[A] =
      // NB: something effectful is hiding in the `discrete` process
      Task.delay(signal.discrete.collect {
        case Acquired(a) => Task.now(a)
        case Failed(t)   => Task.fail(t)
      }.take(1).runLast).join.flatMap(orFail).join

    new TaskResource[A] {
      def get = {
        def attemptAcquire(id: Long): Task[A] =
          signal compareAndSet {
            case Some(Start) => Acquiring(id).some
            case otherwise   => otherwise
          } flatMap {
            case Some(Acquiring(`id`)) =>
              acquire.attempt.flatMap(_.fold(
                e => signal.set(Failed(e)) *> Task.fail(e),
                a => signal.set(Acquired(a)).as(a)))
            case _                     => awaitAcquired
          }

        getCurrent flatMap {
          case Acquired(a) => Task.now(a)
          case _           => nextId >>= attemptAcquire
        }
      }

      def release =
        signal.getAndSet(Start).attempt flatMap {
          case  \/-(Some(Acquired(a)))           => signal.close *> release0(a)
          case  \/-(_)                           => Task.now(())
          case -\/ (Cause.Terminated(Cause.End)) => Task.now(())
          case -\/ (t)                           => Task.fail(t)
        }
    }
  }
}
