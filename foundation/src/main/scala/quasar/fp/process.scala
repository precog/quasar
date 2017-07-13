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

package quasar.fp

import slamdata.Predef._

import scala.collection.{Seq => SSeq}

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

trait ProcessOps {
  implicit class PrOps[F[_], O](self: Process[F, O]) {
    final def evalScan1(f: (O, O) => F[O])(implicit monad: Monad[F]): Process[F, O] = {
      self.zipWithPrevious.evalMap {
        case (None, next) => monad.point(next)
        case (Some(prev), next) => f(prev, next)
      }
    }

    /** Exposes the effect from the first `Await` encountered, the inner process
      * emits the same values, in the same order as this process.
      */
    final def firstStep[F2[x] >: F[x], O2 >: O](implicit F: Monad[F2], C: Catchable[F2]): F2[Process[F2, O2]] = {
      val (hd, tl) = self.unemit
      tl.unconsOption[F2, O2] map {
        case Some((x, xs)) => Process.emitAll(hd :+ x) ++ xs
        case None          => Process.emitAll(hd)
      }
    }

    /** Step through `Await`s in this `Process`, combining effects via `Bind`,
      * until the predicate returns true or the stream ends. The returned inner
      * `Process` emits the same values, in the same order as this process.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    final def stepUntil[F2[x] >: F[x], O2 >: O](p: SSeq[O2] => Boolean)(implicit F: Monad[F2], C: Catchable[F2]): F2[Process[F2, O2]] =
      firstStep[F2, O2] flatMap { next =>
        val (hd, tl) = next.unemit

        if (hd.isEmpty || p(hd))
          (Process.emitAll(hd) ++ tl).point[F2]
        else
          tl.stepUntil(p).map(Process.emitAll(hd) ++ _)
      }
  }

  implicit class ProcessOfTaskOps[O](self: Process[Task,O]) {
    // Is there a better way to implement this?
    def onHaltWithLastElement(f: (Option[O], Cause) => Process[Task,O]): Process[Task,O] = {
      Process.await(TaskRef[Option[O]](None)){ lastA =>
        self.observe(Process.constant((a:O) => lastA.write(Some(a)))).onHalt{ cause =>
          Process.await(lastA.read)( a => f(a,cause))
        }
      }
    }
    def cleanUpWithA(f: Option[O] => Task[Unit]): Process[Task,O] = {
      self.onHaltWithLastElement((a, cause) => Process.eval_(f(a)).causedBy(cause))
    }
  }

  implicit class TaskOps[A](t: Task[A]) {
    def onSuccess(f: A => Task[Unit]): Task[A] = {
      t.flatMap(a => f(a).as(a))
    }
  }
}

object ProcessOps extends ProcessOps
