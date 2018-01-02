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

package quasar.main

import slamdata.Predef._
import quasar.concurrent.NamedDaemonThreadFactory
import quasar.fp._, free._, ski._
import quasar.fs.cache.ViewCacheRefresh
import quasar.fs.QueryFile
import quasar.metastore.MetaStoreAccess

import java.lang.Thread
import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.concurrent.duration._

import doobie.imports.ConnectionIO
import scalaz._, Scalaz._
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream.{Process, time}

object Caching {
  type Eff[A] = Coproduct[Task, Coproduct[ConnectionIO, CoreEff, ?], A]

  val QT = QueryFile.Transforms[Free[Eff, ?]]

  final case class ViewCacheRefreshCtx(start: Task[Unit], shutdown: Task[Unit])

  def viewCacheRefreshCtx(eval: Free[Eff, ?] ~> Task): Task[ViewCacheRefreshCtx] = Task.delay {
    val executor: ScheduledExecutorService =
      Executors.newScheduledThreadPool(
        // A minimum of 8 threads is just a starting point until we learn more on workload characteristics
        scala.math.max(8, java.lang.Runtime.getRuntime.availableProcessors * 2),
        NamedDaemonThreadFactory("quasar-cache-refresh"))

    // TODO: Another approach for error resilience?
    val refresh: Task[Unit] = Task.delay {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def rProc: Process[Task, Unit] =
        periodicRefresh(eval, 1.second, executor).onHalt { e =>
          println(s"job tracker process halted with $e, ${e.asThrowable.getStackTrace.mkString("\n")}, recovering...")
          rProc
        }

      rProc.run.unsafePerformAsync(i =>
        println(i.fold(
          e => "job tracker error: " + e.getMessage + "\n" + e.getStackTrace.mkString("\n") + "\n",
          κ(""))))
    }

    ViewCacheRefreshCtx(refresh, Task.delay(executor.shutdown))
  }

  def refresh(
    f: Free[Eff, ?] ~> Task,
    assigneeId: String
  ): Process[Task, Unit] =
    Process.eval(
      f(compExecMToFree(ViewCacheRefresh.selectCacheForRefresh[Eff])) >>= (_.cata(
        pvc => f(compExecMToFree(ViewCacheRefresh.updateCache(pvc.path, assigneeId))).void.handleWith {
          case ex: Throwable =>
            f(lift(MetaStoreAccess.updateViewCacheErrorMsg(
              pvc.path, ex.getMessage ⊹ ex.getStackTrace.map("  " ⊹ _.toString).mkString("\n"))).into[Eff])
        },
        ().η[Task])))

  def periodicRefresh(
    f: Free[Eff, ?] ~> Task,
    interval: Duration,
    scheduledExecutorService: ScheduledExecutorService
  ): Process[Task, Unit] =
    time.awakeEvery(interval)(Strategy.Executor(scheduledExecutorService), scheduledExecutorService) *>
    refresh(f, Thread.currentThread.getName)

  def compExecMToFree[A](op: QT.CompExecM[A]): Free[Eff, A] =
    op.run.run.value >>= {
      case -\/(semErr) =>
        lift(Task.fail(new RuntimeException(semErr.list.map(_.shows).toList.mkString("; ")))).into
      case \/-(-\/(fsErr)) =>
        lift(Task.fail(new RuntimeException(fsErr.shows))).into
      case \/-( \/-(a)) =>
        a.η[Free[Eff, ?]]
    }
}
