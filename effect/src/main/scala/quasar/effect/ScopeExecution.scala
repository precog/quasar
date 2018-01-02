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

package quasar.effect

import slamdata.Predef._
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.immutable.TreeMap
import quasar.fp._
import quasar.fp.numeric.Natural

import scalaz._, Scalaz._
import scalaz.concurrent.Task

/** Represents the ability to create an execution scope, within which timing scopes can be created.
  */
trait ScopeExecution[F[_], T] {
  def newExecution[A](executionId: ExecutionId, action: ScopeTiming[F, T] => F[A]): F[A]
}
object ScopeExecution {
  def ignore[F[_], T]: ScopeExecution[F, T] = new ScopeExecution[F, T] {
    def newExecution[A](executionId: ExecutionId, action: ScopeTiming[F, T] => F[A]): F[A] =
      action(ScopeTiming.ignore[F, T])
  }
  def forTask[T](repo: TimingRepository, print: String => Task[Unit]): ScopeExecution[Task, T] =
    new ScopeExecution[Task, T] {
      def newExecution[A](executionId: ExecutionId, action: ScopeTiming[Task, T] => Task[A]): Task[A] = {
        for {
          a <- action(ScopeTiming.forTask(executionId, repo))
          repository <- repo.repo.read
          shownTimings <- repository.get(executionId).fold(().point[Task])(timings => print(s"${executionId.shows}\n${timings.shows}"))
        } yield a
      }
    }

  def forFreeTask[F[_], T](repo: TimingRepository, print: String => Free[F, Unit])
                          (implicit task: Task :<: F): ScopeExecution[Free[F, ?], T] =
    new ScopeExecution[Free[F, ?], T] {
      def newExecution[A](executionId: ExecutionId, action: ScopeTiming[Free[F, ?], T] => Free[F, A]): Free[F, A] = {
        for {
          a <- action(ScopeTiming.forFreeTask(executionId, repo))
          repository <- Free.liftF(task(repo.repo.read))
          shownTimings <- repository.get(executionId).fold(().point[Free[F, ?]])(timings => print(s"${executionId.shows}\n${timings.shows}"))
        } yield a
      }
    }
}

/** Represents the ability to create a timing scope.
  */
trait ScopeTiming[F[_], T] {
  def newScope[A](scopeId: String, fa: => F[A]): F[A]
}

object ScopeTiming {
  def ignore[F[_], T]: ScopeTiming[F, T] = new ScopeTiming[F, T] {
    def newScope[A](scopeId: String, fa: => F[A]): F[A] = fa
  }
  def forTask[T](executionId: ExecutionId, repo: TimingRepository): ScopeTiming[Task, T] =
    new ScopeTiming[Task, T] {
      def newScope[A](scopeId: String, f: => Task[A]): Task[A] = {
        for {
          start <- Task.delay(Instant.now())
          result <- f
          end <- Task.delay(Instant.now())
          _ <- repo.addScope(executionId, scopeId, start, end)
        } yield result
      }
    }

  def forFreeTask[F[_], T](executionId: ExecutionId, repo: TimingRepository)
                          (implicit task: Task :<: F): ScopeTiming[Free[F, ?], T] =
  new ScopeTiming[Free[F, ?], T] {
    def newScope[A](scopeId: String, f: => Free[F, A]): Free[F, A] = {
      for {
        start <- Free.liftF(task(Task.delay(Instant.now())))
        result <- f
        end <- Free.liftF(task(Task.delay(Instant.now())))
        _ <- Free.liftF(task(repo.addScope(executionId, scopeId, start, end)))
      } yield result
    }
  }
}

final case class ExecutionId(index: Long, startTime: Instant)
object ExecutionId {
  import scala.Ordering
  implicit val executionIdOrdering: Ordering[ExecutionId] = Ordering.ordered[Instant](x => x).on(_.startTime)
  implicit val executionIdShow: Show[ExecutionId] = Show.shows {
    case ExecutionId(index, start) => s"Query execution $index began at $start"
  }
  def ofRef(ref: TaskRef[Long]): Task[ExecutionId] =
    for {
      i <- ref.modify(_ + 1)
      now <- Task.delay(Instant.now())
    } yield ExecutionId(i, now)
}

final case class ExecutionTimings(timings: Map[String, (Instant, Instant)])
object ExecutionTimings {
  implicit val executionTimingsShow: Show[ExecutionTimings] = Show.show {
    executionTimings =>
      executionTimings.timings.map {
        case (identifier, (start, end)) =>
          val millisBetween = start.until(end, ChronoUnit.MILLIS)
          Cord("phase ") ++
          Cord(identifier) ++
          Cord(": ") ++
          millisBetween.show ++
          Cord(" ms")
      }.mkString("\n")
  }
}

final case class TimingRepository(recordedExecutions: Natural, repo: TaskRef[TreeMap[ExecutionId, ExecutionTimings]]) {
  def addScope(executionId: ExecutionId, scopeId: String, start: Instant, end: Instant): Task[Unit] = {
    repo.modify { executions =>
      val newExecution = executions.get(executionId).fold {
        ExecutionTimings(Map((scopeId, (start, end))))
      } { case ExecutionTimings(timings) => ExecutionTimings(timings + ((scopeId, (start, end)))) }
      val newExecutions = executions + ((executionId, newExecution))
      val excessExecutions = java.lang.Math.max(newExecutions.size - recordedExecutions.value.toInt, 0)
      newExecutions.drop(excessExecutions)
    }.void
  }
}

object TimingRepository {
  def empty(recordedExecutions: Natural): Task[TimingRepository] = {
    TaskRef(TreeMap.empty[ExecutionId, ExecutionTimings])
      .map(TimingRepository(recordedExecutions, _))
  }
}
