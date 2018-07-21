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

package quasar.common.effect

import slamdata.Predef._
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.immutable.Queue
import quasar.RenderedTree
import quasar.fp._
import quasar.fp.numeric.Natural
import matryoshka._
import matryoshka.patterns.EnvT
import matryoshka.data.cofree._
import argonaut.Json

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
  def forTask[T](repo: TimingRepository,
                 print: Execution => Task[Unit]): ScopeExecution[Task, T] =
    new ScopeExecution[Task, T] {
      def newExecution[A](executionId: ExecutionId, action: ScopeTiming[Task, T] => Task[A]): Task[A] = {
        for {
          newExecutionRef <- SingleExecutionRef.empty
          start <- Task.delay(Instant.now())
          result <- action(ScopeTiming.forTask(newExecutionRef))
          end <- Task.delay(Instant.now())
          newScope <- newExecutionRef.under.read
          execution = Execution(executionId, ExecutionTimings(newScope, start, end))
          _ <- repo.addExecution(execution)
          _ <- print(execution)
        } yield result
      }
    }

  def forFreeTask[F[_], T](repo: TimingRepository,
                           print: Execution => Free[F, Unit])
                          (implicit task: Task :<: F): ScopeExecution[Free[F, ?], T] =
    new ScopeExecution[Free[F, ?], T] {
      def newExecution[A](executionId: ExecutionId, action: ScopeTiming[Free[F, ?], T] => Free[F, A]): Free[F, A] = {
        for {
          newExecutionRef <- Free.liftF(task(SingleExecutionRef.empty))
          start <- Free.liftF(task(Task.delay(Instant.now())))
          result <- action(ScopeTiming.forFreeTask(newExecutionRef))
          end <- Free.liftF(task(Task.delay(Instant.now())))
          newScope <- Free.liftF(task(newExecutionRef.under.read))
          execution = Execution(executionId, ExecutionTimings(newScope, start, end))
          _ <- Free.liftF(task(repo.addExecution(execution)))
          _ <- print(execution)
        } yield result
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
  def forTask[T](ref: SingleExecutionRef): ScopeTiming[Task, T] =
    new ScopeTiming[Task, T] {
      def newScope[A](scopeId: String, f: => Task[A]): Task[A] = {
        for {
          start <- Task.delay(Instant.now())
          result <- f
          end <- Task.delay(Instant.now())
          _ <- ref.addScope(scopeId, start, end)
        } yield result
      }
    }

  def forFreeTask[F[_], T](ref: SingleExecutionRef)
                          (implicit task: Task :<: F): ScopeTiming[Free[F, ?], T] =
  new ScopeTiming[Free[F, ?], T] {
    def newScope[A](scopeId: String, f: => Free[F, A]): Free[F, A] = {
      for {
        start <- Free.liftF(task(Task.delay(Instant.now())))
        result <- f
        end <- Free.liftF(task(Task.delay(Instant.now())))
        _ <- Free.liftF(task(ref.addScope(scopeId, start, end)))
      } yield result
    }
  }
}

final case class ExecutionId(identifier: Long)
object ExecutionId {
  implicit val executionIdShow: Show[ExecutionId] = Show.shows {
    case ExecutionId(identifier) => s"Query execution: $identifier"
  }
}

final case class LabelledInterval(label: String, start: Long, size: Long) {
  def contains(other: LabelledInterval) = start < other.start && (start + size) > (other.start + size)
}

final case class ExecutionTimings(timings: Map[String, (Instant, Instant)], start: Instant, end: Instant) {
  import ExecutionTimings._
  def toIntervalTree: LabelledIntervalTree = toLabelledIntervalTree(this)
  def toRenderedTree: RenderedTree = renderTree(toIntervalTree)
}

final case class Execution(id: ExecutionId, timings: ExecutionTimings)

object ExecutionTimings {
  // Flame graph tree
  type LabelledIntervalTree = Cofree[List, LabelledInterval]
  def LabelledIntervalTree[A](head: LabelledInterval, tail: List[LabelledIntervalTree]): LabelledIntervalTree = Cofree(head, tail)
  def toLabelledIntervalTree(timings: ExecutionTimings): LabelledIntervalTree = {
    def constructIntervalTree(newLabelledInterval: LabelledInterval, tree: LabelledIntervalTree): LabelledIntervalTree = {
      if (newLabelledInterval.contains(tree.head)) {
        LabelledIntervalTree(newLabelledInterval, List(tree))
      } else if (tree.tail.isEmpty) {
        LabelledIntervalTree(tree.head, List(LabelledIntervalTree(newLabelledInterval, Nil)))
      } else {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        def findInsert(list: List[Cofree[List, LabelledInterval]]): List[Cofree[List, LabelledInterval]] = list match {
          case Nil =>
            LabelledIntervalTree(newLabelledInterval, Nil) :: Nil
          case Cofree(interval@LabelledInterval(_, _, _), tail) :: xs
            if interval.contains(newLabelledInterval) =>
            LabelledIntervalTree(interval, findInsert(tail)) :: xs
          case c :: xs =>
            c :: findInsert(xs)
        }
        LabelledIntervalTree(tree.head, findInsert(tree.tail))
      }
    }
    val asIntervalsFromStart: List[LabelledInterval] = timings.timings.map {
      case (label, (start, end)) =>
        LabelledInterval(label, 
          timings.start.until(start, ChronoUnit.MILLIS), start.until(end, ChronoUnit.MILLIS))
    }.toList
    val sortedBySize =
      asIntervalsFromStart.sortBy(-_.size)
    val totalInterval = LabelledInterval("total", 0L, timings.start.until(timings.end, ChronoUnit.MILLIS))
    val initTree = LabelledIntervalTree(totalInterval, Nil)
    sortedBySize.foldRight(initTree)(constructIntervalTree)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def renderTree(tree: LabelledIntervalTree): RenderedTree = {
    val LabelledInterval(label, start, size) = tree.head
    RenderedTree((label + " ") :: Nil, (size.shows + " ms").some, tree.tail.sortBy(_.head.start).map(renderTree))
  }

  def asJson(timings: ExecutionTimings): Json = {
    type PF[A] = EnvT[LabelledInterval, List, A]
    def treeToJson(in: PF[(String, Json)]): Json =
      Json.jObjectFields(
        "start" -> Json.jNumber(in.ask.start),
        "size" -> Json.jNumber(in.ask.size),
        "children" -> Json.jObjectFields(in.lower: _*)
      )
    Recursive[LabelledIntervalTree, PF]
      .zygo[Json, String](timings.toIntervalTree)(_.ask.label, treeToJson)
  }

}

final case class SingleExecutionRef(under: TaskRef[Map[String, (Instant, Instant)]]) {
  def addScope(scopeId: String, start: Instant, end: Instant): Task[Unit] = {
    under.modify {
      _ + ((scopeId, (start, end)))
    }.void
  }
}

object SingleExecutionRef {
  def empty: Task[SingleExecutionRef] =
   TaskRef(Map.empty[String, (Instant, Instant)]).map(SingleExecutionRef(_))
}

final case class TimingRepository(recordedExecutions: Natural,
                                  start: Instant,
                                  under: TaskRef[Queue[Execution]]) {
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def dequeueN[A](n: Int, queue: Queue[A]): Queue[A] =
    if (n <= 0) queue
    else dequeueN(n - 1, queue.dequeue._2)

  def addExecution(execution: Execution): Task[Unit] = {
    under.modify { executions =>
      val newExecutions = executions.enqueue(execution)
      val excessExecutions = newExecutions.size - recordedExecutions.value.toInt
      dequeueN(excessExecutions, newExecutions)
    }.void
  }
}

object TimingRepository {
  def empty(recordedExecutions: Natural): Task[TimingRepository] = {
    for {
      ref <- TaskRef(Queue.empty[Execution])
      now <- Task.delay(Instant.now())
    } yield TimingRepository(recordedExecutions, now, ref)
  }
}
