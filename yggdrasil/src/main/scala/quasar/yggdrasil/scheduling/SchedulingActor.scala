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

package quasar.yggdrasil.scheduling

import quasar.precog.common.Path
import quasar.precog.common.accounts.AccountFinder
import quasar.precog.common.jobs._
import quasar.precog.common.security._
import quasar.precog.util.PrecogUnit

import quasar.yggdrasil._
import quasar.yggdrasil.execution._
import quasar.yggdrasil.table.Slice
import quasar.yggdrasil.vfs._

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import quasar.blueeyes.util.Clock

import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import java.time.{Duration => JodaDuration, LocalDateTime, ZoneOffset}

import org.quartz.CronExpression

import org.slf4s.Logging

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.concurrent.ExecutionContext.Implicits.global // FIXME what is this thing

import scalaz.{Ordering => _, idInstance => _, _}
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.syntax.id._
import scalaz.syntax.traverse._
import scalaz.syntax.std.either._
import scalaz.syntax.std.option._

import scala.concurrent.ExecutionContext.Implicits.global     // wtf fix me??!!

sealed trait SchedulingMessage

case class AddTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long])

case class DeleteTask(id: UUID)

case class StatusForTask(id: UUID, limit: Option[Int])

trait SchedulingActorModule extends SecureVFSModule[Future, Slice] {
  object SchedulingActor {
    type TaskKey = (Path, Path)

    private[SchedulingActor] case object WakeForRun extends SchedulingMessage

    private[SchedulingActor] case class AddTasksToQueue(tasks: Seq[ScheduledTask]) extends SchedulingMessage

    private[SchedulingActor] case class RemoveTaskFromQueue(id: UUID) extends SchedulingMessage

    private[SchedulingActor] case class TaskComplete(id: UUID, endedAt: LocalDateTime, total: Long, error: Option[String]) extends SchedulingMessage

    private[SchedulingActor] case class TaskInProgress(task: ScheduledTask, startedAt: LocalDateTime)
  }

  class SchedulingActor(
      jobManager: JobManager[Future],
      permissionsFinder: PermissionsFinder[Future],
      storage: ScheduleStorage[Future],
      platform: Platform[Future, Slice, StreamT[Future, Slice]],
      clock: Clock,
      storageTimeout: Duration = Duration(30, TimeUnit.SECONDS),
      resourceTimeout: Timeout = Timeout(10, TimeUnit.SECONDS)) extends Actor with Logging {
    import SchedulingActor._

    private[this] final implicit val scheduleOrder: Ordering[(LocalDateTime, ScheduledTask)] =
      Ordering.by(_._1.toInstant(ZoneOffset.UTC).toEpochMilli())

    private[this] implicit val M: Monad[Future] = futureInstance

    // Although PriorityQueue is mutable, we're going to treat it as immutable since most methods on it act that way
    private[this] var scheduleQueue = PriorityQueue.empty[(LocalDateTime, ScheduledTask)]

    private[this] var scheduledAwake: Option[Cancellable] = None

    private[this] var running = Map.empty[TaskKey, TaskInProgress]

    // We need to keep this around in case a task is running when its removal is requested
    private[this] var pendingRemovals = Set.empty[UUID]

    override def preStart = {
      val now = new Date

      storage.listTasks onSuccess {
        case tasks => self ! AddTasksToQueue(tasks)
      }
    }

    override def postStop = {
      scheduledAwake foreach { sa =>
        if (! sa.isCancelled) {
          sa.cancel()
        }
      }
    }

    def scheduleNextTask(): Unit = {
      // Just make sure we don't multi-schedule
      scheduledAwake foreach { sa =>
        if (! sa.isCancelled) {
          sa.cancel()
        }
      }

      scheduleQueue.headOption foreach { head =>
        val delay = Duration(JodaDuration.between(LocalDateTime.now, head._1).toMillis, TimeUnit.MILLISECONDS)

        scheduledAwake = Some(context.system.scheduler.scheduleOnce(delay, self, WakeForRun))
      }
    }

    def nextRun(threshold: Date, task: ScheduledTask) = {
      task.repeat.flatMap { sched => Option(sched.getNextValidTimeAfter(threshold)) } map { nextTime =>
        (LocalDateTime.from(nextTime.toInstant), task)
      }
    }

    def rescheduleTasks(tasks: Seq[ScheduledTask]): Unit = {
      val (toRemove, toReschedule) = tasks.partition { task => pendingRemovals.contains(task.id) }

      toRemove foreach { task =>
        log.info("Removing completed task after run: " + task.id)
        pendingRemovals -= task.id
      }

      scheduleQueue ++= {
        toReschedule flatMap { task =>
          nextRun(new Date, task) unsafeTap { next =>
            if (next.isEmpty) log.warn("No further run times for " + task)
          }
        }
      }

      scheduleNextTask()
    }

    def removeTask(id: UUID) = {
      scheduleQueue = scheduleQueue.filterNot(_._2.id == id)
      pendingRemovals += id
    }

    def executeTask(task: ScheduledTask): Future[PrecogUnit] = {
      import EvaluationError._

      if (running.contains((task.source, task.sink))) {
        // We don't allow for more than one concurrent instance of a given task
        Future successful PrecogUnit // AP: in precog we called `Promise` instead of `Future`
      } else {
        def consumeStream(totalSize: Long, stream: StreamT[Future, Slice]): Future[Long] = {
          stream.uncons flatMap {
            case Some((x, xs)) => consumeStream(totalSize + x.size, xs)
            case None => M.point(totalSize)
          }
        }

        val ourself = self
        val startedAt = LocalDateTime.now

        implicit val readTimeout = resourceTimeout

        // This cannot occur inside a Future, or we would be exposing Actor state outside of this thread
        running += ((task.source, task.sink) -> TaskInProgress(task, startedAt))


        val execution = for {
          basePath <- EitherT(M point { task.source.prefix \/> invalidState("Path %s cannot be relativized.".format(task.source.path)) })
          cachingResult <- platform.vfs.executeAndCache(platform, basePath, task.context, QueryOptions(timeout = task.timeout), Some(task.sink), Some(task.taskName))


        } yield cachingResult

        val back = execution.fold[Future[PrecogUnit]](
          failure => M point {
            log.error("An error was encountered processing a scheduled query execution: " + failure)
            ourself ! TaskComplete(task.id, clock.now(), 0, Some(failure.toString)) : PrecogUnit
          },
          storedQueryResult => {
            consumeStream(0, storedQueryResult.data) map { totalSize =>
              ourself ! TaskComplete(task.id, clock.now(), totalSize, None)
              PrecogUnit
            } recoverWith {
              case t: Throwable =>
                for {
                  _ <- storedQueryResult.cachingJob.traverse { jobId =>
                    jobManager.abort(jobId, t.getMessage) map {
                      case Right(jobAbortSuccess) =>
                          ourself ! TaskComplete(task.id, clock.now(), 0, Option(t.getMessage) orElse Some(t.getClass.toString))
                      case Left(jobAbortFailure) => sys.error(jobAbortFailure.toString)
                    }
                  }
                } yield PrecogUnit
            }
          }
        ) flatMap {
          identity[Future[PrecogUnit]](_)
        }

        back onFailure {
          case t: Throwable =>
            log.error("Scheduled query execution failed by thrown error.", t)
            ourself ! TaskComplete(task.id, clock.now(), 0, Option(t.getMessage) orElse Some(t.getClass.toString)) : PrecogUnit
        }

        back
      }
    }

    def receive = {
      case AddTask(repeat, apiKey, authorities, context, source, sink, timeout) =>
        val ourself = self
        val taskId = UUID.randomUUID()
        val newTask = ScheduledTask(taskId, repeat, apiKey, authorities, context, source, sink, timeout)
        val addResult: EitherT[Future, String, PrecogUnit] = repeat match {
          case None =>
            EitherT.right(executeTask(newTask))

          case Some(_) =>
            storage.addTask(newTask) map { task =>
              ourself ! AddTasksToQueue(Seq(task))
            }
        }

        addResult.run.map(_ => taskId) recover {
          case t: Throwable =>
            log.error("Error adding task " + newTask, t)
            \/.left("Internal error adding task")
        } pipeTo sender

      case DeleteTask(id) =>
        val ourself = self
        val deleteResult = storage.deleteTask(id) map { result =>
          ourself ! RemoveTaskFromQueue(id)
          result
        }

        deleteResult.run recover {
          case t: Throwable =>
            log.error("Error deleting task " + id, t)
            \/.left("Internal error deleting task")
        } pipeTo sender

      case StatusForTask(id, limit) =>
        storage.statusFor(id, limit) map(Success(_)) recover {
          case t: Throwable =>
            log.error("Error getting status for task " + id, t)
            Failure("Internal error getting status for task")
        } pipeTo sender

      case AddTasksToQueue(tasks) =>
        rescheduleTasks(tasks)

      case RemoveTaskFromQueue(id) =>
        removeTask(id)

      case WakeForRun =>
        val now = LocalDateTime.now
        val (toRun, newQueue) = scheduleQueue.partition(_._1.isAfter(now))
        scheduleQueue = newQueue
        toRun.map(_._2).foreach(executeTask)

        scheduleNextTask()

      case TaskComplete(id, endedAt, total, error) =>
        running.values.find(_.task.id == id) match {
          case Some(TaskInProgress(task, startAt)) =>
            error match {
              case None =>
                log.info("Scheduled task %s completed with %d records in %d millis".format(id, total, (JodaDuration.between(startAt, endedAt)).toMillis))

              case Some(error) =>
                log.warn("Scheduled task %s failed after %d millis: %s".format(id, (JodaDuration.between(startAt, clock.now())).toMillis, error))
            }

            storage.reportRun(ScheduledRunReport(id, startAt, endedAt, total, error.toList))
            running -= (task.source -> task.sink)
            rescheduleTasks(Seq(task))

          case None =>
            log.error("Task completion reported for unknown task " + id)
        }
    }
  }
}
