/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.push

import slamdata.Predef.{Map => _, _}

import quasar.Condition
import quasar.api.destination.{Destination, ResultSink}
import quasar.api.QueryEvaluator
import quasar.api.destination.ResultType
import quasar.api.push.{ResultPush, ResultPushError, ResultRender, Status}
import quasar.api.resource.ResourcePath
import quasar.api.table.TableRef
import quasar.contrib.fs2.pipe.streamLimiter

import java.time.Instant
import java.util.Map
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import fs2.job.{JobManager, Job, Event => JobEvent}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.either._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.{EitherT, Functor, Id, NonEmptyList, OptionT, Traverse, \/}
import shims._

class DefaultResultPush[
  F[_]: Concurrent: Timer, T, D, Q, R] private (
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[F, Q, Stream[F, R]],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, T, Nothing],
    render: ResultRender[F, R],
    pushStatus: Map[T, Status])
    extends ResultPush[F, T, D] {
  import ResultPushError._

  def start(tableId: T, destinationId: D, path: ResourcePath, format: ResultType, limit: Option[Long])
      : F[Condition[ResultPushError[T, D]]] = {

    val writing = for {
      dest <- liftOptionF[F, ResultPushError[T, D], Destination[F]](
        lookupDestination(destinationId),
        ResultPushError.DestinationNotFound(destinationId))

      tableRef <- liftOptionF[F, ResultPushError[T, D], TableRef[Q]](
        lookupTable(tableId),
        ResultPushError.TableNotFound(tableId))

      sink <- format match {
        case ResultType.Csv =>
          liftOptionF[F, ResultPushError[T, D], ResultSink.Csv[F]](
            findCsvSink(dest.sinks).point[F],
            ResultPushError.FormatNotSupported(destinationId, format.shows))
      }

      query = tableRef.query
      columns = tableRef.columns

      evaluated <- EitherT.rightT(format match {
        case ResultType.Csv => {
          evaluator.evaluate(query)
            .map(_.flatMap(render.renderCsv(_, columns)))
        }
      })

      limited = limit.fold(evaluated)(limit => evaluated.through(streamLimiter(limit)))

      sinked = Stream.eval(sink.run(path, columns, limited)).map(Right(_))

      now <- EitherT.rightT(instantNow)
      submitted <- EitherT.rightT(jobManager.submit(Job(tableId, sinked)))
      _ <- EitherT.either[F, ResultPushError[T, D], Unit](
        if (submitted)
          ().right
        else
          ResultPushError.PushAlreadyRunning(tableId).left)
      _ <- EitherT.rightT(Concurrent[F].delay(pushStatus.put(tableId, Status.running(now))))

    } yield ()

    writing.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  def cancel(tableId: T): F[Condition[ExistentialError[T, D]]] =
    (for {
      _ <- ensureTableExists[ExistentialError[T, D]](tableId)
      result <- EitherT.rightT(jobManager.cancel(tableId))
      now <- EitherT.rightT(instantNow)
      _ <- EitherT.rightT(Concurrent[F].delay(pushStatus.put(tableId, Status.canceled(now))))
    } yield result).run.map(Condition.disjunctionIso.reverseGet(_))

  def status(tableId: T): F[TableNotFound[T] \/ Option[Status]] =
    (ensureTableExists[TableNotFound[T]](tableId) *>
      EitherT.rightT(Concurrent[F].delay(Option(pushStatus.get(tableId))))).run

  def cancelAll: F[Unit] =
    jobManager.jobIds
      .flatMap(Traverse[List].traverse(_)(jobManager.cancel(_))).void

  private def instantNow: F[Instant] =
    Timer[F].clock.realTime(MILLISECONDS)
      .map(Instant.ofEpochMilli(_))

  private def findCsvSink(sinks: NonEmptyList[ResultSink[F]]): Option[ResultSink.Csv[F]] =
    sinks.findMapM[Id.Id, ResultSink.Csv[F]] {
      case csvSink @ ResultSink.Csv(_) => csvSink.some
      case _ => none
    }

  private def ensureTableExists[E >: TableNotFound[T] <: ResultPushError[T, D]](tableId: T)
      : EitherT[F, E, Unit] =
    OptionT(lookupTable(tableId))
      .toRight[E](ResultPushError.TableNotFound(tableId))
      .void

  private def liftOptionF[F[_]: Functor, E, A](oa: F[Option[A]], err: E): EitherT[F, E, A] =
    OptionT(oa).toRight[E](err)
}

object DefaultResultPush {
  def apply[F[_]: Concurrent: Timer, T, D, Q, R](
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[F, Q, Stream[F, R]],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, T, Nothing],
    render: ResultRender[F, R]
  ): F[DefaultResultPush[F, T, D, Q, R]] = {
    for {
      pushStatus <- Concurrent[F].delay(new ConcurrentHashMap[T, Status]())
      // we can't keep track of Completed and Failed jobs in this impl, so we consume them from JobManager
      // and update internal state accordingly
      _ <- Concurrent[F].start((jobManager.events.evalMap {
        case JobEvent.Completed(ti, start, duration) =>
          Concurrent[F].delay(
            pushStatus.put(ti, Status.finished(
              epochToInstant(start.epoch),
              epochToInstant(start.epoch + duration))))

        case JobEvent.Failed(ti, start, duration, ex) =>
          Concurrent[F].delay(
            pushStatus.put(ti, Status.failed(
              ex,
              epochToInstant(start.epoch),
              epochToInstant(start.epoch + duration))))
      }).compile.drain)
    } yield new DefaultResultPush(lookupTable, evaluator, lookupDestination, jobManager, render, pushStatus)
  }

  private def epochToInstant(e: FiniteDuration): Instant =
    Instant.ofEpochMilli(e.toMillis)
}
