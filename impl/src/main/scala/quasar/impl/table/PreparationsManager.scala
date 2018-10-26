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

package quasar.impl.table

import slamdata.Predef._

import quasar.Condition
import quasar.api.QueryEvaluator
import quasar.api.table.PreparationEvent

import cats.effect.{Concurrent, ConcurrentEffect}

import fs2.Stream
import fs2.concurrent.{Queue, SignallingRef}

// monad/traverse syntax conflict
import scalaz.{-\/, \/-, \/, Equal, OptionT, Scalaz}, Scalaz._

import shims._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentHashMap

class PreparationsManager[F[_]: Concurrent, I, Q, R] private (
    evaluator: QueryEvaluator[F, Q, R],
    notificationsQ: Queue[F, Option[PreparationEvent[I]]],
    background: Stream[F, Nothing] => F[Unit])(
    runToStore: (I, R) => F[Stream[F, Unit]]) {

  import PreparationsManager._

  private val status: ConcurrentHashMap[I, F[Unit] \/ Preparation[F]] =
    new ConcurrentHashMap[I, F[Unit] \/ Preparation[F]]

  val F = Concurrent[F]

  val notifications: Stream[F, PreparationEvent[I]] = notificationsQ.dequeue.unNoneTerminate

  // fake Equal for fake parametricity
  def prepareTable(tableId: I, query: Q)(implicit I: Equal[I]): F[Condition[InProgressError[I]]] = {
    for {
      s <- SignallingRef[F, Boolean](false)
      cancel = s.set(true)

      check <- F.delay(Option(status.putIfAbsent(tableId, -\/(cancel))).isDefined)

      back <- if (check) {
        Condition.abnormal(InProgressError(tableId)).point[F]
      } else {
        val persist = for {
          result <- Stream.eval(evaluator.evaluate(query))
          persistStream <- Stream.eval(runToStore(tableId, result))
          results <- persistStream
        } yield results

        val configured = Stream.eval(F.delay(OffsetDateTime.now())) flatMap { start =>
          val preparation = Preparation(cancel, start)
          val halted = persist.interruptWhen(s)

          val handled = halted handleErrorWith { t =>
            val eff = for {
              end <- F.delay(OffsetDateTime.now())
              _ <- s.set(true)    // prevent the onComplete handler

              _ <- notificationsQ.enqueue1(
                Some(
                  PreparationEvent.PreparationErrored(
                    tableId,
                    start,
                    (end.toEpochSecond - start.toEpochSecond).seconds,
                    t)))
            } yield ()

            Stream.eval_(eff)
          } onComplete {
            val eff = for {
              canceled <- s.get

              _ <- if (canceled) {
                ().point[F]
              } else {
                for {
                  end <- F.delay(OffsetDateTime.now())

                  _ <- notificationsQ.enqueue1(
                    Some(
                      PreparationEvent.PreparationSucceeded(
                        tableId,
                        start,
                        (end.toEpochSecond - start.toEpochSecond).seconds)))
                } yield ()
              }
            } yield ()

            Stream.eval_(eff)
          }

          Stream.bracket(
            F.delay(status.replace(tableId, -\/(cancel), \/-(preparation))))(
            _ => F.delay(status.remove(tableId, \/-(preparation))).void
          ).flatMap(flag => if (flag) handled else Stream.empty)
        }

        background(configured.drain).as(Condition.normal[InProgressError[I]]())
      }
    } yield back
  }

  def preparationStatus(tableId: I)(implicit I: Equal[I]): F[Status] = {
    F.delay(Option(status.get(tableId))) map {
      case Some(\/-(Preparation(_, start))) => Status.Started(start)
      case Some(-\/(_)) => Status.Pending
      case None => Status.Unknown
    }
  }

  def cancelPreparation(tableId: I)(implicit I: Equal[I]): F[Condition[NotInProgressError[I]]] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    lazy val eff: OptionT[F, Unit] = for {
      live <- OptionT(F.delay(Option(status.get(tableId))))
      removed <- F.delay(status.remove(tableId, live)).liftM[OptionT]

      _ <- if (removed)
        live.fold(x => x, _.cancel).liftM[OptionT]
      else
        eff
    } yield ()

    eff.run map {
      case Some(_) => Condition.Normal()
      case None => Condition.Abnormal(NotInProgressError(tableId))
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def cancelAll(implicit I: Equal[I]): F[Unit] = {
    for {
      keys <- F.delay(status.keys.asScala.toList)
      results <- keys.traverse(cancelPreparation)

      _ <- if (results.isEmpty)
        ().point[F]
      else
        cancelAll   // handle race condition of new enqueue
    } yield ()
  }
}

object PreparationsManager {

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def apply[F[_]: ConcurrentEffect, I, Q, R](
      evaluator: QueryEvaluator[F, Q, R],
      maxStreams: Int = 10,
      maxConcurrency: Int = 10,   // it's a good idea to keep this equal to maxStreams (different values are sometimes useful for testing)
      maxNotifications: Int = 10)(
      runToStore: (I, R) => F[Stream[F, Unit]])(
      implicit ec: ExecutionContext): Stream[F, PreparationsManager[F, I, Q, R]] = {

    for {
      q <-
        Stream.eval(Queue.bounded[F, Stream[F, Nothing]](maxStreams))

      notificationsQ <-
        Stream.eval(Queue.bounded[F, Option[PreparationEvent[I]]](maxNotifications))

      emit = Stream(new PreparationsManager[F, I, Q, R](evaluator, notificationsQ, q.enqueue1(_))(runToStore))

      back <- emit.concurrently(q.dequeue.parJoin(maxConcurrency)) onComplete {
        Stream.eval_(notificationsQ.enqueue1(None))
      }
    } yield back
  }

  private final case class Preparation[F[_]](
      cancel: F[Unit],
      start: OffsetDateTime)

  final case class InProgressError[I](tableId: I)
  final case class NotInProgressError[I](tableId: I)

  sealed trait Status extends Product with Serializable

  object Status {
    final case class Started(start: OffsetDateTime) extends Status
    case object Pending extends Status
    case object Unknown extends Status
  }
}