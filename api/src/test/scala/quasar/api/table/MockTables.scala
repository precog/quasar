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

package quasar.api.table

import slamdata.Predef.{List, None, Option, Some, String, Unit}

import quasar.Condition
import quasar.contrib.scalaz.MonadState_
import quasar.contrib.std.uuid._

import java.time.{Duration, OffsetDateTime}
import java.util.UUID

import scalaz.{\/, IMap, Monad}
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

final class MockTables[F[_]: Monad: MockTables.TablesMockState]
  extends Tables[F, List, UUID, String, String] {

  import MockTables._
  import PreparationStatus._
  import TableError._

  val store = MonadState_[F, IMap[UUID, MockTable]]

  def allTables: F[List[(UUID, Table[String])]] =
    store.get.map(_.map(_.table).toList)

  def table(tableId: UUID): F[ExistenceError[UUID] \/ Table[String]] =
    store.gets(_.lookup(tableId)
      .map(_.table)
      .toRightDisjunction(TableNotFound(tableId)))

  def createTable(table: Table[String]): F[CreationError \/ UUID] =
    store.get.map { store => 
        (store, store.values.map(_.table.name).contains(table.name))
    } flatMap { case (s, exists) =>
      if (exists)
        Monad[F].point(NameConflict(table.name).left[UUID])
      else
        for {
          id <- UUID.randomUUID.point[F]
          back <- store.put(s.insert(id, MockTable(table, Unprepared)))
        } yield id.right[CreationError]
    }

  def replaceTable(tableId: UUID, table: Table[String])
      : F[Condition[ModificationError[UUID]]] =
    store.gets(_.lookup(tableId)).flatMap {
      _.map(_.status match {
        case Preparing(_, Some(_)) =>
          Condition.abnormal[ModificationError[UUID]](
            PreparationExists(tableId)).point[F]

        case Preparing(_, None) =>
          Condition.abnormal[ModificationError[UUID]](
            PreparationInProgress(tableId)).point[F]

        case Prepared(_, _) =>
          Condition.abnormal[ModificationError[UUID]](
            PreparationExists(tableId)).point[F]

        case Errored(_, _, _, Some(_)) =>
          Condition.abnormal[ModificationError[UUID]](
            PreparationExists(tableId)).point[F]

        case Errored(_, _, _, None) =>
          store.modify(_.insert(tableId, MockTable(table, Unprepared)))
            .as(Condition.normal[ModificationError[UUID]]())

        case Unprepared =>
          store.modify(_.insert(tableId, MockTable(table, Unprepared)))
            .as(Condition.normal[ModificationError[UUID]]())
      }).getOrElse {
        Condition.abnormal[ModificationError[UUID]](
          TableNotFound(tableId)).point[F]
      }
    }

  // mock tables prepare immediately
  def prepareTable(tableId: UUID): F[Condition[PrePreparationError[UUID]]] =
    store.get.flatMap(stateMap => stateMap.lookup(tableId) match {
      case Some(state) =>
        store.put {
          stateMap.insert(tableId,
            MockTable(state.table, Prepared(OffsetDateTime.now(), Duration.ZERO)))
        }.as(Condition.normal())
      case None =>
        Condition.abnormal(
          TableNotFound(tableId): PrePreparationError[UUID]).point[F]
    })

  def preparationStatus(tableId: UUID): F[ExistenceError[UUID] \/ PreparationStatus] =
    store.gets(_.lookup(tableId)
      .map(_.status)
      .toRightDisjunction(TableNotFound(tableId)))

  // mock cannot cancel preparations
  def cancelPreparation(tableId: UUID): F[Condition[ExistenceError[UUID]]] =
    store.get.map(_.lookup(tableId).fold(
      Condition.abnormal(TableNotFound(tableId): ExistenceError[UUID]))(
      _ => Condition.normal()))

  // mock cannot cancel preparations
  def cancelAllPreparations: F[Unit] = ().point[F]

  // the prepared data is the table id
  def preparedData(tableId: UUID): F[ExistenceError[UUID] \/ PreparationResult[UUID, String]] =
    store.gets(_.lookup(tableId).map { s =>
      isPrepared(s.status) match {
        case None =>
          PreparationResult.Unavailable[UUID, String](tableId)
        case Some(since) =>
          PreparationResult.Available[UUID, String](tableId, since, tableId.toString)
      }
    }.toRightDisjunction(TableNotFound(tableId): ExistenceError[UUID]))

  ////

  private def isPrepared(status: PreparationStatus): Option[OffsetDateTime] =
    status match {
      case Preparing(_, Some(Prepared(startedAt, duration))) =>
        Some(calculateFinishTime(startedAt, duration))
      case Preparing(_, None) =>
        None
      case Prepared(startedAt, duration) =>
        Some(calculateFinishTime(startedAt, duration))
      case Errored(_, _, _, Some(Prepared(startedAt, duration))) =>
        Some(calculateFinishTime(startedAt, duration))
      case Errored(_, _, _, None) =>
        None
      case Unprepared =>
        None
    }

  private def calculateFinishTime(startedAt: OffsetDateTime, duration: Duration)
      : OffsetDateTime =
    startedAt.plus(duration)
}

object MockTables {
  final case class MockTable(table: Table[String], status: PreparationStatus)

  type TablesMockState[F[_]] = MonadState_[F, IMap[UUID, MockTable]]

  def apply[F[_]: Monad: TablesMockState] = new MockTables[F]
}
