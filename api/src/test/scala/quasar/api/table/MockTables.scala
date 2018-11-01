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

import slamdata.Predef.{Boolean, None, Some, String, Unit}

import quasar.Condition
import quasar.contrib.scalaz.MonadState_
import quasar.contrib.std.uuid._

import java.util.UUID

import fs2.Stream
import scalaz.{\/, IMap, Monad}
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

final class MockTables[F[_]: Monad: MockTables.TablesMockState]
  extends Tables[F, UUID, String, String, String] {

  import MockTables._
  import TableError._

  val store = MonadState_[F, IMap[UUID, MockTable]]

  def allTables: Stream[F, (UUID, TableRef[String], PreparationStatus)] =
    Stream.force(store.get map { s =>
      Stream.emits(s.toList map {
        case (uuid, MockTable(table, status)) => (uuid, table, status)
      }).covary[F]
    })

  def table(tableId: UUID): F[ExistenceError[UUID] \/ TableRef[String]] =
    store.gets(_.lookup(tableId)
      .map(_.table)
      .toRightDisjunction(TableNotFound(tableId)))

  def createTable(table: TableRef[String]): F[NameConflict \/ UUID] =
    store.get map { store =>
        (store, store.values.map(_.table.name).contains(table.name))
    } flatMap { case (s, exists) =>
      if (exists)
        Monad[F].point(NameConflict(table.name).left[UUID])
      else
        for {
          id <- UUID.randomUUID.point[F]
          back <- store.put(s.insert(id,
            MockTable(table, PreparationStatus(PreparedStatus.Unprepared, OngoingStatus.NotPreparing))))
        } yield id.right[NameConflict]
    }

  def replaceTable(tableId: UUID, table: TableRef[String])
      : F[Condition[ModificationError[UUID]]] =
    store.gets(_.lookup(tableId)) flatMap { res =>
      val modified = res map { s =>
        store.modify(_.insert(tableId, MockTable(table, s.status)))
          .as(Condition.normal[ModificationError[UUID]]())
      }
      modified.getOrElse(Condition.abnormal[ModificationError[UUID]](
        TableNotFound(tableId)).point[F])
    }

  // mock tables prepare immediately
  def prepareTable(tableId: UUID): F[Condition[PrePreparationError[UUID]]] =
    store.get.flatMap(stateMap => stateMap.lookup(tableId) match {
      case Some(MockTable(_, PreparationStatus(_, OngoingStatus.Preparing))) =>
        Condition.abnormal(
          PreparationInProgress(tableId): PrePreparationError[UUID]).point[F]
      case Some(state) =>
        store.put {
          stateMap.insert(tableId,
            MockTable(state.table,
              PreparationStatus(PreparedStatus.Prepared, OngoingStatus.NotPreparing)))
        }.as(Condition.normal())
      case None =>
        Condition.abnormal(
          TableNotFound(tableId): PrePreparationError[UUID]).point[F]
    })

  def preparationEvents: Stream[F, PreparationEvent[UUID]] =
    Stream.empty

  def preparationStatus(tableId: UUID): F[ExistenceError[UUID] \/ PreparationStatus] =
    store.gets(_.lookup(tableId)
      .map(_.status)
      .toRightDisjunction(TableNotFound(tableId)))

  def cancelPreparation(tableId: UUID): F[Condition[PreparationNotInProgress[UUID]]] =
    store.get.flatMap(stateMap => stateMap.lookup(tableId) match {
      case Some(MockTable(table, PreparationStatus(prep, OngoingStatus.Preparing))) =>
        val inserted = store put {
          stateMap.insert(tableId,
            MockTable(table, PreparationStatus(prep, OngoingStatus.NotPreparing)))
        }
        inserted.as(Condition.normal())

      case Some(MockTable(_, PreparationStatus(_, OngoingStatus.NotPreparing))) =>
        Condition.abnormal[PreparationNotInProgress[UUID]](
          PreparationNotInProgress(tableId)).point[F]

      case None =>
        Condition.normal().point[F]
    })

  def cancelAllPreparations: F[Unit] = {
    val cancelled = store.get flatMap { stateMap =>
      val updatedMap = stateMap map {
        case MockTable(ref, PreparationStatus(prep, _)) =>
          MockTable(ref, PreparationStatus(prep, OngoingStatus.NotPreparing))
      }
      store.put(updatedMap)
    }

    cancelled.void
  }

  // the live data is the query
  def liveData(tableId: UUID): F[ExistenceError[UUID] \/ String] =
    store.gets(_.lookup(tableId)
      .map(_.table.query)
      .toRightDisjunction(TableNotFound(tableId): ExistenceError[UUID]))

  // the prepared data is the query
  def preparedData(tableId: UUID): F[ExistenceError[UUID] \/ PreparationResult[UUID, String]] =
    store gets { t =>
      val result = t.lookup(tableId) map { mock =>
        if (isPrepared(mock.status))
          PreparationResult.Available[UUID, String](tableId, mock.table.query)
        else
          PreparationResult.Unavailable[UUID, String](tableId)
      }

      result.toRightDisjunction(TableNotFound(tableId): ExistenceError[UUID])
    }

  def preparedSchema(tableId: UUID): F[ExistenceError[UUID] \/ PreparationResult[UUID, String]] =
    store gets { t =>
      val result = t.lookup(tableId) map { s =>
        if (isPrepared(s.status))
          PreparationResult.Available[UUID, String](tableId, tableId.toString)
        else
          PreparationResult.Unavailable[UUID, String](tableId)
      }

      result.toRightDisjunction(TableNotFound(tableId): ExistenceError[UUID])
    }

  private def isPrepared(status: PreparationStatus): Boolean =
    status match {
      case PreparationStatus(PreparedStatus.Prepared, _) => true
      case _ => false
    }
}

object MockTables {
  final case class MockTable(table: TableRef[String], status: PreparationStatus)

  type TablesMockState[F[_]] = MonadState_[F, IMap[UUID, MockTable]]

  def apply[F[_]: Monad: TablesMockState[?[_]]] = new MockTables[F]
}
