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

package quasar.impl.table

import slamdata.Predef._

import quasar.Condition
import quasar.api.QueryEvaluator
import quasar.api.table.{
  OngoingStatus,
  PreparationEvent,
  PreparationResult,
  PreparationStatus,
  PreparedStatus,
  TableError,
  TableRef,
  Tables
}
import quasar.impl.storage.IndexedStore

import cats.effect.Effect

import fs2.Stream

import scalaz.{\/, -\/, \/-, Equal}
import scalaz.std.option
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._

import shims._

final class DefaultTables[F[_]: Effect, I: Equal, Q, D, S](
    freshId: F[I],
    tableStore: IndexedStore[F, I, TableRef[Q]],
    evaluator: QueryEvaluator[F, Q, D],
    manager: PreparationsManager[F, I, Q, D],
    lookupTableData: I => F[Option[D]],
    lookupTableSchema: I => F[Option[S]])
    extends Tables[F, I, Q, D, S] {

  import TableError.{
    CreateError,
    ExistenceError,
    ModificationError,
    NameConflict,
    PreparationInProgress,
    PreparationNotInProgress,
    PrePreparationError,
    TableNotFound
  }

  def allTables: Stream[F, (I, TableRef[Q], PreparationStatus)] =
    tableStore.entries.evalMap {
      case (id, table) =>
        liveStatus(id).map((id, table, _))
    }

  def cancelAllPreparations: F[Unit] =
    manager.cancelAll

  def cancelPreparation(tableId: I): F[Condition[PreparationNotInProgress[I]]] =
    manager.cancelPreparation(tableId).map(_ map {
      case PreparationsManager.NotInProgressError(i) =>
        PreparationNotInProgress(i)
    })

  def createTable(table: TableRef[Q]): F[CreateError[I] \/ I] =
    tableStore.entries
      .exists(_._2.name === table.name)
      .compile.last
      .flatMap {
        case Some(true) =>
          (NameConflict(table.name): CreateError[I]).left.pure[F]

        case _ => for {
          tableId <- freshId
          _ <- tableStore.insert(tableId, table)
        } yield tableId.right
      }

  def preparationStatus(tableId: I): F[ExistenceError[I] \/ PreparationStatus] =
    for {
      exists <- table(tableId)
      status <- liveStatus(tableId)
    } yield exists.map(_ => status)

  def prepareTable(tableId: I): F[Condition[PrePreparationError[I]]] = {
    val queryF: F[PrePreparationError[I] \/ Q] =
      table(tableId).map(_.map(_.query))

    queryF.flatMap {
      case -\/(err) =>
        Condition.abnormal(err).point[F]
      case \/-(query) =>
        manager.prepareTable(tableId, query) map {
          _.map {
            case PreparationsManager.InProgressError(id) =>
              PreparationInProgress(id)
          }
        }
    }
  }

  def preparationEvents: Stream[F, PreparationEvent[I]] =
    manager.notifications

  def preparedData(tableId: I): F[ExistenceError[I] \/ PreparationResult[I, D]] =
    tableStore.lookup(tableId) flatMap {
      case Some(_) =>
        lookupTableData(tableId) map {
          case Some(dataset) =>
            PreparationResult.Available[I, D](tableId, dataset).right
          case None =>
            PreparationResult.Unavailable[I, D](tableId).right
        }
      case None =>
        (TableNotFound(tableId): ExistenceError[I]).left.pure[F]
    }

  def replaceTable(tableId: I, table: TableRef[Q]): F[Condition[ModificationError[I]]] =
    tableStore.lookup(tableId) flatMap {
      case Some(_) =>
        val nameIsAvailable =
          tableStore.entries
            .collect { case (id, ref) if ref.name === table.name => id }
            .compile.fold(true)((b, id) => b && (id === tableId))

        nameIsAvailable.ifM(
          tableStore.insert(tableId, table).as(Condition.normal[ModificationError[I]]()),
          Condition.abnormal[ModificationError[I]](NameConflict(table.name)).pure[F])

      case None =>
        Condition.abnormal(TableNotFound(tableId): ModificationError[I]).pure[F]
    }

  def table(tableId: I): F[ExistenceError[I] \/ TableRef[Q]] =
    tableStore.lookup(tableId).map(option.toRight(_)(TableNotFound(tableId)))

  def preparedSchema(tableId: I): F[ExistenceError[I] \/ PreparationResult[I, S]] =
    tableStore.lookup(tableId) flatMap {
      case Some(_) =>
        lookupTableSchema(tableId) map {
          case Some(s) =>
            PreparationResult.Available[I, S](tableId, s).right
          case None =>
            PreparationResult.Unavailable[I, S](tableId).right
        }
      case None =>
        (TableNotFound(tableId): ExistenceError[I]).left.pure[F]
    }

  ////

  private def liveStatus(tableId: I): F[PreparationStatus] = {
    import PreparationsManager.Status

    val prepared: F[PreparedStatus] =
      lookupTableData(tableId) map { t =>
        if (t.isDefined) PreparedStatus.Prepared
        else PreparedStatus.Unprepared
      }

    val ongoing: F[OngoingStatus] =
      manager.preparationStatus(tableId) map {
        case Status.Started(_) | Status.Pending => OngoingStatus.Preparing
        case Status.Unknown => OngoingStatus.NotPreparing
      }

    (prepared |@| ongoing)(PreparationStatus(_, _))
  }
}

object DefaultTables {
  def apply[F[_]: Effect, I: Equal, Q, D, S](
      freshId: F[I],
      tableStore: IndexedStore[F, I, TableRef[Q]],
      evaluator: QueryEvaluator[F, Q, D],
      manager: PreparationsManager[F, I, Q, D],
      lookupTableData: I => F[Option[D]],
      lookupTableSchema: I => F[Option[S]])
      : Tables[F, I, Q, D, S] =
      new DefaultTables[F, I, Q, D, S](
        freshId,
        tableStore,
        evaluator,
        manager,
        lookupTableData,
        lookupTableSchema)
}
