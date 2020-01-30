/*
 * Copyright 2014–2020 SlamData Inc.
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
import quasar.api.table.{TableError, TableRef, Tables}
import quasar.impl.storage.IndexedStore

import cats.effect.Effect

import fs2.Stream

import scalaz.{\/, Equal}
import scalaz.std.option
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._

import shims.{eqToScalaz, monadToScalaz}

final class DefaultTables[F[_]: Effect, I: Equal, Q](
    freshId: F[I],
    tableStore: IndexedStore[F, I, TableRef[Q]])
    extends Tables[F, I, Q] {

  import TableError.{
    CreateError,
    ExistenceError,
    ModificationError,
    NameConflict,
    TableNotFound
  }

  val allTables: Stream[F, (I, TableRef[Q])] =
    tableStore.entries

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
}

object DefaultTables {
  def apply[F[_]: Effect, I: Equal, Q](
      freshId: F[I],
      tableStore: IndexedStore[F, I, TableRef[Q]])
      : Tables[F, I, Q] =
    new DefaultTables[F, I, Q](freshId, tableStore)
}
