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

package quasar.api.table

import slamdata.Predef.{None, Some, String}

import quasar.Condition
import quasar.contrib.scalaz.MonadState_
import quasar.contrib.std.uuid._

import java.util.UUID

import fs2.Stream
import scalaz.{\/, IMap, Monad}
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

import shims._

final class MockTables[F[_]: Monad: MockTables.TablesMockState]
    extends Tables[F, UUID, String] {

  import TableError._

  val store = MonadState_[F, IMap[UUID, TableRef[String]]]

  def allTables: Stream[F, (UUID, TableRef[String])] =
    Stream.force(store.get.map(s => Stream.emits(s.toList).covary[F]))

  def table(tableId: UUID): F[ExistenceError[UUID] \/ TableRef[String]] =
    store.gets(_.lookup(tableId)
      .toRightDisjunction(TableNotFound(tableId)))

  def createTable(table: TableRef[String]): F[CreateError[UUID] \/ UUID] =
    store.get map { store =>
        (store, store.values.map(_.name).contains(table.name))
    } flatMap { case (s, exists) =>
      if (exists)
        Monad[F].point(NameConflict(table.name).left[UUID])
      else
        for {
          id <- UUID.randomUUID.point[F]
          back <- store.put(s.insert(id, table))
        } yield id.right[CreateError[UUID]]
    }

  def replaceTable(tableId: UUID, table: TableRef[String])
      : F[Condition[ModificationError[UUID]]] =
    store.gets(s => (s.lookup(tableId), s.filter(_.name === table.name))) flatMap {
      case (Some(mt), sameName) =>
        if (sameName.keys.forall(_ === tableId))
          store.modify(_.insert(tableId, table))
            .as(Condition.normal[ModificationError[UUID]]())
        else
          Condition.abnormal[ModificationError[UUID]](NameConflict(table.name)).point[F]

      case (None, _) =>
        Condition.abnormal[ModificationError[UUID]](TableNotFound(tableId)).point[F]
    }
}

object MockTables {
  type TablesMockState[F[_]] = MonadState_[F, IMap[UUID, TableRef[String]]]

  def apply[F[_]: Monad: TablesMockState[?[_]]] = new MockTables[F]
}
