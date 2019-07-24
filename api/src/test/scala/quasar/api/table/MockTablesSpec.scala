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

package quasar.api.table

import slamdata.Predef._
import quasar.contrib.cats.stateT._
import quasar.contrib.scalaz.MonadState_
import quasar.contrib.std.uuid._
import java.util.UUID

import cats.data.StateT
import cats.effect.{IO, Sync}
import scalaz.{~>, Id, IMap, \/-}, Id.Id
import scalaz.std.string._
import shims._
import MockTablesSpec.MockM

final class MockTablesSpec extends TablesSpec[MockM, UUID, String, String, String] {

  val tables: Tables[MockM, UUID, String, String, String] =
    MockTables[MockM]

  val columns1: List[TableColumn] =
    List(TableColumn("foo1", ColumnType.Number),
      TableColumn("foo2", ColumnType.String),
      TableColumn("foo3", ColumnType.Boolean),
      TableColumn("foo4", ColumnType.Null),
      TableColumn("foo5", ColumnType.OffsetDateTime))

  val columns2: List[TableColumn] =
    List(TableColumn("bar1", ColumnType.Number),
      TableColumn("bar2", ColumnType.String),
      TableColumn("bar3", ColumnType.Boolean),
      TableColumn("bar4", ColumnType.Null),
      TableColumn("bar5", ColumnType.OffsetDateTime))

  val columns3: List[TableColumn] =
    List(TableColumn("baz1", ColumnType.Number),
      TableColumn("baz2", ColumnType.String),
      TableColumn("baz3", ColumnType.Boolean),
      TableColumn("baz4", ColumnType.Null),
      TableColumn("baz5", ColumnType.OffsetDateTime))

  val table1: TableRef[String] = TableRef(TableName("table1"), "select * from table1", columns1)
  val table2: TableRef[String] = TableRef(TableName("table2"), "select * from table2", columns2)

  val preparation1: String = table1.query
  val preparation2: String = table2.query

  val uniqueId: UUID = UUID.randomUUID

  def run: MockM ~> Id.Id =
    λ[MockM ~> Id](
      _.runA(IMap.empty).unsafeRunSync)

  def init(table: MockTables.MockTable): MockM[UUID] =
    for {
      tableId <- Sync[MockM].delay(UUID.randomUUID)
      _ <- MockM.modify(_.insert(tableId, table))
    } yield tableId

  def before: Unit = ()

  "preparations" >> {
    "error on request to prepare an already preparing table" >>* {
      for {
        id <- init(MockTables.MockTable(
          TableRef(TableName("foo"), "bar", columns1),
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.Preparing)))
        result <- tables.prepareTable(id)
      } yield {
        result must beAbnormal(TableError.PreparationInProgress(id))
      }
    }

    "cancel an ongoing preparation" >>* {
      for {
        id1 <- init(MockTables.MockTable(
          TableRef(TableName("foo1"), "bar1", columns1),
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.Preparing)))

        id2 <- init(MockTables.MockTable(
          TableRef(TableName("foo2"), "bar2", columns2),
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.Preparing)))

        cancel1 <- tables.cancelPreparation(id1)

        res1 <- tables.preparationStatus(id1)
        res2 <- tables.preparationStatus(id2)
      } yield {
        cancel1 must beNormal

        res1 must be_\/-(
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.NotPreparing))

        res2 must be_\/-(
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.Preparing))
      }
    }

    "error a request to cancel a not ongoing preparation" >>* {
      for {
        id1 <- init(MockTables.MockTable(
          TableRef(TableName("foo1"), "bar1", columns1),
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.NotPreparing)))

        cancel <- tables.cancelPreparation(id1)
        status <- tables.preparationStatus(id1)
      } yield {
        cancel must beAbnormal(TableError.PreparationNotInProgress(id1))
        status must be_\/-(PreparationStatus(PreparedStatus.Prepared, OngoingStatus.NotPreparing))
      }
    }

    "cancel all ongoing preparations" >>* {
      for {
        id1 <- init(MockTables.MockTable(
          TableRef(TableName("foo1"), "bar1", columns1),
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.NotPreparing)))

        id2 <- init(MockTables.MockTable(
          TableRef(TableName("foo2"), "bar2", columns2),
          PreparationStatus(PreparedStatus.Unprepared, OngoingStatus.Preparing)))

        id3 <- init(MockTables.MockTable(
          TableRef(TableName("foo3"), "bar3", columns3),
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.Preparing)))

        _ <- tables.cancelAllPreparations

        status1 <- tables.preparationStatus(id1)
        status2 <- tables.preparationStatus(id2)
        status3 <- tables.preparationStatus(id3)
      } yield {
        status1 must be_\/-(PreparationStatus(PreparedStatus.Prepared, OngoingStatus.NotPreparing))
        status2 must be_\/-(PreparationStatus(PreparedStatus.Unprepared, OngoingStatus.NotPreparing))
        status3 must be_\/-(PreparationStatus(PreparedStatus.Prepared, OngoingStatus.NotPreparing))
      }
    }
  }

  "schema" >> {
    "successfully request schema for prepared table" >>* {
      for {
        id <- init(MockTables.MockTable(
          TableRef(TableName("foo"), "bar", columns1),
          PreparationStatus(PreparedStatus.Prepared, OngoingStatus.Preparing)))
        result <- tables.preparedSchema(id)
      } yield {
        result must_== \/-(PreparationResult.Available(id, id.toString))
      }
    }
  }
}

object MockTablesSpec {
  type Store = IMap[UUID, MockTables.MockTable]

  type MockM[A] = StateT[IO, Store, A]
  val MockM = MonadState_[MockM, Store]
}
