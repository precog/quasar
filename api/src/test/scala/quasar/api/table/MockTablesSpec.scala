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
import cats.effect.IO
import scalaz.{~>, Id, IMap}, Id.Id
import scalaz.std.string._
import shims.monadToScalaz
import MockTablesSpec.MockM

final class MockTablesSpec extends TablesSpec[MockM, UUID, String] {

  val tables: Tables[MockM, UUID, String] =
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

  val uniqueId: UUID = UUID.randomUUID

  def run: MockM ~> Id.Id =
    λ[MockM ~> Id](_.runA(IMap.empty).unsafeRunSync)

  def before: Unit = ()
}

object MockTablesSpec {
  type Store = IMap[UUID, TableRef[String]]

  type MockM[A] = StateT[IO, Store, A]
  val MockM = MonadState_[MockM, Store]
}
