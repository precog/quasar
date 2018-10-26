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

import quasar.api.QueryEvaluator
import quasar.api.table.{TableName, TableRef, Tables, TablesSpec}
import quasar.contrib.scalaz.MonadState_
import quasar.contrib.std.uuid._
import quasar.impl.storage.{IndexedStore, PureIndexedStore}

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.{IO, Sync}
import fs2.Stream
import scalaz.{~>, IMap, Id}
import scalaz.std.string._
import shims._

final class DefaultTablesSpec extends TablesSpec[IO, UUID, String, String, String] {
  import DefaultTablesSpec._

  sequential

  implicit val cs = IO.contextShift(global)

  val freshId: IO[UUID] = IO(UUID.randomUUID)

  val tableStore: IndexedStore[IO, UUID, TableRef[String]] =
    PureIndexedStore[IO, UUID, TableRef[String]]

  val pTableStore: IndexedStore[IO, UUID, String] =
    PureIndexedStore[IO, UUID, String]

  val runToStore: (UUID, String) => IO[Stream[IO, Unit]] =
    pTableStore.insert(_, _).map(Stream(_))

  val lookup: UUID => IO[Option[String]] =
    pTableStore.lookup(_)

  val lookupSchema: UUID => IO[Option[String]] =
    uuid => {
      for {
        l <- pTableStore.lookup(uuid)
      } yield l match {
        case Some(s) => Some(s)
        case _ => None
      }
    }

  val evaluator: QueryEvaluator[IO, String, String] =
    new QueryEvaluator[IO, String, String] {
      def evaluate(query: String): IO[String] = IO(query)
    }

  val manager: List[PreparationsManager[IO, UUID, String, String]] =
    PreparationsManager[IO, UUID, String, String](evaluator)(runToStore)
      .compile.toList.unsafeRunSync

  val tables: Tables[IO, UUID, String, String, String] =
    DefaultTables[IO, UUID, String, String, String](freshId, tableStore, evaluator, manager(0), lookup, lookupSchema)

  val table1: TableRef[String] = TableRef(TableName("table1"), "select * from table1")
  val table2: TableRef[String] = TableRef(TableName("table2"), "select * from table2")

  val preparation1: String = table1.query
  val preparation2: String = table2.query

  val uniqueId: UUID = UUID.randomUUID

  def run: IO ~> Id.Id = λ[IO ~> Id.Id](_.unsafeRunSync)

  def before: Unit = {
    msmaptref.put(IMap()).unsafeRunSync()
    msmapstring.put(IMap()).unsafeRunSync()
  }
}

object DefaultTablesSpec {

  def mutableState[F[_]: Sync, S](init: S): MonadState_[F, S] = new MonadState_[F, S] {
    private var s: S = init
  
    def get: F[S] = Sync[F].delay(s)
    def put(s0: S): F[Unit] = Sync[F].delay(s = s0)
  }

  implicit val msmaptref: MonadState_[IO, IMap[UUID, TableRef[String]]] =
    mutableState[IO, IMap[UUID, TableRef[String]]](IMap())
  
  implicit val msmapstring: MonadState_[IO, IMap[UUID, String]] =
    mutableState[IO, IMap[UUID, String]](IMap())
}
