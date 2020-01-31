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

import slamdata.Predef._

import quasar.{ConditionMatchers, Qspec}

import cats.effect.Sync
import org.specs2.execute.AsResult
import org.specs2.specification.BeforeEach
import org.specs2.specification.core.Fragment
import scalaz.{~>, \/, \/-, -\/, Equal, Id, Show}, Id.Id
import scalaz.std.list._
import scalaz.syntax.monad._

// NB: this one proved sufficiently annoying to make specific that I left it alone
import shims._

abstract class TablesSpec[F[_]: Sync, I: Equal: Show, Q: Equal: Show]
    extends Qspec
    with ConditionMatchers
    with BeforeEach {

  def tables: Tables[F, I, Q]

  // `table1` and `table2` must have distinct names
  val table1: TableRef[Q]
  val table2: TableRef[Q]

  val uniqueId: I  // generate a unique value of type `I`

  def run: F ~> Id

  def before: Unit

  "test data is compliant" >> {
    table1.name must_!= table2.name
  }

  "tables" >> {
    "error when accessing a nonexistent table" >>* {
      for {
        id <- uniqueId.point[F]
        result <- tables.table(id)
      } yield {
        result must beLike {
          case -\/(TableError.TableNotFound(i)) => i must_= id
        }
      }
    }

    "error when creating a table with a name conflict" >>* {
      for {
        errorOrId <- tables.createTable(table1)
        _ <- isSuccess(errorOrId)
        errorOrIdCopy <- tables.createTable(table2.copy(name=table1.name))
      } yield {
        errorOrIdCopy must beLike {
          case -\/(TableError.NameConflict(n)) => n must_= table1.name
        }
      }
    }

    "error when replacing a nonexistent table" >>* {
      for {
        id <- uniqueId.point[F]
        result <- tables.replaceTable(id, table1)
      } yield {
        result must beAbnormal {
          TableError.TableNotFound(id)
        }
      }
    }

    "error when replacing a table with a name conflict" >>* {
      for {
        errorOrId1 <- tables.createTable(table1)
        id1 <- isSuccess(errorOrId1)
        errorOrId2 <- tables.createTable(table2)
        _ <- isSuccess(errorOrId2)
        table11 = table1.copy(name = table2.name)
        cond <- tables.replaceTable(id1, table11)
      } yield {
        cond must beAbnormal(TableError.NameConflict(table2.name): TableError.ModificationError[I])
      }
    }

    "succesfully access a created table" >>* {
      for {
        errorOrId <- tables.createTable(table1)
        id <- isSuccess(errorOrId)
        result <- tables.table(id)
      } yield {
        result must beLike {
          case \/-(t) => t must_= table1
        }
      }
    }

    "succesfully return all tables" >>* {
      for {
        errorOrId1 <- tables.createTable(table1)
        errorOrId2 <- tables.createTable(table2)
        result <- tables.allTables.compile.toList
      } yield {
        (result.map(_._2) must_= List(table1, table2)) or
          (result.map(_._2) must_= List(table2, table1))
      }
    }

    "succesfully replace a table" >>* {
      for {
        errorOrId <- tables.createTable(table1)
        id <- isSuccess(errorOrId)
        originalResult <- tables.table(id)
        _ <- tables.replaceTable(id, table2)
        replacedResult <- tables.table(id)
      } yield {
        originalResult must beLike {
          case \/-(t) => t must_= table1
        }
        replacedResult must beLike {
          case \/-(t) => t must_= table2
        }
      }
    }

    "succesfully replace a table without changing name" >>* {
      for {
        errorOrId <- tables.createTable(table1)
        id <- isSuccess(errorOrId)
        originalResult <- tables.table(id)
        newCols = TableColumn("quux", ColumnType.Number) :: table1.columns
        newTable = table1.copy(columns = newCols)
        _ <- tables.replaceTable(id, newTable)
        replacedResult <- tables.table(id)
      } yield {
        originalResult must beLike {
          case \/-(t) => t must_= table1
        }
        replacedResult must beLike {
          case \/-(t) => t must_= newTable
        }
      }
    }
  }

  ////

  implicit class RunExample(s: String) {
    def >>*[A: AsResult](fa: => F[A]): Fragment =
      s >> run(fa)
  }

  private def isSuccess[A, B](either: A \/ B): F[B] =
    Sync[F].delay(either.toOption.get)
}
