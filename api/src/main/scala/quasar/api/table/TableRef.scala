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

import slamdata.Predef._

import scalaz.{Cord, Equal, Order, Show}
import scalaz.std.list._
import scalaz.std.tuple._
import scalaz.std.string._
import scalaz.syntax.show._

final case class TableName(name: String)

object TableName {
  implicit val orderTableName: Order[TableName] = Order.orderBy(_.name)
  implicit val showTableName: Show[TableName] = Show.showFromToString
}

final case class TableColumn(name: String, tpe: ColumnType.Scalar)

object TableColumn {
  implicit val equalTableColumn: Equal[TableColumn] =
    Equal.equalBy(c => (c.name, c.tpe))

  implicit val showTableColumn: Show[TableColumn] =
    Show show { tc =>
      Cord("TableColumn(") ++ tc.name ++ Cord(", ") ++ tc.tpe.show ++ Cord(")")
    }
}

final case class TableRef[Q](name: TableName, query: Q, columns: List[TableColumn])

object TableRef {
  implicit def equalTableRef[Q: Equal]: Equal[TableRef[Q]] =
    Equal.equalBy(t => (t.name, t.query, t.columns))

  implicit def showTableRef[Q: Show]: Show[TableRef[Q]] =
    Show show { t =>
      Cord("TableRef(") ++ t.name.show ++ Cord(", ") ++ t.query.show ++ Cord(", ") ++ t.columns.show ++ Cord(")")
    }
}
