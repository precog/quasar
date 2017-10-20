/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.rdbms.planner

import slamdata.Predef._
import quasar.contrib.pathy.AFile
import quasar.qscript._
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.physical.rdbms.planner.sql.SqlExpr._
import quasar.physical.rdbms.planner.sql.SqlExpr

import matryoshka._
import matryoshka.implicits._
import scalaz._
import Scalaz._

class ShiftedReadPlanner[T[_[_]]: CorecursiveT, F[_]: Applicative] extends Planner[T, F, Const[ShiftedRead[AFile], ?]] {

  type R = T[SqlExpr]

  def plan: AlgebraM[F, Const[ShiftedRead[AFile], ?], R] = {
    case Const(semantics) =>
      val from: From[R] = From(Table[R](TablePath.create(semantics.path).shows).embed, alias = None)
      val filter = None
      val fields: T[SqlExpr] = semantics.idStatus match {
        case IdOnly => RowIds[R]().embed
        case ExcludeId => AllCols[R]().embed
        case IncludeId => WithIds[R](AllCols[R]().embed).embed
      }
      Select(Selection[R](fields, alias = none), from, filter).embed.point[F]
  }

}
