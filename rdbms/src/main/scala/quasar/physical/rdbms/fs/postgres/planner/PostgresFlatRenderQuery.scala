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

package quasar.physical.rdbms.fs.postgres.planner

import slamdata.Predef._
import quasar.{DataCodec, Data => QData}
import quasar.physical.rdbms.planner.sql.{RenderQuery, SqlExpr}
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.Planner.{NonRepresentableData, PlannerError}

import matryoshka._
import matryoshka.implicits._
import scalaz._
import Scalaz._

object PostgresFlatRenderQuery extends RenderQuery {
  import SqlExpr._

  implicit val codec: DataCodec = DataCodec.Precise

  def asString[T[_[_]]: BirecursiveT](a: T[SqlExpr]): PlannerError \/ String = {
    val q = a.cataM(alg)

    a.project match {
      case s: Select[T[SqlExpr]] => q ∘ (s => s"$s")
      case _                     => q ∘ ("" ⊹ _)
    }
  }

  val alg: AlgebraM[PlannerError \/ ?, SqlExpr, String] = {
    case Data(QData.Str(v)) =>
      v.flatMap { case ''' => "''"; case iv => iv.toString }.self.right
    case Data(v) =>
      DataCodec.render(v) \/> NonRepresentableData(v)
    case Null() =>
      s"null".right
    case Length(v) =>
      (s"(case when pg_typeof($v) in (1005, 1007, 1009, 1028, 1021, 1263, 2211, 2287, 2277, 2276) " +
        s"then cardinality($v::text[]) when pg_typeof($v) in (1043, 25) then length($v::text) else 0 end)").right
    case SqlExpr.Id(v) =>
      s"""$v""".right
    case ExprWithAlias(expr, alias) =>
      (if (expr === alias.v) s"$expr" else s"""$expr as "${alias.v}"""").right
    case ExprPair(expr1, expr2) =>
      s"$expr1, $expr2".right
    case Table(v) =>
      v.right
    case AllCols() =>
      "*".right
    case WithIds(str)    => str.right
    case SomeCols(names) => names.mkString(",").right
    case RowIds()        => "row_number() over()".right
    case Select(selection, from, filterOpt) =>
      def alias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" as ${i.v}"))
      val selectionStr = selection.v ⊹ alias(selection.alias)
      val filter = ~(filterOpt ∘ (f => s"where ${f.v}"))
      val fromExpr = s" from ${from.v}" ⊹ alias(from.alias)
      s"(select $selectionStr$fromExpr $filter)".right
  }
}
