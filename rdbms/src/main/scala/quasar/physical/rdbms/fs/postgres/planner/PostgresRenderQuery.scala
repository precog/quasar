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
import quasar.common.SortDir.{Ascending, Descending}
import quasar.Data
import quasar.DataCodec
import quasar.DataCodec.Precise.TimeKey
import quasar.physical.rdbms.model._
import quasar.physical.rdbms.fs.postgres._
import quasar.physical.rdbms.planner.RenderQuery
import quasar.physical.rdbms.planner.sql._
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.physical.rdbms.planner.sql.SqlExpr.Case._
import quasar.Planner.InternalError
import quasar.Planner.{NonRepresentableData, PlannerError}

import matryoshka._
import matryoshka.implicits._

import scalaz._
import Scalaz._

object PostgresRenderQuery extends RenderQuery {
  import SqlExpr._

  implicit val codec: DataCodec = DataCodec.Precise

  def asString[T[_[_]]: BirecursiveT](a: T[SqlExpr]): PlannerError \/ String = {
    val q = a.cataM(alg)
    q ∘ (s => s"select row_to_json(row) from ($s) as row")
  }

  def alias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" as ${i.v}"))

  def rowAlias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" ${i.v}"))

  def buildJson(str: String): String =
    s"json_build_object($str)#>>'{}'"

  val alg: AlgebraM[PlannerError \/ ?, SqlExpr, String] = {
    case Unreferenced() => InternalError("Unexpected Unreferenced!", none).left
    case Null() => "null".right
    case SqlExpr.Id(v) =>
      s"""$v""".right
    case Table(v) =>
      v.right
    case AllCols() =>
      s"*".right
    case Refs(srcs) =>
      srcs match {
        case Vector(key, value) =>
          val valueStripped = value.stripPrefix("'").stripSuffix("'")
          s"""$key.$valueStripped""".right
        case key +: mid :+ last =>
          val firstValStripped = ~mid.headOption.map(_.stripPrefix("'").stripSuffix("'"))
          val midTail = mid.drop(1)
          val midStr = if (midTail.nonEmpty)
            s"->${midTail.map(e => s"$e").intercalate("->")}"
          else
            ""
          s"""$key.$firstValStripped$midStr->$last""".right
        case _ => InternalError.fromMsg(s"Cannot process Refs($srcs)").left
      }
    case Obj(m) =>
      buildJson(m.map {
        case (k, v) => s"'$k', $v"
      }.mkString(",")).right
    case RegexMatches(str, pattern) =>
      s"($str ~ '$pattern')".right
    case IsNotNull(expr) =>
      s"($expr notnull)".right
    case ToJson(v) =>
      s"to_json($v)".right
    case IfNull(a) =>
      s"coalesce(${a.intercalate(", ")})".right
    case ExprWithAlias(expr: String, alias: String) =>
      (if (expr === alias) s"$expr" else s"""$expr as "$alias"""").right
    case ExprPair(expr1, expr2) =>
      s"$expr1, $expr2".right
    case ConcatStr(str1, str2)  =>
      s"$str1 || $str2".right
    case Time(expr) =>
      buildJson(s"""{ "$TimeKey": $expr }""").right
    case NumericOp(sym, left, right) => s"(($left)::text::numeric $sym ($right)::text::numeric)".right
    case Mod(a1, a2) => s"mod(($a1)::text::numeric, ($a2)::text::numeric)".right
    case Pow(a1, a2) => s"power(($a1)::text::numeric, ($a2)::text::numeric)".right
    case And(a1, a2) =>
      s"($a1 and $a2)".right
    case Or(a1, a2) =>
      s"($a1 or $a2)".right
    case Neg(str) => s"(-$str)".right
    case WithIds(str)    => s"(row_number() over(), $str)".right
    case RowIds()        => "row_number() over()".right
    case Limit(from, count) => s"$from LIMIT $count".right
    case Select(selection, from, filterOpt, order) =>
      val selectionStr = selection.v ⊹ alias(selection.alias)
      val filter = ~(filterOpt ∘ (f => s" where ${f.v}"))
      val orderStr = order.map { o =>
        val dirStr = o.sortDir match {
          case Ascending => "asc"
          case Descending => "desc"
        }
        s"${o.v} $dirStr"
      }.mkString(", ")

      val orderByStr = if (order.nonEmpty)
        s" order by $orderStr"
      else
        ""

      val fromExpr = s" from ${from.v} ${from.alias.v}"
      s"(select $selectionStr$fromExpr$filter$orderByStr)".right
    case SelectRow(selection, from, order) =>
      val fromExpr = s" from ${from.v}"

      val orderStr = order.map { o =>
        val dirStr = o.sortDir match {
          case Ascending => "asc"
          case Descending => "desc"
        }
        s"${o.v} $dirStr"
      }.mkString(", ")

      val orderByStr = if (order.nonEmpty)
        s" order by $orderStr"
      else
       ""

      s"(select ${selection.v}$fromExpr$orderByStr)".right
    case Constant(Data.Str(v)) =>
      val text = v.flatMap { case ''' => "''"; case iv => iv.toString }.self
      s"'$text'".right
    case Constant(v) =>
      DataCodec.render(v) \/> NonRepresentableData(v)
    case Case(wt, e) =>
      val wts = wt ∘ { case WhenThen(w, t) => s"when $w then $t" }
      s"(case ${wts.intercalate(" ")} else ${e.v} end)".right
    case Coercion(t, e) => s"($e)::${t.mapToStringName}".right
    case UnaryFunction(fType, e) =>
      val fName = fType match {
        case StrLower => "lower"
        case StrUpper => "upper"
      }
      s"$fName($e)".right
    case BinaryFunction(fType, a1, a2) =>
      val fName = fType match {
        case SplitStr => "regexp_split_to_array"
      }
      s"$fName($a1, $a2)".right
    case TernaryFunction(fType, a1, a2, a3) => (fType match {
      case Search => s"(case when $a3 then $a1 ~* $a2 else $a1 ~ $a2 end)"
      case Substring => s"substring($a1 from ($a2+1) for $a3)"
    }).right
  }
}
