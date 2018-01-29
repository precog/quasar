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

package quasar.physical.rdbms.fs.postgres.planner

import slamdata.Predef._

import quasar.common.JoinType._
import quasar.common.SortDir.{Ascending, Descending}
import quasar.{Data, DataCodec}
import quasar.fp.ski._
import quasar.DataCodec.Precise.TimeKey
import quasar.physical.rdbms.model._
import quasar.physical.rdbms.fs.postgres._
import quasar.physical.rdbms.planner.RenderQuery
import quasar.physical.rdbms.planner.sql._
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.physical.rdbms.planner.sql.SqlExpr.Case._
import quasar.Planner.InternalError
import quasar.Planner.{NonRepresentableData, PlannerError}
import quasar.physical.rdbms.planner.sql.Indirections._

import matryoshka._
import matryoshka.implicits._
import scalaz._
import Scalaz._

object PostgresRenderQuery extends RenderQuery {
  import SqlExpr._

  implicit val codec: DataCodec = DataCodec.Precise

  def asString[T[_[_]]: BirecursiveT](a: T[SqlExpr]): PlannerError \/ String = {

    a.paraM(galg) ∘ (s => s"select row_to_json(row) from ($s) as row")
  }

  def alias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" as ${i.v}"))

  def rowAlias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" ${i.v}"))

  def buildJson(str: String): String =
    s"json_build_object($str)#>>'{}'"

  def text[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): String = {
    // The -> operator returns jsonb type, while ->> returns text. We need to choose one
    // depending on context, hence this function called in certain cases (see functionsNested.test)
    val (expr, str) = pair
    val toReplace = "->"
    val replacement = "->>"
    expr.project match {
      case Refs(elems, _) =>
        val pos = str.lastIndexOf(toReplace)
        if (pos > -1 && !str.contains(replacement))
          s"${str.substring(0, pos)}$replacement${str.substring(pos + toReplace.length, str.length)}"
        else
          str
      case _ =>
        str
    }
  }

  def num[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): String = {
    val (expr, str) = pair
    expr.project match {
      case Constant(Data.Int(_)) =>
        str
      case _ =>
        s"(${text(pair)})::numeric"
    }
  }

  def bool[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): String = {
      val (expr, str) = pair
      expr.project match {
        case Constant(Data.Bool(_)) =>
          str
        case _ =>
          s"(${text(pair)})::boolean"
      }
    }

  object TextExpr {
    def unapply[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): Option[String] =
      s"(${text(pair)})::text".some
  }

  object NumExpr {
    def unapply[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): Option[String] =
      num(pair).some
  }

  object BoolExpr {
    def unapply[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): Option[String] =
      bool(pair).some
  }

  private def postgresArray(jsonArrayRepr: String) = s"array_to_json(ARRAY$jsonArrayRepr)"

  final case class Acc(s: String, m: Indirections.Indirection)

  def galg[T[_[_]]: BirecursiveT]: GAlgebraM[(T[SqlExpr], ?), PlannerError \/ ?, SqlExpr, String] = {
    case Unreferenced() =>
    InternalError("Unexpected Unreferenced!", none).left
    case Null() => "null".right
    case SqlExpr.Id(v, _) =>
      s"""$v""".right
    case Table(v) =>
      v.right
    case AllCols() =>
      s"*".right
    case Refs(srcs, m) =>
      srcs.unzip(ι) match {
        case (_, firstStr +: tail) =>
          tail.foldLeft(Acc(firstStr, m)) {
            case (acc@Acc(accStr, Branch(mFunc, _)), nextStr) =>
              val nextStrStripped = nextStr.stripPrefix("'").stripSuffix("'")
              val (metaType, nextMeta) = mFunc(nextStrStripped)
              val str = metaType match {
                case Field =>
                  s"""($accStr."$nextStrStripped")"""
                case InnerField => s"$accStr->$nextStr"
              }
              Acc(str, nextMeta)
          }.s.right
        case _ =>
          InternalError("Refs with empty vector!", none).left // TODO refs should carry a Nel
      }
    case Obj(m) =>
      buildJson(m.map {
        case ((_, k), (_, v)) => s"'$k', $v"
      }.mkString(",")).right
    case RegexMatches(TextExpr(e), TextExpr(pattern), caseInsensitive: Boolean) =>
      val op = if (caseInsensitive) "~*" else "~"
      s"($e $op $pattern)".right
    case IsNotNull((_, expr)) =>
      s"($expr notnull)".right
    case IfNull(exprs) =>
      s"coalesce(${exprs.map(e => text(e)).intercalate(", ")})".right
    case ExprWithAlias((_, expr), alias) =>
        s"""$expr as "$alias"""".right
    case ExprPair((_, s1), (_, s2), _) =>
      s"$s1, $s2".right
    case ConcatStr(TextExpr(e1), TextExpr(e2))  =>
      s"$e1 || $e2".right
    case Avg((_, e)) =>
      s"avg($e)".right
    case Count((_, e)) =>
      s"count($e)".right
    case Max((_, e)) =>
      s"max($e)".right
    case Min((_, e)) =>
      s"min($e)".right
    case Sum((_, e)) =>
      s"sum($e)".right
    case Distinct((_, e)) =>
      s"distinct $e".right
    case Time((_, expr)) =>
      buildJson(s"""{ "$TimeKey": $expr }""").right
    case NumericOp(sym, NumExpr(left), NumExpr(right)) =>
      s"($left $sym $right)".right
    case Mod(NumExpr(a1), NumExpr(a2)) =>
      s"mod($a1, $a2)".right
    case Pow(NumExpr(a1), NumExpr(a2)) =>
      s"power($a1, $a2)".right
    case And(BoolExpr(a1), BoolExpr(a2)) =>
      s"($a1 and $a2)".right
    case Or(BoolExpr(a1), BoolExpr(a2)) =>
      s"($a1 or $a2)".right
    case Neg(NumExpr(e)) =>
      s"(-$e)".right
    case Eq(TextExpr(a1), TextExpr(a2)) =>
      s"($a1 = $a2)".right
    case Neq(TextExpr(a1), TextExpr(a2)) =>
      s"($a1 != $a2)".right
    case Lt(NumExpr(a1), NumExpr(a2)) =>
      s"($a1 < $a2)".right
    case Lte(NumExpr(a1), NumExpr(a2)) =>
      s"($a1 <= $a2)".right
    case Gt(NumExpr(a1), NumExpr(a2)) =>
      s"($a1 > $a2)".right
    case Gte(NumExpr(a1), NumExpr(a2)) =>
      s"($a1 >= $a2)".right
    case WithIds((_, str))    => s"(row_number() over(), $str)".right
    case RowIds()        => "row_number() over()".right
    case Offset((_, from), NumExpr(count)) => s"($from OFFSET $count)".right
    case Limit((_, from), NumExpr(count)) => s"($from LIMIT $count)".right
    case Select(selection, from, joinOpt, filterOpt, groupBy, order) =>
      val filter = ~(filterOpt ∘ (f => s" where ${f.v._2}"))
      val join = ~(joinOpt ∘ (j => {

        val joinKeyStr = j.keys.map {
          case (TextExpr(lK), TextExpr(rK)) => s"$lK = $rK"
        }.intercalate(" and ")

        val joinKeyExpr = if (j.keys.nonEmpty) s"on $joinKeyStr" else ""
        val joinTypeStr = if (j.keys.nonEmpty) {
          j.jType match {
            case Inner => "inner"
            case FullOuter => "full outer"
            case LeftOuter => "left outer"
            case RightOuter => "right outer"
          }
        } else "cross"
        s" $joinTypeStr join ${j.v._2} ${j.alias.v} $joinKeyExpr"
      }))
      val orderStr = order.map { o =>
        val dirStr = o.sortDir match {
          case Ascending => "asc"
          case Descending => "desc"
        }
        s"${o.v._2} $dirStr"
      }.mkString(", ")

      val orderByStr = if (order.nonEmpty) s" order by $orderStr" else ""

      val groupByStr = ~(groupBy.flatMap{
        case GroupBy(Nil) => none
        case GroupBy(v) => v.map(_._2).intercalate(", ").some
      }.map(v => s" GROUP BY $v"))


      val fromExpr = s" from ${from.v._2} ${from.alias.v}"
      s"(select ${selection.v._2}$fromExpr$join$filter$groupByStr$orderByStr)".right
    case Union((_, left), (_, right)) => s"($left UNION $right)".right
    case Constant(a @ Data.Arr(_)) =>  s"${dataFormatter("", a)}::jsonb".right
    case Constant(Data.Str(v)) =>
      val text = v.flatMap { case '\'' => "''"; case iv => iv.toString }.self
      s"'$text'".right
    case Constant(v) =>
      DataCodec.render(v) \/> NonRepresentableData(v)
    case Case(wt, e) =>
      val wts = wt ∘ { case WhenThen(TextExpr(w), TextExpr(t)) => s"when ($w)::boolean then $t" }
      s"(case ${wts.intercalate(" ")} else ${text(e.v)} end)".right
    case Coercion(t, (_, e)) => s"($e)::${t.mapToStringName}".right
    case ToArray(TextExpr(v)) => postgresArray(s"[$v]").right
    case UnaryFunction(fType, TextExpr(e)) =>
      val fName = fType match {
        case StrLower => "lower"
        case StrUpper => "upper"
        case ToJson => "row_to_json"
      }
      s"$fName($e)".right
    case BinaryFunction(fType, (_, a1), (a2Src, a2)) => (fType match {
        case StrSplit => s"regexp_split_to_array($a1, ${text((a2Src, a2))})"
        case ArrayConcat => s"(to_jsonb($a1) || to_jsonb($a2))"
        case Contains => s"($a1 IN (SELECT jsonb_array_elements_text(to_jsonb($a2))))"
      }).right
    case TernaryFunction(fType, a1, a2, a3) => (fType match {
      case Search => s"(case when ${bool(a3)} then ${text(a1)} ~* ${text(a2)} else ${text(a1)} ~ ${text(a2)} end)"
      case Substring => s"substring(${text(a1)} from ((${text(a2)})::integer + 1) for (${text(a3)})::integer)"
    }).right

    case ArrayUnwind(toUnwind) => s"jsonb_array_elements_text(${text(toUnwind)})".right
  }
}
