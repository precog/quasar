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
import quasar.physical.rdbms.model._
import quasar.physical.rdbms.fs.postgres._
import quasar.physical.rdbms.fs.postgres.mapping._
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

  def rowToJson(q: String): String =
    s"select row_to_json(row) from ($q) as row"

  def asString[T[_[_]]: BirecursiveT](a: T[SqlExpr]): PlannerError \/ String = {

    type SqlTransform = T[SqlExpr] => T[SqlExpr]

    // Postgres represents record types with a special data structure, so
    // an additional wrapping with coercion is required to get the string name instead of a numeric representation.
    val stringifyTypeOf: SqlTransform = s => s.project match {
      case TypeOf(_) => Coercion[T[SqlExpr]](StringCol, s).embed
      case _ => s
    }

    // This is a rudimentary solution to selects where selection yields a single field without
    // any alias, so that queries like "select field from db" return just "[val]" instead
    // of {"field": "val"}. A proper, generic solution should be incorporated.
    def markSingleFieldResult(s: T[SqlExpr]): T[SqlExpr] = s.project match {
      case select@Select(Selection(sel, _, _), _, _, _, _, _) =>
        sel.project match {
          case NumericOp(_, _, _) | Refs(_, _) =>
            val newSelection = ExprWithAlias(sel, SingleFieldKey).embed
            select.copy(selection = select.selection.copy(v = newSelection)).embed
          case _=>
            s
        }
      case _ =>
        s
    }

    val selectStr = markSingleFieldResult(a)
      .transCataT(stringifyTypeOf)
      .paraM(galg)

    // Wrap the whole SQL query in a "select row_to_json..." unless it's a query which
    // already uses this function. We don't want to double the row_to_json call.
    a.project match {
      case Select(Selection(sel, _, _), _, _, _, _, _) =>
        sel.project match {
          case DeleteKey(_, _) =>
            selectStr
          case _ =>
              selectStr.map(rowToJson)
        }
      case _ =>
        selectStr.map(rowToJson)
    }
  }

  def alias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" as ${i.v}"))

  def rowAlias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" ${i.v}"))

  def buildJsonSingleton(str: String): String =
    s"json_build_object('$str')#>>'{}'"

  def buildJson(str: (String, String)*): String =
    s"""json_build_object(${str.map {case (k, v) => s"'$k', $v"} .mkString(", ")})"""

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

  def jsonb[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): String = {
    val (expr, str) = pair
    expr.project match {
      case Constant(Data.Obj(_)) =>
        str
      case _ =>
        s"($str)::jsonb"
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

  // Tries to reconcile types left and right side of != and = operators by doing the best possible casting.
  def findComparisonSideCasts[T[_[_]]: BirecursiveT](lSrc: T[SqlExpr], left: String, rSrc: T[SqlExpr], right: String): (String, String) = {

    def castSide(oppositeSideSrc: T[SqlExpr], src: T[SqlExpr], thisStr: String) =
      oppositeSideSrc.project match {
        case Constant(Data.Id(_)) => jsonb((src, thisStr))
        case Constant(Data.Int(_)) => num((src, thisStr))
        case Constant(Data.Dec(_)) => num((src, thisStr))
        case Constant(Data.Bool(_)) => bool((src, thisStr))
        case _ => s"(${text((src, thisStr))})::text"
      }

    (castSide(rSrc, lSrc, left), castSide(rSrc, lSrc, right))
  }

  private def postgresArray(jsonArrayRepr: String) = s"to_jsonb(array_to_json(ARRAY$jsonArrayRepr))"

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
    // The Refs element represents a field/column reference which is rendered in various ways.
    // We are using additional metadata of type Indirections, which carries information on
    // what kind of references we are using for each element. (If we want to render a.b.c, a.b->'c', etc.)
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
      buildJsonSingleton(m.map {
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
    case ArrayAgg((_, e)) =>
      s"array_agg($e)".right
    case Length((_, e)) =>
      //conditional expressions in Postgres have no defined lazyness guarantees and are effectively eager in a lot
      //of situations, see https://www.postgresql.org/docs/9.6/static/sql-expressions.html#SYNTAX-EXPRESS-EVAL
      //This necessesites that all branch subexpressions have compatible types, even when their evaluated values
      //make no sens for the given argument, hence the apparent convolution of the exoression below
      s"(case when (pg_typeof($e)::regtype::text ~ 'jsonb?') then jsonb_array_length(to_jsonb($e)) else length($e::text) end)".right
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
    case Not(BoolExpr(e)) =>
      s"not ($e)".right
    case DeleteKey((srcExp, src), (_, field)) =>
      (srcExp.project match {
        case Id(v, _) =>
          s"(row_to_json($v)::jsonb - $field)"
        case _ =>
          s"($src - $field)"
      }).right
    case Neg(NumExpr(e)) =>
      s"(-$e)".right
    case Eq((lSrc, lStr), (rSrc, rStr)) =>
      val (l, r) = findComparisonSideCasts(lSrc, lStr, rSrc, rStr)
      s"($l = $r)".right
    case Neq((lSrc, lStr), (rSrc, rStr)) =>
      val (l, r) = findComparisonSideCasts(lSrc, lStr, rSrc, rStr)
      s"($l != $r)".right
    case Lt(NumExpr(a1), NumExpr(a2)) =>
      s"($a1 < $a2)".right
    case Lte(NumExpr(a1), NumExpr(a2)) =>
      s"($a1 <= $a2)".right
    case Gt(NumExpr(a1), NumExpr(a2)) =>
      s"($a1 > $a2)".right
    case Gte(NumExpr(a1), NumExpr(a2)) =>
      s"($a1 >= $a2)".right
    case WithIds((_, str))    => s"row_number() over(), $str".right
    case RowIds()        => "row_number() over()".right
    case Offset((_, from), NumExpr(count)) => s"($from OFFSET $count)".right
    case Limit((_, from), NumExpr(count)) => s"($from LIMIT $count)".right
    case Select(selection, from, joinOpt,filterOpt, groupBy, order) =>
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
    case Constant(a @ Data.Arr(_)) =>  s"${dataFormatter("", a, JsonCol)}::jsonb".right
    case Constant(Data.Str(v)) =>
      val text = v.flatMap { case '\'' => "''"; case iv => iv.toString }.self
      s"'$text'".right
    case Constant(a @ Data.Obj(lm)) =>
      lm.toList match {
        case ((k, Data.Null) :: Nil) =>
          s"null as $k".right
        case _ =>
          s"${dataFormatter("", a, JsonCol)}::jsonb".right
      }
    case Constant(Data.Id(str)) =>
      DataCodec.render(Data.Id(str)).map(i => s"""'$i'""") \/> NonRepresentableData(Data.Id(str))
    case Constant(v) =>
      DataCodec.render(v) \/> NonRepresentableData(v)
    case Case(wt, e) =>
      val wts = wt ∘ { case WhenThen(TextExpr(w), TextExpr(t)) => s"when ($w)::boolean then $t" }
      s"(case ${wts.intercalate(" ")} else ${text(e.v)} end)".right
    case TypeOf(e) => s"pg_typeof($e)".right
    case Coercion(t, (eSrc, e)) =>
      val inner = t match {
        //these have to be converted raw, because otherwise we in e.g. 4.5::text::bigint,
        //which is an invalid conversion according to Postgres (unlike 4.5::bigint)
        case IntCol | DecCol => e
        case _ => text((eSrc, e))
      }
      s"($inner)::${t.mapToStringName}".right
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
        //QScripts ConcatArray is emitted both for strings and JSONs, using || here
        //since the same operator is used in Postgres for both.
        //May cause issues with mixed-typed (e.g. JSON/String) operands,
        //and necessitate explicit type casts.
        case ArrayConcat => s"($a1 || $a2)"
        case Contains => s"($a1 IN (SELECT jsonb_array_elements_text(to_jsonb($a2))))"
      }).right
    case TernaryFunction(fType, a1, a2, a3) => (fType match {
      case Search => s"(case when ${bool(a3)} then ${text(a1)} ~* ${text(a2)} else ${text(a1)} ~ ${text(a2)} end)"
      case Substring => s"substring(${text(a1)} from ((${text(a2)})::integer + 1) for (${text(a3)})::integer)"
    }).right

    case ArrayUnwind(toUnwind) => s"jsonb_array_elements_text(${text(toUnwind)})".right

    case Time((_, expr)) =>
      ???
      // TODO reimplement this
      // buildJson((TimeKey, expr)).right
    case Timestamp((_, expr)) =>
      ???
      // TODO reimplement this
      // buildJson((TimestampKey, expr)).right
    case DatePart(TextExpr(part), (_, expr)) =>
      ???
      // TODO reimplement this
      // s"date_part($part, to_timestamp($expr->>'$TimestampKey', 'YYYY-MM-DD HH24:MI:SSZ'))".right
  }
}
