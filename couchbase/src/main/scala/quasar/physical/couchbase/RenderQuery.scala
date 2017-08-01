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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar.{Data => QData, DataCodec}
import quasar.fp.ski.κ
import quasar.Planner.{NonRepresentableData, PlannerError}
import quasar.common.SortDir, SortDir.{Ascending, Descending}
import quasar.DataCodec.Precise.{DateKey, TimeKey, TimestampKey}

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

object RenderQuery {
  import N1QL._, Case._, Select._

  implicit val codec: DataCodec = common.CBDataCodec

  def compact[T[_[_]]: BirecursiveT](a: T[N1QL]): PlannerError \/ String = {
    val q = a.cataM(alg)

    a.project match {
      case s: Select[T[N1QL]] => q ∘ (s => s"select v from $s v")
      case _                  => q ∘ ("select " ⊹ _)
    }
  }

  val alg: AlgebraM[PlannerError \/ ?, N1QL, String] = {
    case Data(QData.Str(v)) =>
      ("'" ⊹ v.flatMap { case ''' => "''"; case v   => v.toString } ⊹ "'").right
    case Data(v) =>
      DataCodec.render(v) \/> NonRepresentableData(v)
    case Id(v) =>
      s"`$v`".right
    case Obj(m) =>
      m.map {
        case (k, v) => s"$k: $v"
      }.mkString("{", ", ", "}").right
    case Arr(l) =>
      l.mkString("[", ", ", "]").right
    case Date(a1) =>
      s"""{ "$DateKey": $a1 }""".right
    case Time(a1) =>
      s"""{ "$TimeKey": $a1 }""".right
    case Timestamp(a1) =>
      s"""{ "$TimestampKey": $a1 }""".right
    case Null() =>
      s"null".right
    case Unreferenced() =>
      s"(select value [])".right
    case SelectField(a1, a2) =>
      s"$a1.[$a2]".right
    case SelectElem(a1, a2) =>
      s"$a1[$a2]".right
    case Slice(a1, a2) =>
      s"$a1:${~a2}".right
    case ConcatStr(a1, a2) =>
      s"($a1 || $a2)".right
    case Not(a1) =>
      s"not $a1".right
    case Eq(a1, a2) =>
      s"($a1 = $a2)".right
    case Neq(a1, a2) =>
      s"($a1 != $a2)".right
    case Lt(a1, a2) =>
      s"($a1 < $a2)".right
    case Lte(a1, a2) =>
      s"($a1 <= $a2)".right
    case Gt(a1, a2) =>
      s"($a1 > $a2)".right
    case Gte(a1, a2) =>
      s"($a1 >= $a2)".right
    case IsNull(a1) =>
      s"($a1 is null)".right
    case IsNotNull(a1) =>
      s"($a1 is not null)".right
    case Neg(a1) =>
      s"(- $a1)".right
    case Add(a1, a2) =>
      s"($a1 + $a2)".right
    case Sub(a1, a2) =>
      s"($a1 - $a2)".right
    case Mult(a1, a2) =>
      s"($a1 * $a2)".right
    case Div(a1, a2) =>
      s"($a1 / $a2)".right
    case Mod(a1, a2) =>
      s"($a1 % $a2)".right
    case And(a1, a2) =>
      s"($a1 and $a2)".right
    case Or(a1, a2) =>
      s"($a1 or $a2)".right
    case Meta(a1) =>
      s"meta($a1)".right
    case ConcatArr(a1, a2) =>
      s"array_concat($a1, $a2)".right
    case ConcatObj(a1, a2) =>
      s"object_concat($a1, $a2)".right
    case IfNull(a) =>
      s"ifnull(${a.intercalate(", ")})".right
    case IfMissing(a) =>
      s"ifmissing(${a.intercalate(", ")})".right
    case IfMissingOrNull(a) =>
      s"ifmissingornull(${a.intercalate(", ")})".right
    case Type(a1) =>
      s"type($a1)".right
    case ToString(a1) =>
      s"tostring($a1)".right
    case ToNumber(a1) =>
      s"tonumber($a1)".right
    case Floor(a1) =>
      s"floor($a1)".right
    case Length(a1) =>
      s"length($a1)".right
    case LengthArr(a1) =>
      s"array_length($a1)".right
    case LengthObj(a1) =>
      s"object_length($a1)".right
    case IsString(a1) =>
      s"isstring($a1)".right
    case Lower(a1) =>
      s"lower($a1)".right
    case Upper(a1) =>
      s"upper($a1)".right
    case Split(a1, a2) =>
      s"split($a1, $a2)".right
    case Substr(a1, a2, a3) =>
      val l = ~(a3 ∘ (", " ⊹ _))
      s"substr($a1, $a2$l)".right
    case RegexContains(a1, a2) =>
      s"regex_contains($a1, $a2)".right
    case Least(a) =>
      s"least(${a.intercalate(", ")})".right
    case Pow(a1, a2) =>
      s"power($a1, $a2)".right
    case Ceil(a1) =>
      s"ceil($a1)".right
    case Millis(a1) =>
      s"millis($a1)".right
    case MillisToUTC(a1, a2) =>
      val fmt = ~(a2 ∘ (", " ⊹ _))
      s"millis_to_utc($a1$fmt)".right
    case DateAddStr(a1, a2, a3) =>
      s"date_add_str($a1, $a2, $a3)".right
    case DatePartStr(a1, a2) =>
      s"date_part_str($a1, $a2)".right
    case DateDiffStr(a1, a2, a3) =>
      s"date_diff_str($a1, $a2, $a3)".right
    case DateTruncStr(a1, a2) =>
      s"date_trunc_str($a1, $a2)".right
    case StrToMillis(a1) =>
      s"str_to_millis($a1)".right
    case NowStr() =>
      s"now_str()".right
    case ArrContains(a1, a2) =>
      s"array_contains($a1, $a2)".right
    case ArrRange(a1, a2, a3) =>
      val step = ~(a3 ∘ (", " ⊹ _))
      s"array_range($a1, $a2$step)".right
    case IsArr(a1) =>
      s"isarray($a1)".right
    case ObjNames(a1) =>
      s"object_names($a1)".right
    case ObjValues(a1) =>
      s"object_values($a1)".right
    case ObjRemove(a1, a2) =>
      s"object_remove($a1, $a2)".right
    case IsObj(a1) =>
      s"isobject($a1)".right
    case Avg(a1) =>
      s"avg($a1)".right
    case Count(a1) =>
      s"count($a1)".right
    case Max(a1) =>
      s"max($a1)".right
    case Min(a1) =>
      s"min($a1)".right
    case Sum(a1) =>
      s"sum($a1)".right
    case ArrAgg(a1) =>
      s"array_agg($a1)".right
    case Union(a1, a2) =>
      s"($a1 union $a2)".right
    case ArrFor(a1, a2, a3) =>
      s"(array $a1 for $a2 in $a3 end)".right
    case Select(v, re, ks, jn, un, lt, ft, gb, ob) =>
      def alias(a: Option[Id[String]]) = ~(a ∘ (i => s" as `${i.v}`"))
      val value       = v.v.fold("value ", "")
      val resultExprs = (re ∘ (r => r.expr ⊹ alias(r.alias))).intercalate(", ")
      val kSpace      = ~(ks ∘ (k => s" from ${k.expr}" ⊹ alias(k.alias)))
      val join        = ~(jn ∘ (j =>
                          j.joinType.fold(κ(""), κ(" left outer")) ⊹ s" join `${j.id.v}`" ⊹ alias(j.alias) ⊹
                          " on keys " ⊹ j.pred))
      val unnest      = ~(un ∘ (u => s" unnest ${u.expr}" ⊹ alias(u.alias)))
      val let         = lt.toNel.foldMap(
                          " let " ⊹ _.map(b => s"${b.id.v} = ${b.expr}").intercalate(", "))
      val filter      = ~(ft ∘ (f  => s" where ${f.v}"))
      val groupBy     = ~(gb ∘ (g  => s" group by ${g.v}"))
      val orderBy     =
        ~((ob ∘ {
          case OrderBy(a, Ascending)  => s"$a ASC"
          case OrderBy(a, Descending) => s"$a DESC"
        }).toNel ∘ (" order by " ⊹ _.intercalate(", ")))
      s"(select $value$resultExprs$kSpace$join$unnest$let$filter$groupBy$orderBy)".right
    case Case(wt, e) =>
      val wts = wt ∘ { case WhenThen(w, t) => s"when $w then $t" }
      s"(case ${wts.intercalate(" ")} else ${e.v} end)".right
  }
}
