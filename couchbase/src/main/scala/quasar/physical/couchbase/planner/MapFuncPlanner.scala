/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.couchbase.planner

import quasar.Predef._
import quasar.DataCodec, DataCodec.Precise.{DateKey, TimeKey, TimestampKey}
import quasar.{NameGenerator, Type}
import quasar.Planner.{NonRepresentableEJson, PlannerError}
import quasar.common.PhaseResult.detail
import quasar.contrib.matryoshka._
import quasar.fp._, eitherT._
import quasar.fp.ski.κ
import quasar.physical.couchbase._, N1QL._, Select._
import quasar.physical.couchbase.common.CBDataCodec
import quasar.qscript, qscript.{Map => _, _}
import quasar.std.StdLib.string._

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz.{ToIdOps => _, _}

final class MapFuncPlanner[F[_]: Monad: NameGenerator, T[_[_]]: Recursive: ShowT]
  extends Planner[F, MapFunc[T, ?]] {
  import MapFuncs._

  implicit val codec = CBDataCodec

  // TODO: Move datetime handling out of generated N1QL once enriched N1QL representation is available

  def unwrap(expr: N1QL): N1QL = {
    val exprN1ql = n1ql(expr)
    partialQueryString(
      s"""ifmissing($exprN1ql.["$DateKey"], $exprN1ql.["$TimeKey"], $exprN1ql.["$TimestampKey"], $exprN1ql)"""
    )
  }

  def extract(expr: N1QL, part: String): M[N1QL] =
    partialQueryString(
      s"""date_part_str(${n1ql(unwrap(expr))}, "$part")"""
    ).point[M]

  def datetime(expr: N1QL, key: String, regex: String): M[N1QL] = {
    val exprN1ql = n1ql(expr)
    partialQueryString(s"""
      (case
       when $exprN1ql.["$key"] is not null then $exprN1ql
       when regexp_contains($exprN1ql, "$regex") then { "$key": $exprN1ql }
       else null
       end)"""
    ).point[M]
  }

  def rel(a1: N1QL, a2: N1QL, op: String): M[N1QL] = {
    val a1N1ql = n1ql(a1)
    val a2N1ql = n1ql(a2)
    val a1Unwrapped = n1ql(unwrap(a1))
    val a2Unwrapped = n1ql(unwrap(a2))

    // TODO: De-stringly
    val q = (op, a2N1ql) match {
      case ("=",  "null") => s"$a1Unwrapped is null"
      case ("!=", "null") => s"$a1Unwrapped is not null"
      case _              =>
        s"""
          (case
           when ifmissing($a1N1ql.["$DateKey"], $a2N1ql.["$DateKey"]) is not null
             then date_diff_str($a1Unwrapped, $a2Unwrapped, "day") $op 0
           when ifmissing($a1N1ql.["$TimeKey"], $a1N1ql.["$TimestampKey"], $a2N1ql.["$TimeKey"], $a2N1ql.["$TimestampKey"]) is not null
             then date_diff_str($a1Unwrapped, $a2Unwrapped, "millisecond") $op 0
           else $a1Unwrapped $op $a2Unwrapped
           end)"""
    }

    partialQueryString(q).point[M]
  }

  def proj(a1: N1QL, a2: N1QL): N1QL =
    partialQueryString(s"""${n1ql(a1)}.[${n1ql(a2)}]""")

  def plan: AlgebraM[M, MapFunc[T, ?], N1QL] = {
    // nullary
    case Constant(v) =>
      EitherT(v.cataM(quasar.Data.fromEJson) >>= (d =>
        DataCodec.render(d).bimap(
          κ(NonRepresentableEJson(v.shows): PlannerError),
          partialQueryString(_)
        ).point[PR]
      ))
    case Undefined() =>
      partialQueryString("null").point[M]

    // array
    case Length(a1) =>
      val a1N1ql = n1ql(a1)
      partialQueryString(s"ifmissingornull(length($a1N1ql), array_length($a1N1ql), object_length($a1N1ql), $naStr)").point[M]

    // date
    case Date(a1) =>
      datetime(a1, DateKey, dateRegex)
    case Time(a1) =>
      datetime(a1, TimeKey, timeRegex)
    case Timestamp(a1) =>
      datetime(a1, TimestampKey, timestampRegex)
    case Interval(a1)              =>
      unimplementedP("Interval")
    case TimeOfDay(a1)             =>
      val a1N1ql = n1ql(a1)
      def fracZero(expr: String) = s"""
        (case
         when regex_contains($expr, "[.]") then { "$TimeKey": $expr }
         when $expr is not null then { "$TimeKey": $expr || ".000" }
         else $naStr
         end)"""
      partialQueryString(s"""
        (case
         when $a1N1ql.["$DateKey"] then $naStr
         when $a1N1ql.["$TimeKey"] then $a1N1ql
         when $a1N1ql.["$TimestampKey"] then { "$TimeKey": split(split($a1N1ql.["$TimestampKey"], "T")[1], "Z")[0] }
         else ${fracZero(s"""millis_to_utc(millis($a1N1ql), "00:00:00.000")""")}
         end)"""
      ).point[M]
    case ToTimestamp(a1)           =>
      partialQueryString(
        s"""{ "$TimestampKey": millis_to_utc(${n1ql(a1)}) }"""
      ).point[M]
    case ExtractCentury(a1)        =>
      extract(a1, "year") ∘ (y => partialQueryString(s"ceil(${n1ql(y)} / 100)"))
    case ExtractDayOfMonth(a1)     =>
      extract(a1, "day")
    case ExtractDecade(a1)         =>
      extract(a1, "decade")
    case ExtractDayOfWeek(a1)      =>
      extract(a1, "day_of_week")
    case ExtractDayOfYear(a1)      =>
      extract(a1, "day_of_year")
    case ExtractEpoch(a1)          =>
      val a1N1ql = n1ql(a1)
      partialQueryString(s"""
        millis(
          case
          when $a1N1ql.["$DateKey"] then $a1N1ql.["$DateKey"] || "T00:00:00.000Z"
          when $a1N1ql.["$TimeKey"] then $naStr
          else ifmissing($a1N1ql.["$TimestampKey"], $a1N1ql)
          end
        ) / 1000"""
      ).point[M]
    case ExtractHour(a1)           =>
      extract(a1, "hour")
    case ExtractIsoDayOfWeek(a1)   =>
      extract(a1, "iso_dow")
    case ExtractIsoYear(a1)        =>
      extract(a1, "iso_year")
    case ExtractMicroseconds(a1)   =>
      (
        extract(a1, "second")      ⊛
        extract(a1, "millisecond")
      )((seconds, millis) =>
        partialQueryString(s"((${n1ql(seconds)} * 1000) + ${n1ql(millis)}) * 1000")
      )
    case ExtractMillennium(a1)     =>
      extract(a1, "year") ∘ (y => partialQueryString(s"ceil(${n1ql(y)} / 1000)"))
    case ExtractMilliseconds(a1)   =>
    (
      extract(a1, "second")      ⊛
      extract(a1, "millisecond")
    )((seconds, millis) =>
      partialQueryString(s"(${n1ql(seconds)} * 1000) + ${n1ql(millis)}")
    )
    case ExtractMinute(a1)         =>
      extract(a1, "minute")
    case ExtractMonth(a1)          =>
      extract(a1, "month")
    case ExtractQuarter(a1)        =>
      extract(a1, "quarter")
    case ExtractSecond(a1)         =>
      (
        extract(a1, "second")      ⊛
        extract(a1, "millisecond")
      )((seconds, millis) =>
        partialQueryString(s"${n1ql(seconds)} + (${n1ql(millis)} / 1000)")
      )
    case ExtractTimezone(a1)       =>
      extract(a1, "timezone")
    case ExtractTimezoneHour(a1)   =>
      extract(a1, "timezone_hour")
    case ExtractTimezoneMinute(a1) =>
      extract(a1, "timezone_minute")
    case ExtractWeek(a1)           =>
      extract(a1, "iso_week")
    case ExtractYear(a1)           =>
      extract(a1, "year")
    case Now() =>
      partialQueryString("now_str()").point[M]

    // math
    case Negate(a1)       =>
      partialQueryString(s"-${n1ql(a1)}").point[M]
    case Add(a1, a2)      =>
      partialQueryString(s"(${n1ql(a1)} + ${n1ql(a2)})").point[M]
    case Multiply(a1, a2) =>
      partialQueryString(s"(${n1ql(a1)} * ${n1ql(a2)})").point[M]
    case Subtract(a1, a2) =>
      partialQueryString(s"(${n1ql(a1)} - ${n1ql(a2)})").point[M]
    case Divide(a1, a2)   =>
      partialQueryString(s"(${n1ql(a1)} / ${n1ql(a2)})").point[M]
    case Modulo(a1, a2)   =>
      partialQueryString(s"(${n1ql(a1)} % ${n1ql(a2)})").point[M]
    case Power(a1, a2)    =>
      partialQueryString(s"power(${n1ql(a1)}, ${n1ql(a2)})").point[M]

    // relations
    case Not(a1)             =>
      partialQueryString(s"not ${n1ql(a1)}").point[M]
    case Eq(a1, a2)          =>
      rel(a1, a2, "=")
    case Neq(a1, a2)         =>
      val a1N1ql = n1ql(a1)
      val a2N1ql = n1ql(a2)
      for {
        r <- rel(a1, a2, "!=")
        _ <- prtell[M](Vector(detail(
               "N1QL Neq",
               s"""  a1:   $a1N1ql
                  |  a2:   $a2N1ql
                  |  n1ql: ${n1ql(r)}""".stripMargin('|')
             )))
      } yield r
    case Lt(a1, a2)          =>
      rel(a1, a2, "<")
    case Lte(a1, a2)         =>
      rel(a1, a2, "<=")
    case Gt(a1, a2)          =>
      rel(a1, a2, ">")
    case Gte(a1, a2)         =>
      rel(a1, a2, ">=")
    case IfUndefined(a1, a2) =>
      partialQueryString(s"ifmissing(${n1ql(a1)}, ${n1ql(a2)})").point[M]
    case And(a1, a2)         =>
      partialQueryString(s"(${n1ql(a1)} and ${n1ql(a2)})").point[M]
    case Or(a1, a2)          =>
      partialQueryString(s"(${n1ql(a1)} or ${n1ql(a2)})").point[M]
    case Between(a1, a2, a3) =>
      val a1N1ql = n1ql(a1)
      val a2N1ql = n1ql(a2)
      val a3N1ql = n1ql(a3)
      for {
        b <- (rel(a1, a2, ">=") ⊛ rel(a1, a3, "<="))((a, b) =>
               partialQueryString(s"${n1ql(a)} and ${n1ql(b)}")
             )
        _ <- prtell[M](Vector(detail(
               "N1QL Between",
               s"""  a1:   $a1N1ql
                  |  a2:   $a2N1ql
                  |  a3:   $a3N1ql
                  |  n1ql: $b""".stripMargin('|')
             )))
      } yield b
    case Cond(cond, then_, else_) =>
      partialQueryString(s"""
        (case
         when ${n1ql(cond)} then ${n1ql(then_)}
         else ${n1ql(else_)}
         end)"""
      ).point[M]

    // set
    case Within(a1, a2) =>
      partialQueryString(s"array_contains(${n1ql(a2)}, ${n1ql(a1)})").point[M]

    // string
    case Lower(a1)             =>
      partialQueryString(s"lower(${n1ql(a1)})").point[M]
    case Upper(a1)             =>
      partialQueryString(s"upper(${n1ql(a1)})").point[M]
    case Bool(a1)              =>
      val a1N1ql = n1ql(a1)
      partialQueryString(s"""
        (case
         when lower($a1N1ql) = "true"  then true
         when lower($a1N1ql) = "false" then false
         else null
         end)"""
      ).point[M]
    // TODO: Handle large numbers across the board. Couchbase's number type truncates.
    case Integer(a1)           =>
      val a1N1ql = n1ql(a1)
      partialQueryString(s"""
        (case
         when tonumber($a1N1ql) = floor(tonumber($a1N1ql)) then tonumber($a1N1ql)
         else null
         end)"""
      ).point[M]
    case Decimal(a1)           =>
      partialQueryString(s"""tonumber(${n1ql(a1)})""").point[M]
    case Null(a1)              =>
      partialQueryString(s"""
        (case
         when lower(${n1ql(a1)}) = "null" then null
         else $naStr
         end)"""
      ).point[M]
    case ToString(a1)          =>
      val a1N1ql = n1ql(a1)
      partialQueryString(
        s"""ifnull(tostring($a1N1ql), ${n1ql(unwrap(a1))}, case when type($a1N1ql) = "null" then "null" else $a1N1ql end)"""
      ).point[M]
    case Search(a1, a2, a3)    =>
      val a1N1ql = n1ql(a1)
      val a2N1ql = n1ql(a2)
      val a3N1ql = n1ql(a3)
      partialQueryString(s"""
        (case
         when $a3N1ql then regexp_contains($a1N1ql, "(?i)(?s)" || $a2N1ql)
         else regexp_contains($a1N1ql, "(?s)" || $a2N1ql)
         end)"""
       ).point[M]
    case Substring(a1, a2, a3) =>
      val a1N1ql = n1ql(a1)
      val a2N1ql = n1ql(a2)
      val a3N1ql = n1ql(a3)
      partialQueryString(s"""
        (case
         when $a2N1ql < 0 then ""
         when $a3N1ql < 0 then ifnull(substr($a1N1ql, $a2N1ql), "")
         else ifnull(substr($a1N1ql, $a2N1ql, least($a3N1ql, length($a1N1ql) - $a2N1ql)), "")
         end)"""
      ).point[M]

    // structural
    case MakeArray(a1)                            =>
      val a1N1ql  = n1ql(a1)
      val n1qlStr = s"[$a1N1ql]"
      prtell[M](Vector(detail(
        "N1QL MakeArray",
        s"""  a1:   $a1N1ql
           |  n1ql: $n1qlStr""".stripMargin('|')
      ))).as(
        partialQueryString(n1qlStr)
      )
    case MakeMap(a1, a2)                          =>
      for {
        tmpName1 <- genName[M]
        a1N1ql   =  n1ql(a1)
        a2N1ql   =  n1ql(a2)
        sel      =  select(
                      value         = true,
                      resultExprs   = s"{ to_string($a1N1ql): ifnull($tmpName1, $naStr) }".wrapNel,
                      keyspace      = a2,
                      keyspaceAlias = tmpName1)
        objAdd   =  partialQueryString(s"""object_add({}, $a1N1ql, $a2N1ql)""")
        rN1ql    =  a2 match {
                      case _: Select => sel
                      case _         => objAdd
                    }
        _        <- prtell[M](Vector(detail(
                      "N1QL MakeMap",
                      s"""  a1:   $a1N1ql
                         |  a2:   $a2N1ql
                         |  n1ql: ${n1ql(rN1ql)}""".stripMargin('|')
                    )))
      } yield rN1ql
    case ConcatArrays(a1, a2)                     =>
      val a1N1ql  = n1ql(a1)
      val a2N1ql  = n1ql(a2)
      val n1qlStr = s"""
        ifnull($a1N1ql || $a2N1ql, array_concat(
          case when isstring($a1N1ql) then split($a1N1ql, "") else $a1N1ql end,
          case when isstring($a2N1ql) then split($a2N1ql, "") else $a2N1ql end))"""
      prtell[M](Vector(detail(
        "N1QL ConcatArrays",
        s"""  a1:   $a1N1ql
           |  a2:   $a2N1ql
           |  n1ql: $n1qlStr""".stripMargin('|')
      ))).as(
        partialQueryString(n1qlStr)
      )
    case ConcatMaps(a1, a2)                       =>
      val a1N1ql  = n1ql(a1)
      val a2N1ql  = n1ql(a2)
      val n1qlStr = s"object_concat($a1N1ql, $a2N1ql)"
      prtell[M](Vector(detail(
        "N1QL ConcatMaps",
        s"""  a1:   $a1N1ql
           |  a2:   $a2N1ql
           |  n1ql: $n1qlStr""".stripMargin('|')
      ))).as(
        partialQueryString(n1qlStr)
      )
    case ProjectField(a1N1ql @ PartialQueryString(a1), a2) =>
      val r = proj(a1N1ql, a2)
      prtell[M](Vector(detail(
        "N1QL ProjectField(PartialQueryString(_), _)",
        s"""  a1:   $a1
           |  a2:   ${n1ql(a2)}
           |  n1ql: ${n1ql(r)}""".stripMargin('|')
      ))).as(
        r
      )
    case ProjectField(a1, a2) =>
      for {
        tempName <- genName[M]
        a1N1ql   =  n1ql(a1)
        a2N1ql   =  n1ql(a2)
        s        =  select(
                      value         = true,
                      resultExprs   = n1ql(proj(partialQueryString(tempName), a2)).wrapNel,
                      keyspace      = a1,
                      keyspaceAlias = tempName)
        sN1ql    =  n1ql(s)
        _        <- prtell[M](Vector(detail(
                      "N1QL ProjectField(_, _)",
                      s"""  a1:   $a1N1ql
                         |  a2:   $a2N1ql
                         |  n1ql: $sN1ql""".stripMargin('|'))))
      } yield s
    case ProjectIndex(PartialQueryString(a1), a2) =>
      val a2N1ql  = n1ql(a2)
      val n1qlStr = s"$a1[$a2N1ql]"
      prtell[M](Vector(detail(
        "N1QL ProjectIndex(PartialQueryString(_), _)",
        s"""  a1:   $a1
           |  a2:   $a2N1ql
           |  n1ql: $n1qlStr""".stripMargin('|')
      ))).as(
        partialQueryString(n1qlStr)
      )
    case ProjectIndex(a1, a2)                     =>
      for {
        tmpName1 <- genName[M]
        tmpName2 <- genName[M]
        a1N1ql   =  n1ql(a1)
        a2N1ql   =  n1ql(a2)
                    // TODO: custom select for the moment
        n1qlStr  =  s"(select value $tmpName1[$a2N1ql] let $tmpName1 = (select value array_agg($tmpName2) from $a1N1ql $tmpName2))"
        _        <- prtell[M](Vector(detail(
                      "N1QL ProjectIndex(_, _)",
                      s"""  a1:   $a1N1ql
                         |  a2:   $a2N1ql
                         |  n1ql: $n1qlStr""".stripMargin('|'))))
      } yield partialQueryString(n1qlStr)
    case DeleteField(a1, a2)                      =>
      partialQueryString(s"object_remove(${n1ql(a1)}, ${n1ql(a2)})").point[M]

    case Meta(a1) =>
      partialQueryString(s"meta(${n1ql(a1)})").point[M]

    // helpers & QScript-specific
    case Range(a1, a2)             =>
      val a1N1ql = n1ql(a1)
      val a2N1ql = n1ql(a2)
      partialQueryString(s"[$a2N1ql:($a1N1ql + 1)]").point[M]
    case Guard(expr, typ, cont, _) =>
      val exprN1ql     = n1ql(expr)
      val contN1ql     = n1ql(cont)
      def grd(f: String, e: String, c: String) =
        partialQueryString(s"(case when $f($e) then $c else $naStr end)")
      def grdSel(f: String) = genName[M] ∘ (n =>
        select(
          value         = true,
          resultExprs   = n1ql(grd(f, n, n)).wrapNel,
          keyspace      = cont,
          keyspaceAlias = n))
      ((cont, typ) match {
        case (_: Select, _: Type.FlexArr) => grdSel("isarray")
        case (_: Select, _: Type.Obj)     => grdSel("isobject")
        case (_        , _: Type.FlexArr) => grd("isarray", exprN1ql, contN1ql).point[M]
        case (_        , _: Type.Obj)     => grd("isobject", exprN1ql, contN1ql).point[M]
        case _                            => cont.point[M]
      }) >>= (r =>
        prtell[M](Vector(detail(
         "N1QL Guard",
         s"""  expr:     $exprN1ql
            |  typ:      $typ
            |  cont:     $contN1ql
            |  n1ql:     ${n1ql(r)}""".stripMargin('|')
        ))).as(
         r
        )
      )
  }
}
