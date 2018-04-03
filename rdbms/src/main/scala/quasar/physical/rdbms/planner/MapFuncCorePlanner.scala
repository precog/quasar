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

package quasar.physical.rdbms.planner

import slamdata.Predef._
import slamdata.Predef.{Eq => _}
import quasar.Data
import quasar.Planner._
import quasar.physical.rdbms.planner.sql.{Contains, StrLower, StrUpper, Substring, Search, StrSplit, ArrayConcat, SqlExpr => SQL}
import quasar.physical.rdbms.planner.sql._
import quasar.physical.rdbms.planner.sql.SqlExpr._
import quasar.qscript.{MapFuncsCore => MFC, _}
import matryoshka._
import matryoshka.implicits._
import quasar.physical.rdbms.model.{BoolCol, DecCol, IntCol, StringCol}

import scalaz._
import Scalaz._
import quasar.physical.rdbms.planner.sql.Indirections._


class MapFuncCorePlanner[T[_[_]]: BirecursiveT: ShowT, F[_] : Applicative : PlannerErrorME]
    extends Planner[T, F, MapFuncCore[T, ?]] {

  val undefined: T[SQL] = SQL.Null[T[SQL]]().embed

  def str(s: String): T[SQL] = SQL.Constant[T[SQL]](Data.Str(s)).embed

  // TODO consider reusing "project()"
  def toKeyValue(expr: T[SQL], key: String): T[SQL] = {
    val nr: Refs[T[SQL]] = expr.project match {
      case Refs(elems, m) => Refs(elems :+ str(key), m)
      case _ => Refs(Vector(expr, str(key)), deriveIndirection(expr))
    }
    nr.embed
  }

  private def project(fSrc: T[SQL], fKey: T[SQL]) = fSrc.project match {
    case SQL.Refs(list, m) => SQL.Refs(list :+ fKey, m).embed.η[F]
    case _ => SQL.Refs(Vector(fSrc, fKey), deriveIndirection(fSrc)).embed.η[F] // _12
  }

  private def datePart(part: String, f: T[SQL]) = SQL.DatePart(SQL.Constant[T[SQL]](Data.Str(part)).embed, f).embed.η[F]

  def plan: AlgebraM[F, MapFuncCore[T, ?], T[SQL]] = {
    case MFC.Constant(ejson) => SQL.Constant[T[SQL]](ejson.cata(Data.fromEJson)).embed.η[F]
    case MFC.Undefined() =>  undefined.η[F]
    case MFC.JoinSideName(n) =>  notImplemented("JoinSideName", this)
    case MFC.Length(f) => SQL.Length(f).embed.η[F]
    case MFC.LocalDateTime(f) => notImplemented("LocalDateTime", this)
    case MFC.LocalDate(f) => notImplemented("LocalDate", this)
    case MFC.LocalTime(f) => notImplemented("LocalTime", this)
    case MFC.OffsetDateTime(f) => notImplemented("OffsetDateTime", this)
    case MFC.OffsetDate(f) => notImplemented("OffsetDate", this)
    case MFC.OffsetTime(f) => notImplemented("OffsetTime", this)
    case MFC.Interval(f) => notImplemented("Interval", this)
    case MFC.StartOfDay(f) =>  notImplemented("StartOfDay", this)
    case MFC.TemporalTrunc(p, f) =>  notImplemented("TemporalTrunc", this)
    case MFC.TimeOfDay(f) =>  notImplemented("TimeOfDay", this)
    case MFC.ToTimestamp(f) =>  notImplemented("ToTimestamp", this)
    case MFC.SetTimeZone(f1, f2) =>  notImplemented("SetTimeZone", this)
    case MFC.SetTimeZoneMinute(f1, f2) =>  notImplemented("SetTimeZoneMinute", this)
    case MFC.SetTimeZoneHour(f1, f2) =>  notImplemented("SetTimeZoneHour", this)
    case MFC.ExtractCentury(f) =>   datePart("century", f)
    case MFC.ExtractDayOfMonth(f) =>   datePart("day", f)
    case MFC.ExtractDecade(f) =>   datePart("decade", f)
    case MFC.ExtractDayOfWeek(f) =>   datePart("dow", f)
    case MFC.ExtractDayOfYear(f) =>   datePart("doy", f)
    case MFC.ExtractEpoch(f) =>   datePart("epoch", f)
    case MFC.ExtractHour(f) =>   datePart("hour", f)
    case MFC.ExtractIsoDayOfWeek(f) =>   datePart("isodow", f)
    case MFC.ExtractIsoYear(f) =>   datePart("isoyear", f)
    case MFC.ExtractMicrosecond(f) =>  datePart("microseconds", f)
    case MFC.ExtractMillennium(f) =>   datePart("millennium", f)
    case MFC.ExtractMillisecond(f) =>   datePart("milliseconds", f)
    case MFC.ExtractMinute(f) =>   datePart("minute", f)
    case MFC.ExtractMonth(f) =>   datePart("month", f)
    case MFC.ExtractQuarter(f) =>   datePart("quarter", f)
    case MFC.ExtractSecond(f) =>   datePart("second", f)
    case MFC.ExtractWeek(f) =>  datePart("week", f)
    case MFC.ExtractYear(f) =>  datePart("year", f)
    case MFC.Now() =>  notImplemented("Now", this)
    case MFC.Negate(f) =>  SQL.Neg[T[SQL]](f).embed.η[F]
    case MFC.Add(f1, f2) =>  SQL.NumericOp[T[SQL]]("+", f1, f2).embed.η[F]
    case MFC.Multiply(f1, f2) => SQL.NumericOp[T[SQL]]("*", f1, f2).embed.η[F]
    case MFC.Subtract(f1, f2) =>  SQL.NumericOp[T[SQL]]("-", f1, f2).embed.η[F]
    case MFC.Divide(f1, f2) => SQL.NumericOp[T[SQL]]("/", f1, f2).embed.η[F]
    case MFC.Modulo(f1, f2) => SQL.Mod[T[SQL]](f1, f2).embed.η[F]
    case MFC.Power(f1, f2) =>  SQL.Pow[T[SQL]](f1, f2).embed.η[F]
    case MFC.Eq(f1, f2) => SQL.Eq[T[SQL]](f1, f2).embed.η[F]
    case MFC.Neq(f1, f2) => SQL.Neq[T[SQL]](f1, f2).embed.η[F]
    case MFC.Lt(f1, f2) =>  SQL.Lt[T[SQL]](f1, f2).embed.η[F]
    case MFC.Lte(f1, f2) => SQL.Lte[T[SQL]](f1, f2).embed.η[F]
    case MFC.Gt(f1, f2) =>  SQL.Gt[T[SQL]](f1, f2).embed.η[F]
    case MFC.Gte(f1, f2) => SQL.Gte[T[SQL]](f1, f2).embed.η[F]
    case MFC.IfUndefined(f1, f2) => SQL.IfNull.build(f1, f2).embed.η[F]
    case MFC.And(f1, f2) =>  SQL.And[T[SQL]](f1, f2).embed.η[F]
    case MFC.Or(f1, f2) =>  SQL.Or[T[SQL]](f1, f2).embed.η[F]
    case MFC.Not(f) =>  SQL.Not[T[SQL]](f).embed.η[F]
    case MFC.Between(f1, f2, f3) =>  notImplemented("Between", this)
    case MFC.Cond(fCond, fThen, fElse) =>  notImplemented("Cond", this)
    case MFC.Within(f1, f2) =>  SQL.BinaryFunction(Contains, f1, f2).embed.η[F]
    case MFC.Lower(f) =>  SQL.UnaryFunction(StrLower, f).embed.η[F]
    case MFC.Upper(f) =>  SQL.UnaryFunction(StrUpper, f).embed.η[F]
    case MFC.Bool(f) =>  SQL.Coercion(BoolCol, f).embed.η[F]
    case MFC.Integer(f) =>  SQL.Coercion(IntCol, f).embed.η[F]
    case MFC.Decimal(f) =>  SQL.Coercion(DecCol, f).embed.η[F]
    case MFC.Null(f) =>  SQL.Null[T[SQL]].embed.η[F]
    case MFC.ToString(f) =>  SQL.Coercion(StringCol, f).embed.η[F]
    case MFC.Search(fStr, fPattern, fIsCaseInsensitive) =>
      fIsCaseInsensitive.project match {
        case Constant(Data.Bool(insensitive)) =>
          SQL.RegexMatches(fStr, fPattern, insensitive).embed.η[F]
        case _ =>
          SQL.TernaryFunction(Search, fStr, fPattern, fIsCaseInsensitive).embed.η[F]
      }
    case MFC.Substring(fStr, fFrom, fCount) => SQL.TernaryFunction(Substring, fStr, fFrom, fCount).embed.η[F]
    case MFC.Split(fStr, fDelim) => SQL.BinaryFunction(StrSplit, fStr, fDelim).embed.η[F]
    case MFC.MakeArray(f) =>  SQL.ToArray(f).embed.η[F]
    case MFC.MakeMap(key, value) =>
      key.project match {
              case Constant(Data.Str(keyStr)) =>
                SQL.ExprWithAlias[T[SQL]](value, keyStr).embed.η[F]
              case other =>
                notImplemented(s"MakeMap with key = $other", this)
            }
    case MFC.ConcatArrays(f1, f2) =>  SQL.BinaryFunction(ArrayConcat, f1, f2).embed.η[F]
    case MFC.ConcatMaps(f1, f2) =>
      (f1.project, f2.project) match {
        case (ExprWithAlias(e1, a1), ExprWithAlias(e2, a2)) =>

          // This builds information about indirections for pairs of expressions with aliases
          // The alias becomes a part of information about indirection, so that we know that
          // further references to this alias have the same type of indirection (dot or arrow)
          // that referenced expression.
          val patmat: PartialFunction[String, (IndirectionType, Indirection)] = {
            case `a1` => (Field, deriveIndirection(e1))
            case `a2` => (Field, deriveIndirection(e2))
          }

          val meta = Branch(patmat.orElse {
            s => Default match {
              case Branch(m, _) => {
                m(s)
              }
            }
          }, s"$a1 -> (Dot, ${deriveIndirection(e1).shows}), $a2 -> (Dot, ${deriveIndirection(e2).shows})")

          ExprPair[T[SQL]](f1, f2, meta).embed.η[F]

        case (o1, o2) =>
          ExprPair[T[SQL]](f1, f2, Default).embed.η[F]
      }

    case MFC.ProjectIndex(fSrc, fKey) => project(fSrc, fKey)
    case MFC.ProjectKey(fSrc, fKey) => project(fSrc, fKey)
    case MFC.DeleteKey(fSrc, fField) => DeleteKey(fSrc, fField).embed.η[F]
    case MFC.Range(fFrom, fTo) =>  notImplemented("Range", this)
    case MFC.Guard(f1, fPattern, f2, ff3) => f2.η[F]
    case MFC.TypeOf(f) => SQL.TypeOf(f).embed.η[F]
    case other => unexpected(other.getClass.getName, this)
  }
}
