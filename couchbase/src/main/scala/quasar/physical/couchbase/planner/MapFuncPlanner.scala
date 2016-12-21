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
import quasar.{Data => QData, Type => QType, NameGenerator}
import quasar.fp._, eitherT._
import quasar.physical.couchbase._, N1QL.{Eq, Split, _}, Case._, Select.{Value, _}
import quasar.qscript, qscript.{MapFunc, MapFuncs => MF}
import quasar.std.StdLib.string.{dateRegex, timeRegex, timestampRegex}

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

final class MapFuncPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: NameGenerator]
  extends Planner[T, F, MapFunc[T, ?]] {

  def str(s: String): T[N1QL]   = Data[T[N1QL]](QData.Str(s)).embed
  def int(i: Int): T[N1QL]      = Data[T[N1QL]](QData.Int(i)).embed
  def bool(b: Boolean): T[N1QL] = Data[T[N1QL]](QData.Bool(b)).embed
  val na: T[N1QL]               = Data[T[N1QL]](QData.NA).embed

  def unwrap(a1: T[N1QL]): T[N1QL] =
    IfMissing(
      SelectField(a1, str(DateKey)).embed,
      SelectField(a1, str(TimeKey)).embed,
      SelectField(a1, str(TimestampKey)).embed,
      a1).embed

  def extract(a1: T[N1QL], part: String): T[N1QL] =
    DatePartStr(unwrap(a1), str(part)).embed

  def rel(op: N1QL[T[N1QL]]): T[N1QL] = {
    def handleDates(a1: T[N1QL], a2: T[N1QL], o: (T[N1QL], T[N1QL]) => N1QL[T[N1QL]]): T[N1QL] = {
      val a1U = unwrap(a1)
      val a2U = unwrap(a2)
      Case(
        WhenThen(
          IsNotNull(IfMissing(
            SelectField(a1, str(DateKey)).embed,
            SelectField(a2, str(DateKey)).embed).embed).embed,
          o(DateDiffStr(a1U, a2U, str("day")).embed, int(0)).embed),
        WhenThen(
          IsNotNull(IfMissing(
            SelectField(a1, str(TimeKey)).embed,
            SelectField(a1, str(TimestampKey)).embed,
            SelectField(a2, str(TimeKey)).embed,
            SelectField(a2, str(TimestampKey)).embed).embed).embed,
          o(DateDiffStr(a1U, a2U, str("millisecond")).embed, int(0)).embed)
      )(
        Else(o(a1, a2).embed)
      ).embed
    }

    op match {
      case Eq(a1, a2)              => handleDates(a1, a2, Eq(_, _))
      case Neq(a1, a2)             => handleDates(a1, a2, Neq(_, _))
      case Lt(a1, a2)              => handleDates(a1, a2, Lt(_, _))
      case Lte(a1, a2)             => handleDates(a1, a2, Lte(_, _))
      case Gt(a1, a2)              => handleDates(a1, a2, Gt(_, _))
      case Gte(a1, a2)             => handleDates(a1, a2, Gte(_, _))
      case v                       => v.embed
    }
  }

  def datetime(a1: T[N1QL], key: String, regex: Regex): T[N1QL] =
    Case(
      WhenThen(
        IsNotNull(SelectField(a1, str(key)).embed).embed,
        a1),
      WhenThen(
        RegexContains(a1, str(regex.regex)).embed,
        Obj(Map(str(key) -> a1)).embed)
    )(
      Else(Null[T[N1QL]].embed)
    ).embed

  def plan: AlgebraM[M, MapFunc[T, ?], T[N1QL]] = {
    // nullary
    case MF.Constant(v) =>
      Data[T[N1QL]](v.cata(QData.fromEJson)).embed.η[M]
    case MF.Undefined() =>
      Data[T[N1QL]](QData.NA).embed.η[M]

    // array
    case MF.Length(a1) =>
      IfMissingOrNull(
        Length(a1).embed,
        LengthArr(a1).embed,
        LengthObj(a1).embed,
        na
      ).embed.η[M]

    // date
    case MF.Date(a1) =>
      datetime(a1, DateKey, dateRegex.r).η[M]
    case MF.Time(a1) =>
      datetime(a1, TimeKey, timeRegex.r).η[M]
    case MF.Timestamp(a1) =>
      datetime(a1, TimestampKey, timestampRegex.r).η[M]
    case MF.Interval(a1) =>
      unimplementedP("Interval")
    case MF.TimeOfDay(a1) =>
      def fracZero(a1: T[N1QL]): T[N1QL] =
        Case(
          WhenThen(
            RegexContains(a1, str("[.]")).embed,
            Time(a1).embed),
          WhenThen(
            IsNotNull(a1).embed,
            Time(ConcatStr(a1, str(".000")).embed).embed)
        )(
          Else(na)
        ).embed

      def timeFromTS(a1: T[N1QL]): T[N1QL] =
        Time(SelectElem(
          Split(
            SelectElem(
              Split(SelectField(a1, str(TimestampKey)).embed,  str("T")).embed,
              int(1)).embed,
            str("Z")).embed,
          int(0)).embed).embed

      Case(
        WhenThen(SelectField(a1, str(DateKey)).embed, na),
        WhenThen(SelectField(a1, str(TimeKey)).embed, a1),
        WhenThen(SelectField(a1, str(TimestampKey)).embed, timeFromTS(a1))
      )(
        Else(fracZero(MillisToUTC(Millis(a1).embed, str("00:00:00.000").some).embed))
      ).embed.η[M]
    case MF.ToTimestamp(a1) =>
      Timestamp(MillisToUTC(a1, none).embed).embed.η[M]
    case MF.ExtractCentury(a1) =>
      Ceil(Div(extract(a1, "year"), int(100)).embed).embed.η[M]
    case MF.ExtractDayOfMonth(a1) =>
      extract(a1, "day").η[M]
    case MF.ExtractDecade(a1)         =>
      extract(a1, "decade").η[M]
    case MF.ExtractDayOfWeek(a1)      =>
      extract(a1, "day_of_week").η[M]
    case MF.ExtractDayOfYear(a1)      =>
      extract(a1, "day_of_year").η[M]
    case MF.ExtractEpoch(a1) =>
      Div(
        Millis(
          Case(
            WhenThen(
              SelectField(a1, str(DateKey)).embed,
              ConcatStr(
                SelectField(a1, str(DateKey)).embed,
                str("T00:00:00.000Z")).embed),
            WhenThen(
              SelectField(a1, str(TimeKey)).embed,
              na)
          )(
            Else(IfMissing(
              SelectField(a1, str(TimestampKey)).embed,
              a1).embed)
          ).embed
        ).embed,
        int(1000)
      ).embed.η[M]
    case MF.ExtractHour(a1) =>
      extract(a1, "hour").η[M]
    case MF.ExtractIsoDayOfWeek(a1) =>
      extract(a1, "iso_dow").η[M]
    case MF.ExtractIsoYear(a1)        =>
      extract(a1, "iso_year").η[M]
    case MF.ExtractMicroseconds(a1) =>
      Mult(
        Add(
          Mult(extract(a1, "second"), int(1000)).embed,
          extract(a1, "millisecond")).embed,
        int(1000)
      ).embed.η[M]
    case MF.ExtractMillennium(a1) =>
      Ceil(Div(extract(a1, "year"), int(1000)).embed).embed.η[M]
    case MF.ExtractMilliseconds(a1) =>
      Add(
        Mult(extract(a1, "second"), int(1000)).embed,
        extract(a1, "millisecond")
      ).embed.η[M]
    case MF.ExtractMinute(a1) =>
      extract(a1, "minute").η[M]
    case MF.ExtractMonth(a1) =>
      extract(a1, "month").η[M]
    case MF.ExtractQuarter(a1) =>
      extract(a1, "quarter").η[M]
    case MF.ExtractSecond(a1) =>
      Add(
        extract(a1, "second"),
        Div(extract(a1, "millisecond"), int(1000)).embed
      ).embed.η[M]
    case MF.ExtractTimezone(a1) =>
      extract(a1, "timezone").η[M]
    case MF.ExtractTimezoneHour(a1) =>
      extract(a1, "timezone_hour").η[M]
    case MF.ExtractTimezoneMinute(a1) =>
      extract(a1, "timezone_minute").η[M]
    case MF.ExtractWeek(a1) =>
      extract(a1, "iso_week").η[M]
    case MF.ExtractYear(a1) =>
      extract(a1, "year").η[M]
    case MF.Now() =>
      NowStr[T[N1QL]].embed.η[M]

    // math
    case MF.Negate(a1) =>
      Neg(a1).embed.η[M]
    case MF.Add(a1, a2) =>
      Add(a1, a2).embed.η[M]
    case MF.Multiply(a1, a2) =>
      Mult(a1, a2).embed.η[M]
    case MF.Subtract(a1, a2) =>
      Sub(a1, a2).embed.η[M]
    case MF.Divide(a1, a2) =>
      Div(a1, a2).embed.η[M]
    case MF.Modulo(a1, a2) =>
      Mod(a1, a2).embed.η[M]
    case MF.Power(a1, a2) =>
      Pow(a1, a2).embed.η[M]

    // relations
    case MF.Not(a1) =>
      Not(a1).embed.η[M]
    case MF.Eq(a1, a2) => a2.project match {
      case Data(QData.Null) => IsNull(unwrap(a1)).embed.η[M]
      case _                => rel(Eq(a1, a2)).η[M]
    }
    case MF.Neq(a1, a2) => a2.project match {
      case Data(QData.Null) => IsNotNull(unwrap(a1)).embed.η[M]
      case _                => rel(Neq(a1, a2)).η[M]
    }
    case MF.Lt(a1, a2) =>
      rel(Lt(a1, a2)).η[M]
    case MF.Lte(a1, a2) =>
      rel(Lte(a1, a2)).η[M]
    case MF.Gt(a1, a2) =>
      rel(Gt(a1, a2)).η[M]
    case MF.Gte(a1, a2) =>
      rel(Gte(a1, a2)).η[M]
    case MF.IfUndefined(a1, a2) =>
      IfMissing(a1, a2).embed.η[M]
    case MF.And(a1, a2) =>
      And(a1, a2).embed.η[M]
    case MF.Or(a1, a2) =>
      Or(a1, a2).embed.η[M]
    case MF.Between(a1, a2, a3) =>
      And(rel(Gte(a1, a2)), rel(Lte(a1, a3))).embed.η[M]
    case MF.Cond(cond, then_, else_) =>
      Case(
        WhenThen(cond, then_)
      )(
        Else(else_)
      ).embed.η[M]

    // set
    case MF.Within(a1, a2) =>
      ArrContains(a2, a1).embed.η[M]

    // string
    case MF.Lower(a1) =>
      Lower(a1).embed.η[M]
    case MF.Upper(a1) =>
      Upper(a1).embed.η[M]
    case MF.Bool(a1) =>
      Case(
        WhenThen(
          Eq(Lower(a1).embed, str("true")).embed,
          bool(true)),
        WhenThen(
          Eq(Lower(a1).embed, str("false")).embed,
          bool(false))
      )(
        Else(na)
      ).embed.η[M]
    // TODO: Handle large numbers across the board. Couchbase's number type truncates.
    case MF.Integer(a1) =>
      Case(
        WhenThen(
          Eq(
            ToNumber(a1).embed,
            Floor(ToNumber(a1).embed).embed).embed,
          ToNumber(a1).embed)
      )(
        Else(na)
      ).embed.η[M]
    case MF.Decimal(a1) =>
      ToNumber(a1).embed.η[M]
    case MF.Null(a1) =>
      Case(
        WhenThen(
          Eq(Lower(a1).embed, str("null")).embed,
          Null[T[N1QL]].embed)
      )(
        Else(na)
      ).embed.η[M]
    case MF.ToString(a1) =>
      IfNull(
        ToString(a1).embed,
        unwrap(a1),
        Case(
          WhenThen(
            Eq(Type(a1).embed, str("null")).embed ,
            str("null"))
        )(
          Else(a1)
        ).embed
      ).embed.η[M]
    case MF.Search(a1, a2, a3)    =>
      Case(
        WhenThen(
          a3,
          RegexContains(a1, ConcatStr(str("(?i)(?s)"), a2).embed).embed)
      )(
        Else(RegexContains(a1, ConcatStr(str("(?s)"), a2).embed).embed)
      ).embed.η[M]
    case MF.Substring(a1, a2, a3) =>
      Case(
        WhenThen(
          Lt(a2, int(0)).embed,
          str("")),
        WhenThen(
          Lt(a3, int(0)).embed,
          IfNull(
            Substr(a1, a2, None).embed,
            str("")).embed)
      )(
        Else(IfNull(
          Substr(
            a1,
            a2,
            Least(a3, Sub(Length(a1).embed, a2).embed).embed.some
          ).embed,
          str("")).embed)
      ).embed.η[M]

    // structural
    case MF.MakeArray(a1) =>
      Arr(List(a1)).embed.η[M]
    case MF.MakeMap(a1, a2) =>
      genId[T[N1QL], M] ∘ (id1 => selectOrElse(
        a2,
        Select(
          Value(true),
          ResultExpr(
            Obj(Map(
              ToString(a1).embed -> IfNull(id1.embed, na).embed
            )).embed,
            none
          ).wrapNel,
          Keyspace(a2, id1.some).some,
          unnest  = none,
          filter  = none,
          groupBy = none,
          orderBy = Nil).embed,
      Obj(Map(a1 -> a2)).embed))
    case MF.ConcatArrays(a1, a2) =>
      IfNull(
        ConcatStr(a1, a2).embed,
        ConcatArr(
          Case(
            WhenThen(
              IsString(a1).embed,
              Split(a1, str("")).embed)
          )(
            Else(a1)
          ).embed,
          Case(
            WhenThen(
              IsString(a2).embed,
              Split(a2, str("")).embed)
          )(
            Else(a2)
          ).embed).embed
      ).embed.η[M]
    case MF.ConcatMaps(a1, a2) =>
      ConcatObj(a1, a2).embed.η[M]
    case MF.ProjectField(a1, a2) =>
      genId[T[N1QL], M] ∘ (id1 => selectOrElse(
        a1,
        Select(
          Value(true),
          ResultExpr(SelectField(id1.embed, a2).embed, none).wrapNel,
          Keyspace(a1, id1.some).some,
          unnest  = none,
          filter  = none,
          groupBy = none,
          orderBy = Nil).embed,
        SelectField(a1, a2).embed))
    case MF.ProjectIndex(a1, a2) =>
      genId[T[N1QL], M] ∘ (id1 => selectOrElse(
        a1,
        Select(
          Value(true),
          ResultExpr(SelectElem(id1.embed, a2).embed, none).wrapNel,
          Keyspace(a1, id1.some).some,
          unnest  = none,
          filter  = none,
          groupBy = none,
          orderBy = Nil).embed,
        SelectElem(a1, a2).embed))
    case MF.DeleteField(a1, a2) =>
      ObjRemove(a1, a2).embed.η[M]

    case MF.Meta(a1) =>
      Meta(a1).embed.η[M]

    // helpers & QScript-specific
    case MF.Range(a1, a2) =>
      Slice(a2, Add(a1, int(1)).embed.some).embed.η[M]
    case MF.Guard(expr, typ, cont, _) =>
      def grd(f: T[N1QL] => T[N1QL], e: T[N1QL], c: T[N1QL]): T[N1QL] =
        Case(
          WhenThen(f(e), c))(
          Else(na)).embed

      def grdSel(f: T[N1QL] => T[N1QL]): M[T[N1QL]] =
        genId[T[N1QL], M] ∘ (id =>
          Select(
            Value(true),
            ResultExpr(grd(f, id.embed, id.embed), none).wrapNel,
            Keyspace(cont, id.some).some,
            unnest  = none,
            filter  = none,
            groupBy = none,
            orderBy = Nil).embed)

      def isArr(n: T[N1QL]): T[N1QL] = IsArr(n).embed
      def isObj(n: T[N1QL]): T[N1QL] = IsObj(n).embed

      (cont.project, typ) match {
        case (_: Select[T[N1QL]], _: QType.FlexArr) => grdSel(isArr)
        case (_: Select[T[N1QL]], _: QType.Obj)     => grdSel(isObj)
        case (_                 , _: QType.FlexArr) => grd(isArr, expr, cont).η[M]
        case (_                 , _: QType.Obj)     => grd(isObj, expr, cont).η[M]
        case _                                      => cont.η[M]
      }
  }
}
