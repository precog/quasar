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

package quasar.physical.couchbase.planner

import quasar.Predef._
import quasar.DataCodec, DataCodec.Precise.{DateKey, IntervalKey, TimeKey, TimestampKey}
import quasar.{Data => QData, Type => QType, NameGenerator}
import quasar.contrib.scalaz.eitherT._
import quasar.fp._
import quasar.physical.couchbase._, N1QL.{Eq, Split, _}, Case._, Select.{Value, _}
import quasar.qscript, qscript.{MapFunc, MapFuncs => MF}
import quasar.std.StdLib.string.{dateRegex, timeRegex, timestampRegex}
import quasar.std.DateLib.TemporalPart, TemporalPart._

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

final class MapFuncPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: NameGenerator]
  extends Planner[T, F, MapFunc[T, ?]] {

  def str(s: String): T[N1QL]   = Data[T[N1QL]](QData.Str(s)).embed
  def int(i: Int): T[N1QL]      = Data[T[N1QL]](QData.Int(i)).embed
  def bool(b: Boolean): T[N1QL] = Data[T[N1QL]](QData.Bool(b)).embed
  val undefined: T[N1QL]        = Null[T[N1QL]]().embed

  val emptyStr       = str("")
  val nullStr        = str("null")
  val dateTimeDelim  = str("T")
  val zeroUTC        = str("Z")
  val dateFillPrefix = str("0000-01-01T")
  val zeroTime       = str("00:00:00.000")
  val zeroTimeSuffix = str("T00:00:00.000Z")
  val microsecond    = str("microsecond")
  val millisecond    = str("millisecond")
  val second         = str("second")
  val minute         = str("minute")
  val hour           = str("hour")
  val day            = str("day")
  val week           = str("week")
  val month          = str("month")
  val quarter        = str("quarter")
  val year           = str("year")
  val decade         = str("decade")
  val century        = str("century")
  val millennium     = str("millennium")
  val dayOfWeek      = str("day_of_week")
  val dayOfYear      = str("day_of_year")
  val isoDow         = str("iso_dow")
  val isoWeek        = str("iso_week")
  val isoYear        = str("iso_year")
  val timezone       = str("timezone")
  val timezoneHour   = str("timezone_hour")
  val timezoneMinute = str("timezone_minute")

  def unwrap(a1: T[N1QL]): T[N1QL] =
    IfMissing(
      SelectField(a1, str(DateKey)).embed,
      SelectField(a1, str(TimeKey)).embed,
      SelectField(a1, str(TimestampKey)).embed,
      SelectField(a1, str(IntervalKey)).embed,
      a1).embed

  def extract(a1: T[N1QL], part: T[N1QL]): T[N1QL] =
    DatePartStr(unwrap(a1), part).embed

  def temporalPart(part: TemporalPart): T[N1QL] = {
    import TemporalPart._

    part match {
      case Century     => century
      case Day         => day
      case Decade      => decade
      case Hour        => hour
      case Microsecond => microsecond
      case Millennium  => millennium
      case Millisecond => millisecond
      case Minute      => minute
      case Month       => month
      case Quarter     => quarter
      case Second      => second
      case Week        => week
      case Year        => year
    }
  }

  def trunc(part: TemporalPart, dt: T[N1QL]) = part match {
    case Week =>
      DateTruncStr(
        DateAddStr(
          dt,
          Neg(
            Case(
              WhenThen(Eq(DatePartStr(dt, isoDow).embed, int(0)).embed, int(0))
            )(
              Else(Sub(DatePartStr(dt, isoDow).embed, int(1)).embed)
            ).embed).embed,
          day).embed,
        day).embed
    case _ =>
      DateTruncStr(dt, temporalPart(part)).embed
  }

  def temporalTrunc(part: TemporalPart, a: T[N1QL]): T[N1QL] =
    Case(
      WhenThen(
        SelectField(a, str(DateKey)).embed,
        Date(SelectElem(
          Split(
            trunc(part, ConcatStr(SelectField(a, str(DateKey)).embed, zeroTimeSuffix).embed), dateTimeDelim).embed,
          int(0)).embed).embed),
      WhenThen(
        SelectField(a, str(TimeKey)).embed,
        Time((part === Week).fold(
          zeroTime,
          SelectElem(
            Split(
              SelectElem(
                Split(
                  trunc(part, ConcatStr(
                    ConcatStr(dateFillPrefix, SelectField(a, str(TimeKey)).embed).embed,
                    zeroUTC).embed),
                  dateTimeDelim).embed,
                int(1)).embed,
              zeroUTC).embed,
            int(0)).embed)).embed),
      WhenThen(
        SelectField(a, str(TimestampKey)).embed,
        Timestamp(trunc(part, SelectField(a, str(TimestampKey)).embed)).embed)
    )(
      Else(DateTruncStr(a, temporalPart(part)).embed)
    ).embed

  def fracZero(a1: T[N1QL]): T[N1QL] =
    Case(
      WhenThen(RegexContains(a1, str("[.]")).embed, a1)
    )(
      Else(ConcatStr(a1, str(".000")).embed)
    ).embed

  def rel(op: N1QL[T[N1QL]]): T[N1QL] = {
    def handleDates(a1: T[N1QL], a2: T[N1QL], o: (T[N1QL], T[N1QL]) => N1QL[T[N1QL]]): T[N1QL] = {
      val a1Date = SelectField(a1, str(DateKey)).embed
      val a2Date = SelectField(a2, str(DateKey)).embed
      val a1U = unwrap(a1)
      val a2U = unwrap(a2)
      IfMissingOrNull(
        o(DateDiffStr(a1Date, a2Date, day).embed, int(0)).embed,
        o(DateDiffStr(a1U, a2U, millisecond).embed, int(0)).embed,
        o(a1, a2).embed
      ).embed
    }

    op match {
      case Eq(a1, a2)  => handleDates(a1, a2, Eq(_, _))
      case Neq(a1, a2) => handleDates(a1, a2, Neq(_, _))
      case Lt(a1, a2)  => handleDates(a1, a2, Lt(_, _))
      case Lte(a1, a2) => handleDates(a1, a2, Lte(_, _))
      case Gt(a1, a2)  => handleDates(a1, a2, Gt(_, _))
      case Gte(a1, a2) => handleDates(a1, a2, Gte(_, _))
      case v           => v.embed
    }
  }

  def datetime(a1: T[N1QL], key: String, regex: Regex): T[N1QL] =
    Case(
      WhenThen(
        IsNotNull(SelectField(a1, str(key)).embed).embed,
        a1),
      WhenThen(
        RegexContains(a1, str(regex.regex)).embed,
        Obj(List(str(key) -> a1)).embed)
    )(
      Else(Null[T[N1QL]].embed)
    ).embed

  def plan: AlgebraM[M, MapFunc[T, ?], T[N1QL]] = {
    // nullary
    case MF.Constant(v) =>
      Data[T[N1QL]](v.cata(QData.fromEJson)).embed.η[M]
    case MF.Undefined() =>
      undefined.η[M]

    // array
    case MF.Length(a1) =>
      IfMissingOrNull(
        Length(a1).embed,
        LengthArr(a1).embed,
        LengthObj(a1).embed,
        undefined
      ).embed.η[M]

    // date
    case MF.Date(a1) =>
      datetime(a1, DateKey, dateRegex.r).η[M]
    case MF.Time(a1) =>
      datetime(a1, TimeKey, timeRegex.r).η[M]
    case MF.Timestamp(a1) =>
      datetime(a1, TimestampKey, timestampRegex.r).η[M]
    case MF.Interval(a1) =>
      Case(
        WhenThen(IsNotNull(SelectField(a1, str(IntervalKey)).embed).embed, a1)
      )(
        Else(Null[T[N1QL]].embed)
      ).embed.η[M]
    case MF.TimeOfDay(a1) =>
      def fracZeroTime(a1: T[N1QL]): T[N1QL] = {
        val fz = fracZero(a1)
        Case(
          WhenThen(IsNotNull(fz).embed, Time(fz).embed)
        )(
          Else(undefined)
        ).embed
      }

      def timeFromTS(a1: T[N1QL]): T[N1QL] =
        Time(SelectElem(
          Split(
            SelectElem(
              Split(SelectField(a1, str(TimestampKey)).embed,  dateTimeDelim).embed,
              int(1)).embed,
            zeroUTC).embed,
          int(0)).embed).embed

      Case(
        WhenThen(SelectField(a1, str(DateKey)).embed, undefined),
        WhenThen(SelectField(a1, str(TimeKey)).embed, a1),
        WhenThen(SelectField(a1, str(TimestampKey)).embed, timeFromTS(a1))
      )(
        Else(fracZeroTime(MillisToUTC(Millis(a1).embed, zeroTime.some).embed))
      ).embed.η[M]
    case MF.ToTimestamp(a1) =>
      Timestamp(MillisToUTC(a1, none).embed).embed.η[M]
    case MF.TypeOf(a1) =>
      unimplementedP("TypeOf")
    case MF.ExtractCentury(a1) =>
      Ceil(Div(extract(a1, year), int(100)).embed).embed.η[M]
    case MF.ExtractDayOfMonth(a1) =>
      extract(a1, day).η[M]
    case MF.ExtractDecade(a1)         =>
      extract(a1, decade).η[M]
    case MF.ExtractDayOfWeek(a1)      =>
      extract(a1, dayOfWeek).η[M]
    case MF.ExtractDayOfYear(a1)      =>
      extract(a1, dayOfYear).η[M]
    case MF.ExtractEpoch(a1) =>
      Div(
        Millis(
          Case(
            WhenThen(
              SelectField(a1, str(DateKey)).embed,
              ConcatStr(
                SelectField(a1, str(DateKey)).embed,
                zeroTimeSuffix).embed),
            WhenThen(
              SelectField(a1, str(TimeKey)).embed,
              undefined)
          )(
            Else(IfMissing(
              SelectField(a1, str(TimestampKey)).embed,
              a1).embed)
          ).embed
        ).embed,
        int(1000)
      ).embed.η[M]
    case MF.ExtractHour(a1) =>
      extract(a1, hour).η[M]
    case MF.ExtractIsoDayOfWeek(a1) =>
      extract(a1, isoDow).η[M]
    case MF.ExtractIsoYear(a1)        =>
      extract(a1, isoYear).η[M]
    case MF.ExtractMicroseconds(a1) =>
      Mult(
        Add(
          Mult(extract(a1, second), int(1000)).embed,
          extract(a1, millisecond)).embed,
        int(1000)
      ).embed.η[M]
    case MF.ExtractMillennium(a1) =>
      Ceil(Div(extract(a1, year), int(1000)).embed).embed.η[M]
    case MF.ExtractMilliseconds(a1) =>
      Add(
        Mult(extract(a1, second), int(1000)).embed,
        extract(a1, millisecond)
      ).embed.η[M]
    case MF.ExtractMinute(a1) =>
      extract(a1, minute).η[M]
    case MF.ExtractMonth(a1) =>
      extract(a1, month).η[M]
    case MF.ExtractQuarter(a1) =>
      extract(a1, quarter).η[M]
    case MF.ExtractSecond(a1) =>
      Add(
        extract(a1, second),
        Div(extract(a1, millisecond), int(1000)).embed
      ).embed.η[M]
    case MF.ExtractTimezone(a1) =>
      extract(a1, timezone).η[M]
    case MF.ExtractTimezoneHour(a1) =>
      extract(a1, timezoneHour).η[M]
    case MF.ExtractTimezoneMinute(a1) =>
      extract(a1, timezoneMinute).η[M]
    case MF.ExtractWeek(a1) =>
      extract(a1, isoWeek).η[M]
    case MF.ExtractYear(a1) =>
      extract(a1, year).η[M]
    case MF.StartOfDay(a1) =>
        Case(
          WhenThen(
            SelectField(a1, str(TimestampKey)).embed,
            Timestamp(trunc(Day, SelectField(a1, str(TimestampKey)).embed)).embed),
          WhenThen(
            SelectField(a1, str(DateKey)).embed,
            Timestamp(trunc(Day, ConcatStr(SelectField(a1, str(DateKey)).embed, zeroTimeSuffix).embed)).embed)
        )(
          Else(undefined)
        ).embed.η[M]
    case MF.TemporalTrunc(Microsecond | Millisecond, a2) =>
      a2.η[M]
    case MF.TemporalTrunc(a1, a2) =>
      temporalTrunc(a1, a2).η[M]
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
        Else(undefined)
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
        Else(undefined)
      ).embed.η[M]
    case MF.Decimal(a1) =>
      ToNumber(a1).embed.η[M]
    case MF.Null(a1) =>
      Case(
        WhenThen(
          Eq(Lower(a1).embed, nullStr).embed,
          Null[T[N1QL]].embed)
      )(
        Else(undefined)
      ).embed.η[M]
    case MF.ToString(a1) =>
      IfNull(
        ToString(a1).embed,
        unwrap(a1),
        Case(
          WhenThen(
            Eq(Type(a1).embed, nullStr).embed ,
            nullStr)
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
          emptyStr),
        WhenThen(
          Lt(a3, int(0)).embed,
          IfNull(
            Substr(a1, a2, None).embed,
            emptyStr).embed)
      )(
        Else(IfNull(
          Substr(
            a1,
            a2,
            Least(a3, Sub(Length(a1).embed, a2).embed).embed.some
          ).embed,
          emptyStr).embed)
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
            Obj(List(
              ToString(a1).embed -> IfNull(id1.embed, undefined).embed
            )).embed,
            none
          ).wrapNel,
          Keyspace(a2, id1.some).some,
          join    = none,
          unnest  = none,
          let     = nil,
          filter  = none,
          groupBy = none,
          orderBy = nil).embed,
      Obj(List(a1 -> a2)).embed))
    case MF.ConcatArrays(a1, a2) =>
      def containsAgg(v: T[N1QL]): Boolean = v.cataM[Option, Unit] {
        case Avg(_) | Count(_) | Max(_) | Min(_) | Sum(_) | ArrAgg(_) => none
        case _                                                        => ().some
      }.isEmpty

      (containsAgg(a1) || containsAgg(a2)).fold(
        IfNull(
          ConcatStr(a1, a2).embed,
          ConcatArr(
            Case(
              WhenThen(
                IsString(a1).embed,
                Split(a1, emptyStr).embed)
            )(
              Else(a1)
            ).embed,
            Case(
              WhenThen(
                IsString(a2).embed,
                Split(a2, emptyStr).embed)
            )(
              Else(a2)
            ).embed).embed
        ).embed.η[M],
        (genId[T[N1QL], M] ⊛ genId[T[N1QL], M]) { (id1, id2) =>
          SelectElem(
            Select(
              Value(true),
              ResultExpr(
                IfNull(
                  ConcatStr(id1.embed, id2.embed).embed,
                  ConcatArr(Split(id1.embed, emptyStr).embed, id2.embed).embed,
                  ConcatArr(id1.embed, Split(id2.embed, emptyStr).embed).embed,
                  ConcatArr(id1.embed, id2.embed).embed).embed,
                none
              ).wrapNel,
              keyspace = none,
              join     = none,
              unnest   = none,
              List(Binding(id1, a1), Binding(id2, a2)),
              filter   = none,
              groupBy  = none,
              orderBy  = nil).embed,
            int(0)).embed
        })
    case MF.ConcatMaps(a1, a2) =>
      ConcatObj(a1, a2).embed.η[M]
    case MF.ProjectField(a1, a2) =>
      genId[T[N1QL], M] ∘ (id1 => selectOrElse(
        a1,
        Select(
          Value(true),
          ResultExpr(SelectField(id1.embed, a2).embed, none).wrapNel,
          Keyspace(a1, id1.some).some,
          join    = none,
          unnest  = none,
          let     = nil,
          filter  = none,
          groupBy = none,
          orderBy = nil).embed,
        SelectField(a1, a2).embed))
    case MF.ProjectIndex(a1, a2) =>
      genId[T[N1QL], M] ∘ (id1 => selectOrElse(
        a1,
        Select(
          Value(true),
          ResultExpr(SelectElem(id1.embed, a2).embed, none).wrapNel,
          Keyspace(a1, id1.some).some,
          join    = none,
          unnest  = none,
          let     = nil,
          filter  = none,
          groupBy = none,
          orderBy = nil).embed,
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
          Else(undefined)).embed

      def grdSel(f: T[N1QL] => T[N1QL]): M[T[N1QL]] =
        genId[T[N1QL], M] ∘ (id =>
          Select(
            Value(true),
            ResultExpr(grd(f, id.embed, id.embed), none).wrapNel,
            Keyspace(cont, id.some).some,
            join    = none,
            unnest  = none,
            let     = nil,
            filter  = none,
            groupBy = none,
            orderBy = nil).embed)

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
