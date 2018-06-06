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

package quasar.physical.couchbase.planner

import slamdata.Predef._
import quasar.DataCodec, DataCodec.PreciseKeys.{
  LocalDateKey,
  LocalDateTimeKey,
  LocalTimeKey,
  IntervalKey,
  OffsetDateKey,
  OffsetDateTimeKey,
  OffsetTimeKey
}
import quasar.{Data => QData, Type => QType}
import quasar.effect.NameGenerator
import quasar.fp._
import quasar.contrib.iota._
import quasar.physical.couchbase._, N1QL.{Eq, Split, _}, Case._, Select.{Value, _}
import quasar.fs.Planner.PlannerErrorME
import quasar.qscript, qscript.{MapFuncCore, MapFuncsCore => MF}
import quasar.std.StdLib.string.{dateRegex, timeRegex}
import quasar.time.TemporalPart, TemporalPart._

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
final class MapFuncCorePlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Applicative: NameGenerator: PlannerErrorME]
  extends Planner[T, F, MapFuncCore[T, ?]] {

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
      SelectField(a1, str(OffsetDateTimeKey)).embed,
      SelectField(a1, str(OffsetDateKey)).embed,
      SelectField(a1, str(OffsetTimeKey)).embed,
      SelectField(a1, str(LocalDateTimeKey)).embed,
      SelectField(a1, str(LocalDateKey)).embed,
      SelectField(a1, str(LocalTimeKey)).embed,
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

  def temporalTrunc(part: TemporalPart, a: T[N1QL]): T[N1QL] = ???

  def fracZero(a1: T[N1QL]): T[N1QL] =
    Case(
      WhenThen(RegexContains(a1, str("[.]")).embed, a1)
    )(
      Else(ConcatStr(a1, str(".000")).embed)
    ).embed

  def rel(op: N1QL[T[N1QL]]): T[N1QL] = {
    def handleDates(a1: T[N1QL], a2: T[N1QL], o: (T[N1QL], T[N1QL]) => N1QL[T[N1QL]]): T[N1QL] = {
      val a1Date = SelectField(a1, str(LocalDateKey)).embed
      val a2Date = SelectField(a2, str(LocalDateKey)).embed
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

  def plan: AlgebraM[F, MapFuncCore[T, ?], T[N1QL]] = {
    // nullary
    case MF.Constant(v) =>
      Data[T[N1QL]](v.cata(QData.fromEJson)).embed.η[F]
    case MF.Undefined() =>
      undefined.η[F]
    case MF.JoinSideName(n) =>
      unexpected(s"JoinSideName(${n.shows})")

    // array
    case MF.Length(a1) =>
      IfMissingOrNull(
        Length(a1).embed,
        LengthArr(a1).embed,
        LengthObj(a1).embed,
        undefined
      ).embed.η[F]

    // date
    case MF.LocalDate(a1) =>
      datetime(a1, LocalDateKey, dateRegex.r).η[F]
    case MF.LocalTime(a1) =>
      datetime(a1, LocalTimeKey, timeRegex.r).η[F]
    case MF.LocalDateTime(a1) =>
      ???
    case MF.OffsetDate(a1) =>
      ???
    case MF.OffsetTime(a1) =>
      ???
    case MF.OffsetDateTime(a1) =>
      ???
    case MF.Interval(a1) =>
      Case(
        WhenThen(IsNotNull(SelectField(a1, str(IntervalKey)).embed).embed, a1)
      )(
        Else(Null[T[N1QL]].embed)
      ).embed.η[F]
    case MF.TimeOfDay(a1) =>
      ???
    case MF.ToTimestamp(a1) =>
      Timestamp(MillisToUTC(a1, none).embed).embed.η[F]
    case MF.ToLocal(a1) =>
      ???
    case MF.TypeOf(a1) =>
      unimplemented("TypeOf")
    case MF.ToId(a1) =>
      unimplemented("ToId")
    case MF.ExtractCentury(a1) =>
      Ceil(Div(extract(a1, year), int(100)).embed).embed.η[F]
    case MF.ExtractDayOfMonth(a1) =>
      extract(a1, day).η[F]
    case MF.ExtractDecade(a1)         =>
      extract(a1, decade).η[F]
    case MF.ExtractDayOfWeek(a1)      =>
      extract(a1, dayOfWeek).η[F]
    case MF.ExtractDayOfYear(a1)      =>
      extract(a1, dayOfYear).η[F]
    case MF.ExtractEpoch(a1) =>
      ???
    case MF.ExtractHour(a1) =>
      extract(a1, hour).η[F]
    case MF.ExtractIsoDayOfWeek(a1) =>
      extract(a1, isoDow).η[F]
    case MF.ExtractIsoYear(a1)        =>
      extract(a1, isoYear).η[F]
    case MF.ExtractMicrosecond(a1) =>
      Mult(
        Add(
          Mult(extract(a1, second), int(1000)).embed,
          extract(a1, millisecond)).embed,
        int(1000)
      ).embed.η[F]
    case MF.ExtractMillennium(a1) =>
      Ceil(Div(extract(a1, year), int(1000)).embed).embed.η[F]
    case MF.ExtractMillisecond(a1) =>
      Add(
        Mult(extract(a1, second), int(1000)).embed,
        extract(a1, millisecond)
      ).embed.η[F]
    case MF.ExtractMinute(a1) =>
      extract(a1, minute).η[F]
    case MF.ExtractMonth(a1) =>
      extract(a1, month).η[F]
    case MF.ExtractQuarter(a1) =>
      extract(a1, quarter).η[F]
    case MF.ExtractSecond(a1) =>
      Add(
        extract(a1, second),
        Div(extract(a1, millisecond), int(1000)).embed
      ).embed.η[F]
    case MF.ExtractTimeZone(a1) =>
      extract(a1, timezone).η[F]
    case MF.ExtractTimeZoneHour(a1) =>
      extract(a1, timezoneHour).η[F]
    case MF.ExtractTimeZoneMinute(a1) =>
      extract(a1, timezoneMinute).η[F]
    case MF.ExtractWeek(a1) =>
      extract(a1, isoWeek).η[F]
    case MF.ExtractYear(a1) =>
      extract(a1, year).η[F]
    case MF.StartOfDay(a1) =>
      ???
    case MF.TemporalTrunc(Microsecond | Millisecond, a2) =>
      a2.η[F]
    case MF.TemporalTrunc(a1, a2) =>
      temporalTrunc(a1, a2).η[F]

    case MF.Now() =>
      NowStr[T[N1QL]].embed.η[F]
    case MF.NowTime() =>
      ???
    case MF.NowDate() =>
      ???
    case MF.CurrentTimeZone() =>
      ???

    // math
    case MF.Negate(a1) =>
      Neg(a1).embed.η[F]
    case MF.Add(a1, a2) =>
      Add(a1, a2).embed.η[F]
    case MF.Multiply(a1, a2) =>
      Mult(a1, a2).embed.η[F]
    case MF.Subtract(a1, a2) =>
      Sub(a1, a2).embed.η[F]
    case MF.Divide(a1, a2) =>
      Div(a1, a2).embed.η[F]
    case MF.Modulo(a1, a2) =>
      Mod(a1, a2).embed.η[F]
    case MF.Power(a1, a2) =>
      Pow(a1, a2).embed.η[F]

    // relations
    case MF.Not(a1) =>
      Not(a1).embed.η[F]
    case MF.Eq(a1, a2) => a2.project match {
      case Data(QData.Null) => IsNull(unwrap(a1)).embed.η[F]
      case _                => rel(Eq(a1, a2)).η[F]
    }
    case MF.Neq(a1, a2) => a2.project match {
      case Data(QData.Null) => IsNotNull(unwrap(a1)).embed.η[F]
      case _                => rel(Neq(a1, a2)).η[F]
    }
    case MF.Lt(a1, a2) =>
      rel(Lt(a1, a2)).η[F]
    case MF.Lte(a1, a2) =>
      rel(Lte(a1, a2)).η[F]
    case MF.Gt(a1, a2) =>
      rel(Gt(a1, a2)).η[F]
    case MF.Gte(a1, a2) =>
      rel(Gte(a1, a2)).η[F]
    case MF.IfUndefined(a1, a2) =>
      IfMissing(a1, a2).embed.η[F]
    case MF.And(a1, a2) =>
      And(a1, a2).embed.η[F]
    case MF.Or(a1, a2) =>
      Or(a1, a2).embed.η[F]
    case MF.Between(a1, a2, a3) =>
      And(rel(Gte(a1, a2)), rel(Lte(a1, a3))).embed.η[F]
    case MF.Cond(cond, then_, else_) =>
      Case(
        WhenThen(cond, then_)
      )(
        Else(else_)
      ).embed.η[F]

    // set
    case MF.Within(a1, a2) =>
      ArrContains(a2, a1).embed.η[F]

    // string
    case MF.Lower(a1) =>
      Lower(a1).embed.η[F]
    case MF.Upper(a1) =>
      Upper(a1).embed.η[F]
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
      ).embed.η[F]
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
      ).embed.η[F]
    case MF.Decimal(a1) =>
      ToNumber(a1).embed.η[F]
    case MF.Null(a1) =>
      Case(
        WhenThen(
          Eq(Lower(a1).embed, nullStr).embed,
          Null[T[N1QL]].embed)
      )(
        Else(undefined)
      ).embed.η[F]
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
      ).embed.η[F]
    case MF.Search(a1, a2, a3)    =>
      Case(
        WhenThen(
          a3,
          RegexContains(a1, ConcatStr(str("(?i)(?s)"), a2).embed).embed)
      )(
        Else(RegexContains(a1, ConcatStr(str("(?s)"), a2).embed).embed)
      ).embed.η[F]
    case MF.SetTimeZone(a1, a2)   =>
      ???
    case MF.SetTimeZoneMinute(a1, a2)   =>
      ???
    case MF.SetTimeZoneHour(a1, a2)   =>
      ???
    case MF.Split(a1, a2)         =>
      Split(a1, a2).embed.η[F]
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
      ).embed.η[F]

    // structural
    case MF.MakeArray(a1) =>
      Arr(List(a1)).embed.η[F]
    case MF.MakeMap(a1, a2) =>
      genId[T[N1QL], F] ∘ (id1 => selectOrElse(
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
        ConcatArr(a1, a2).embed.η[F],
        (genId[T[N1QL], F] ⊛ genId[T[N1QL], F]) { (id1, id2) =>
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
      ConcatObj(a1, a2).embed.η[F]
    case MF.ProjectKey(a1, a2) =>
      genId[T[N1QL], F] ∘ (id1 => selectOrElse(
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
      genId[T[N1QL], F] ∘ (id1 => selectOrElse(
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
    case MF.DeleteKey(a1, a2) =>
      ObjRemove(a1, a2).embed.η[F]

    case MF.Meta(a1) =>
      Meta(a1).embed.η[F]

    // helpers & QScript-specific
    case MF.Range(a1, a2) =>
      Slice(a2, Add(a1, int(1)).embed.some).embed.η[F]
    case MF.Guard(expr, typ, cont, _) =>
      def grd(f: T[N1QL] => T[N1QL], e: T[N1QL], c: T[N1QL]): T[N1QL] =
        Case(
          WhenThen(f(e), c))(
          Else(undefined)).embed

      def grdSel(f: T[N1QL] => T[N1QL]): F[T[N1QL]] =
        genId[T[N1QL], F] ∘ (id =>
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
        case (_                 , _: QType.FlexArr) => grd(isArr, expr, cont).η[F]
        case (_                 , _: QType.Obj)     => grd(isObj, expr, cont).η[F]
        case _                                      => cont.η[F]
      }
  }
}
