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

package quasar.physical.sparkcore.fs

import slamdata.Predef.{Eq => _, _}
import quasar.{datetime, Data, DateTimeInterval, TemporalPart}
import quasar.Planner._
import quasar.common.PrimaryType
import quasar.fp.ski._
import quasar.qscript._, MapFuncsCore._
import quasar.std.{DateLib, StringLib}

import java.time.{LocalTime=>JLocalTime}, java.time.ZoneOffset.UTC
import java.time.{OffsetDateTime=>JOffsetDateTime, Instant}
import scala.math
import scala.util.matching.Regex

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{Divide => _,Split => _, _}, Scalaz._
import quasar.DataDateTimeExtractors._

object CoreMap extends Serializable {

  private val undefined = Data.NA

  def changeFreeMap[T[_[_]]: BirecursiveT](f: FreeMap[T])
      : PlannerError \/ (Data => Data) =
    f.cataM(interpretM(κ(ι[Data].right[PlannerError]), change[T, Data]))

  def changeJoinFunc[T[_[_]]: BirecursiveT](f: JoinFunc[T])
      : PlannerError \/ ((Data, Data) => Data) =
    f.cataM(interpretM[PlannerError \/ ?, MapFunc[T, ?], JoinSide, ((Data, Data)) => Data](
      (js: JoinSide) => (js match {
        case LeftSide  => (_: (Data, Data))._1
        case RightSide => (_: (Data, Data))._2
      }).right,
      change[T, (Data, Data)])).map(f => (l: Data, r: Data) => f((l, r)))

  def changeReduceFunc[T[_[_]]: BirecursiveT](f: Free[MapFunc[T, ?], ReduceIndex])
      : PlannerError \/ ((List[Data], List[Data]) => Data) =
    f.cataM(interpretM(
      _.idx.fold[((List[Data], List[Data])) => Data](i => _._1(i), i => _._2(i)).right,
      change[T, (List[Data], List[Data])])).map(f => (l: List[Data], r: List[Data]) => f((l, r)))

  def change[T[_[_]]: BirecursiveT, A]
      : AlgebraM[PlannerError \/ ?, MapFunc[T, ?], A => Data] =
    _.run.fold(changeCore, changeDerived)

  def changeDerived[T[_[_]]: BirecursiveT, A]
      : AlgebraM[PlannerError \/ ?, MapFuncDerived[T, ?], A => Data] =
    ExpandMapFunc.expand(changeCore, κ(None))

  def changeCore[T[_[_]]: RecursiveT, A]
      : AlgebraM[PlannerError \/ ?, MapFuncCore[T, ?], A => Data] = {
    case Constant(f) => κ(f.cata(Data.fromEJson)).right
    case Undefined() => κ(undefined).right
    case JoinSideName(n) => UnexpectedJoinSide(n).left

    case Length(f) => (f >>> {
      case Data.Str(v) => Data.Int(v.length)
      case Data.Arr(v) => Data.Int(v.size)
      case _ => undefined
    }).right

    case OffsetDate(f) => (f >>> {
      case Data.Str(v) => DateLib.parseOffsetDate(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case OffsetTime(f) => (f >>> {
      case Data.Str(v) => DateLib.parseOffsetTime(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case OffsetDateTime(f) => (f >>> {
      case Data.Str(v) => DateLib.parseOffsetDateTime(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case LocalDate(f) => (f >>> {
      case Data.Str(v) => DateLib.parseLocalDate(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case LocalTime(f) => (f >>> {
      case Data.Str(v) => DateLib.parseLocalTime(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case LocalDateTime(f) => (f >>> {
      case Data.Str(v) => DateLib.parseLocalDateTime(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case Interval(f) => (f >>> {
      case Data.Str(v) => DateLib.parseInterval(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case StartOfDay(f) => (f >>> {
      case CanAddTime(g) => g(JLocalTime.MIN)
      case _ => undefined
    }).right
    case TemporalTrunc(p, f) => (f >>> (temporalTrunc(p, _))).right
    case TimeOfDay(f) => (f >>> {
      case Data.LocalDateTime(v) => Data.LocalTime(v.toLocalTime)
      case Data.OffsetDateTime(v) => Data.OffsetTime(v.toOffsetTime)
      case _ => undefined
    }).right
    case ToTimestamp(f) => (f >>> {
      case Data.Int(epoch) => Data.OffsetDateTime(Instant.ofEpochMilli(epoch.toLong).atOffset(UTC))
      case _ => undefined
    }).right
    case ExtractCentury(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractCentury(i.pos))
      case _ => undefined
    }).right
    case ExtractDayOfMonth(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractDayOfMonth(i.pos))
      case _ => undefined
    }).right
    case ExtractDecade(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractDecade(i.pos))
      case _ => undefined
    }).right
    case ExtractDayOfWeek(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractDayOfWeek(i.pos))
      case _ => undefined
    }).right
    case ExtractDayOfYear(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractDayOfYear(i.pos))
      case _ => undefined
    }).right
    case ExtractEpoch(f) => (f >>> {
      case Data.OffsetDateTime(v) => Data.Dec(v.toEpochSecond())
      case _ => undefined
    }).right
    case ExtractHour(f) => (f >>> {
      case CanLensTime(i) => Data.Int(datetime.extractHour(i.pos))
      case _ => undefined
    }).right
    case ExtractIsoDayOfWeek(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractIsoDayOfWeek(i.pos))
      case _ => undefined
    }).right
    case ExtractIsoYear(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractIsoYear(i.pos))
      case _ => undefined
    }).right
    case ExtractMicrosecond(f) => (f >>> {
      case CanLensTime(i) => Data.Int(datetime.extractMicrosecond(i.pos))
      case _ => undefined
    }).right
    case ExtractMillennium(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractMillennium(i.pos))
      case _ => undefined
    }).right
    case ExtractMillisecond(f) => (f >>> {
      case CanLensTime(i) => Data.Int(datetime.extractMillisecond(i.pos))
      case _ => undefined
    }).right
    case ExtractMinute(f) => (f >>> {
      case CanLensTime(i) => Data.Int(datetime.extractMinute(i.pos))
      case _ => undefined
    }).right
    case ExtractMonth(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractMonth(i.pos))
      case _ => undefined
    }).right
    case ExtractQuarter(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractQuarter(i.pos))
      case _ => undefined
    }).right
    case ExtractSecond(f) => (f >>> {
      case CanLensTime(i) =>
        Data.Dec(datetime.extractSecond(i.pos))
      case _ => undefined
    }).right
    case ExtractWeek(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractWeek(i.pos))
      case _ => undefined
    }).right
    case ExtractYear(f) => (f >>> {
      case CanLensDate(i) => Data.Int(datetime.extractYear(i.pos))
      case _ => undefined
    }).right
    case ExtractTimeZone(f) => (f >>> {
      case CanLensTimeZone(i) => Data.Int(datetime.extractTimeZone(i.pos))
      case _ => undefined
    }).right
    case ExtractTimeZoneMinute(f) => (f >>> {
      case CanLensTimeZone(i) => Data.Int(datetime.extractTimeZoneMinute(i.pos))
      case _ => undefined
    }).right
    case ExtractTimeZoneHour(f) => (f >>> {
      case CanLensTimeZone(i) => Data.Int(datetime.extractTimeZoneHour(i.pos))
      case _ => undefined
    }).right

    case Now() => κ(Data.OffsetDateTime(JOffsetDateTime.now())).right

    case Negate(f) => (f >>> {
      case Data.Int(v) => Data.Int(-v)
      case Data.Dec(v) => Data.Dec(-v)
      case Data.Interval(v) => Data.Interval(v.multiply(-1))
      case _ => undefined
    }).right
    case Add(f1, f2) => ((x: A) => add(f1(x), f2(x))).right
    case Multiply(f1, f2) => ((x: A) => multiply(f1(x), f2(x))).right
    case Subtract(f1, f2) => ((x: A) => subtract(f1(x), f2(x))).right
    case Divide(f1, f2) => ((x: A) => divide(f1(x), f2(x))).right
    case Modulo(f1, f2) => ((x: A) => modulo(f1(x), f2(x))).right
    case Power(f1, f2) => ((x: A) => power(f1(x), f2(x))).right

    case Not(f) => (f >>> {
      case Data.Bool(b) => Data.Bool(!b)
      case _ => undefined
    }).right
    case Eq(f1, f2) => ((x: A) => Data.Bool(f1(x) === f2(x))).right
    case Neq(f1, f2) => ((x: A) => Data.Bool(f1(x) =/= f2(x))).right
    case Lt(f1, f2) => ((x: A) => lt(f1(x), f2(x))).right
    case Lte(f1, f2) => ((x: A) => lte(f1(x), f2(x))).right
    case Gt(f1, f2) => ((x: A) => gt(f1(x), f2(x))).right
    case Gte(f1, f2) => ((x: A) => gte(f1(x), f2(x))).right
    case IfUndefined(f1, f2) => ((x: A) => f1(x) match {
      case Data.NA => f2(x)
      case d => d
    }).right
    case And(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Bool(a), Data.Bool(b)) => Data.Bool(a && b)
      case _ => undefined
    }).right
    case Or(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Bool(a), Data.Bool(b)) => Data.Bool(a || b)
      case (Data.NA, b) => b
      case (a, Data.NA) => a
      case _ => undefined
    }).right
    case Between(f1, f2, f3) => ((x: A) => between(f1(x), f2(x), f3(x))).right
    case Cond(fCond, fThen, fElse) => ((x: A) => fCond(x) match {
      case Data.Bool(true) => fThen(x)
      case Data.Bool(false) => fElse(x)
      case _ => undefined
    }).right

    case Within(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (d, Data.Arr(list)) => Data.Bool(list.contains(d))
      case _ => undefined
    }).right

    case Lower(f) => (f >>> {
      case Data.Str(a) => Data.Str(a.toLowerCase())
      case _ => undefined
    }).right
    case Upper(f) => (f >>> {
      case Data.Str(a) => Data.Str(a.toUpperCase())
      case _ => undefined
    }).right
    case Bool(f) => (f >>> {
      case Data.Str("true") => Data.Bool(true)
      case Data.Str("false") => Data.Bool(false)
      case _ => undefined
    }).right
    case Integer(f) => (f >>> {
      case Data.Str(a) => \/.fromTryCatchNonFatal(Data.Int(BigInt(a))).fold(κ(Data.NA), ι)
      case _ => undefined
    }).right
    case Decimal(f) => (f >>> {
      case Data.Str(a) => \/.fromTryCatchNonFatal(Data.Dec(BigDecimal(a))).fold(κ(Data.NA), ι)
      case _ => undefined
    }).right
    case Null(f) => (f >>> {
      case Data.Str("null") => Data.Null
      case _ => undefined
    }).right
    case ToString(f) => (f >>> toStringFunc).right
    case Search(fStr, fPattern, fInsen) =>
      ((x: A) => search(fStr(x), fPattern(x), fInsen(x))).right
    case Substring(fStr, fFrom, fCount) =>
      ((x: A) => substring(fStr(x), fFrom(x), fCount(x))).right
     case Split(fStr, fDelim) =>
       ((x: A) => split(fStr(x), fDelim(x))).right
    case MakeArray(f) => (f >>> ((x: Data) => Data.Arr(List(x)))).right
    case MakeMap(fK, fV) => ((x: A) => (fK(x), fV(x)) match {
      case (Data.Str(k), v) => Data.Obj(ListMap(k -> v))
      case _ => undefined
    }).right
    case ConcatArrays(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Arr(l1), Data.Arr(l2)) => Data.Arr(l1 ++ l2)
      case (Data.Str(s1), Data.Str(s2)) => Data.Str(s1 ++ s2)
      case _ => undefined
    }).right
    case ConcatMaps(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Obj(m1), Data.Obj(m2)) => Data.Obj(m1 ++ m2)
      case _ => undefined
    }).right
    case ProjectIndex(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Arr(list), Data.Int(index)) =>
        if(index >= 0 && index < list.size) list(index.toInt) else undefined
      case _ => undefined
    }).right
    case ProjectKey(fSrc, fKey) => ((x: A) => (fSrc(x), fKey(x)) match {
      case (Data.Obj(m), Data.Str(key)) if m.isDefinedAt(key) => m(key)
      case _ => undefined
    }).right
    case DeleteKey(fSrc, fField) =>  ((x: A) => (fSrc(x), fField(x)) match {
      case (Data.Obj(m), Data.Str(key)) if m.isDefinedAt(key) => Data.Obj(m - key)
      case (obj @ Data.Obj(_), _) => obj
      case _ => undefined
    }).right
    case Range(fFrom, fTo) => ((x: A) => (fFrom(x), fTo(x)) match {
      case (Data.Int(a), Data.Int(b)) if(a <= b) => Data.Arr((a to b).map(Data.Int(_)).toList)
    }).right
    case Guard(f1, fPattern, f2, ff3) => f2.right
    case TypeOf(f) =>
      (f >>> ((d: Data) => d.dataType.toPrimaryType.fold(Data.NA : Data)(p => Data.Str(PrimaryType.name(p))))).right[PlannerError]
    case _ => InternalError.fromMsg("not implemented").left
  }

  private def add(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Int(a + b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) + b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a + BigDecimal(b))
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a + b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Interval(a.plus(b))
    case (Data.Interval(b), CanLensDateTime(a)) => a.peeks(b.addTo)
    case (Data.Interval(DateTimeInterval.DateLike(b)), CanLensDate(a)) => a.peeks(_.plus(b))
    case (Data.Interval(DateTimeInterval.TimeLike(b)), CanLensTime(a)) => a.peeks(_.plus(b))
    case (CanLensDateTime(a), Data.Interval(b)) => a.peeks(b.addTo)
    case (CanLensDate(a), Data.Interval(DateTimeInterval.DateLike(b))) => a.peeks(_.plus(b))
    case (CanLensTime(a), Data.Interval(DateTimeInterval.TimeLike(b))) => a.peeks(_.plus(b))
    case _ => undefined
  }

  private def subtract(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a - b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) - b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a - BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a - b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Interval(a.minus(b))
    case (CanLensDateTime(a), Data.Interval(b)) => a.peeks(b.subtractFrom)
    case (CanLensDate(a), Data.Interval(DateTimeInterval.DateLike(b))) => a.peeks(_.minus(b))
    case (CanLensTime(a), Data.Interval(DateTimeInterval.TimeLike(b))) => a.peeks(_.minus(b))
    case _ => undefined
  }

  private def multiply(d1: Data, d2: Data): Data = (d1, d2)  match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a * b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) * b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a * BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a * b)
    case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multiply(b.toInt))
    case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multiply(b.toInt))
    case (Data.Dec(a), Data.Interval(b)) => Data.Interval(b.multiply(a.toInt))
    case (Data.Int(a), Data.Interval(b)) => Data.Interval(b.multiply(a.toInt))
    case _ => undefined
  }

  private def divide(d1: Data, d2: Data): Data = (d1, d2) match {
    case (_, Data.Dec(b)) if b === BigDecimal(0) => Data.NA
    case (_, Data.Int(b)) if b === 0 => Data.NA
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a(BigDecimal.defaultMathContext) / b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) / b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a(BigDecimal.defaultMathContext) / BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Dec(BigDecimal(a) / BigDecimal(b))
    case _ => undefined
  }

  private def modulo(d1: Data, d2: Data) = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Int(a % b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a).remainder(b))
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a.remainder(BigDecimal(b)))
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a.remainder(b))
    case _ => undefined
  }

  // TODO we loose precision here, consider using https://github.com/non/spire/
  private def power(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Dec(math.pow(a.toDouble, b.toDouble))
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(math.pow(a.toDouble, b.toDouble))
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(math.pow(a.toDouble, b.toDouble))
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(math.pow(a.toDouble, b.toDouble))
    case _ => undefined
  }

  private def lt(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Bool(a < b)
    case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a < b)
    case (Data.Int(a), Data.Dec(b)) => Data.Bool(BigDecimal(a) < b)
    case (Data.Dec(a), Data.Int(b)) => Data.Bool(a < BigDecimal(b))
    case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.OffsetDateTime(a), Data.OffsetDateTime(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.OffsetDate(a), Data.OffsetDate(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.OffsetTime(a), Data.OffsetTime(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.LocalDateTime(a), Data.LocalDateTime(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.LocalDate(a), Data.LocalDate(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.LocalTime(a), Data.LocalTime(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
      case (false, true) => Data.Bool(true)
      case _ => Data.Bool(false)
    }
    case _ => undefined
  }

  private def lte(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Bool(a <= b)
    case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a <= b)
    case (Data.Int(a), Data.Dec(b)) => Data.Bool(BigDecimal(a) <= b)
    case (Data.Dec(a), Data.Int(b)) => Data.Bool(a <= BigDecimal(b))
    case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.OffsetDateTime(a), Data.OffsetDateTime(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.OffsetDate(a), Data.OffsetDate(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.OffsetTime(a), Data.OffsetTime(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.LocalDateTime(a), Data.LocalDateTime(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.LocalDate(a), Data.LocalDate(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.LocalTime(a), Data.LocalTime(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
      case (true, false) => Data.Bool(false)
      case _ => Data.Bool(true)
    }
    case _ => undefined
  }

  private def gt(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Bool(a > b)
    case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a > b)
    case (Data.Int(a), Data.Dec(b)) => Data.Bool(BigDecimal(a) > b)
    case (Data.Dec(a), Data.Int(b)) => Data.Bool(a > BigDecimal(b))
    case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.OffsetDateTime(a), Data.OffsetDateTime(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.OffsetDate(a), Data.OffsetDate(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.OffsetTime(a), Data.OffsetTime(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.LocalDateTime(a), Data.LocalDateTime(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.LocalDate(a), Data.LocalDate(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.LocalTime(a), Data.LocalTime(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
      case (true, false) => Data.Bool(true)
      case _ => Data.Bool(false)
    }
    case _ => undefined
  }

  private def gte(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Bool(a >= b)
    case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a >= b)
    case (Data.Int(a), Data.Dec(b)) => Data.Bool(BigDecimal(a) >= b)
    case (Data.Dec(a), Data.Int(b)) => Data.Bool(a >= BigDecimal(b))
    case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.OffsetDateTime(a), Data.OffsetDateTime(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.OffsetDate(a), Data.OffsetDate(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.OffsetTime(a), Data.OffsetTime(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.LocalDateTime(a), Data.LocalDateTime(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.LocalDate(a), Data.LocalDate(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.LocalTime(a), Data.LocalTime(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
      case (false, true) => Data.Bool(false)
      case _ => Data.Bool(true)
    }
    case _ => undefined
  }

  private def between(d1: Data, d2: Data, d3: Data): Data = (d1, d2, d3) match {
    case (Data.Int(a), Data.Int(b), Data.Int(c)) =>
      Data.Bool(b <= a && a <= c)
    case (Data.Dec(a), Data.Dec(b), Data.Dec(c)) =>
      Data.Bool(b <= a && a <= c)
    case (Data.Interval(a), Data.Interval(b), Data.Interval(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Str(a), Data.Str(b), Data.Str(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.OffsetDateTime(a), Data.OffsetDateTime(b), Data.OffsetDateTime(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.OffsetDate(a), Data.OffsetDate(b), Data.OffsetDate(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.OffsetTime(a), Data.OffsetTime(b), Data.OffsetTime(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.LocalDateTime(a), Data.LocalDateTime(b), Data.LocalDateTime(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.LocalDate(a), Data.LocalDate(b), Data.LocalDate(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.LocalTime(a), Data.LocalTime(b), Data.LocalTime(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Bool(a), Data.Bool(b), Data.Bool(c)) => (b,a,c) match {
      case (false, false, true) => Data.Bool(true)
      case (false, true, true) => Data.Bool(true)
      case (true, true, true) => Data.Bool(true)
      case (false, false, false) => Data.Bool(true)
      case _ => Data.Bool(false)
    }
    case _ => undefined
  }

  private def toStringFunc: Data => Data = {
    case Data.Null => Data.Str("null")
    case d: Data.Str => d
    case Data.Bool(v) => Data.Str(v.toString)
    case Data.Dec(v) => Data.Str(v.toString)
    case Data.Int(v) => Data.Str(v.toString)
    case Data.OffsetDateTime(v) => Data.Str(v.toString)
    case Data.OffsetDate(v) => Data.Str(v.toString)
    case Data.OffsetTime(v) => Data.Str(v.toString)
    case Data.LocalDateTime(v) => Data.Str(v.toString)
    case Data.LocalDate(v) => Data.Str(v.toString)
    case Data.LocalTime(v) => Data.Str(v.toString)
    case Data.Interval(v) => Data.Str(v.toString)
    case Data.Binary(v) => Data.Str(v.toList.mkString(""))
    case Data.Id(s) => Data.Str(s)
    case _ => undefined
  }

  private def century(year: Int): Data = Data.Int(((year - 1) / 100) + 1)

  private def temporalTrunc(part: TemporalPart, src: Data): Data =
    src match {
      case CanLensDateTime(i) => i.peeks(datetime.truncDateTime(part, _))
      case CanLensDate(i)     => i.peeks(datetime.truncDate(part, _))
      case CanLensTime(i)     => i.peeks(datetime.truncTime(part, _))
      case _                  => undefined
    }

  private def search(dStr: Data, dPattern: Data, dInsen: Data): Data =
    (dStr, dPattern, dInsen) match {
      case (Data.Str(str), Data.Str(pattern), Data.Bool(insen)) =>
        Data.Bool(StringLib.matchAnywhere(str, pattern, insen))
      case _ => undefined
    }

  private def substring(dStr: Data, dFrom: Data, dCount: Data): Data =
    (dStr, dFrom, dCount) match {
      case (Data.Str(str), Data.Int(from), Data.Int(count)) =>
        \/.fromTryCatchNonFatal(Data.Str(StringLib.safeSubstring(str, from.toInt, count.toInt))).fold(κ(Data.NA), ι)
      case _ => undefined
    }

  private def split(dStr: Data, dDelim: Data): Data =
    (dStr, dDelim) match {
      case (Data.Str(str), Data.Str(delim)) =>
        \/.fromTryCatchNonFatal(Data.Arr(str.split(Regex.quote(delim), -1).toList.map(Data.Str(_)))).fold(κ(Data.NA), ι)
      case _ => undefined
    }

}
