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
import quasar.{Data, DataCodec}
import quasar.Planner._
import quasar.common.PrimaryType
import quasar.fp.ski._
import quasar.qscript._, MapFuncsCore._
import quasar.std.{DateLib, StringLib}
import quasar.std.TemporalPart

import java.time._, ZoneOffset.UTC
import scala.math

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{Divide => _, _}, Scalaz._

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
      : PlannerError \/ ((Data, List[Data]) => Data) =
    f.cataM(interpretM(
      _.idx.fold((_: (Data, List[Data]))._1)(i => _._2(i)).right,
      change[T, (Data, List[Data])])).map(f => (l: Data, r: List[Data]) => f((l, r)))

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

    case Date(f) => (f >>> {
      case Data.Str(v) => DateLib.parseDate(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case Time(f) => (f >>> {
      case Data.Str(v) => DateLib.parseTime(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case Timestamp(f) => (f >>> {
      case Data.Str(v) => DateLib.parseTimestamp(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case Interval(f) => (f >>> {
      case Data.Str(v) => DateLib.parseInterval(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case StartOfDay(f) => (f >>> {
      case d @ Data.Timestamp(_) => temporalTrunc(TemporalPart.Day, d).fold(κ(undefined), ι)
      case Data.Date(v)          => Data.Timestamp(v.atStartOfDay(ZoneOffset.UTC).toInstant)
      case _ => undefined
    }).right
    case TemporalTrunc(p, f) => (f >>> (d => temporalTrunc(p, d).fold(κ(undefined), ι))).right
    case TimeOfDay(f) => (f >>> {
      case Data.Timestamp(v) => Data.Time(v.atZone(ZoneOffset.UTC).toLocalTime)
      case _ => undefined
    }).right
    case ToTimestamp(f) => (f >>> {
      case Data.Int(epoch) => Data.Timestamp(Instant.ofEpochMilli(epoch.toLong))
      case _ => undefined
    }).right
    case ExtractCentury(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => century(dt.getYear()))
      case Data.Date(v) => century(v.getYear())
      case _ => undefined
    }).right
    case ExtractDayOfMonth(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getDayOfMonth()))
      case Data.Date(v) => Data.Int(v.getDayOfMonth())
      case _ => undefined
    }).right
    case ExtractDecade(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getYear() / 10))
      case Data.Date(v) => Data.Int(v.getYear() / 10)
      case _ => undefined
    }).right
    case ExtractDayOfWeek(f) => (f >>> {
      case Data.Timestamp(v) =>
        fromDateTime(v)(dt => Data.Int(dt.getDayOfWeek().getValue() % 7))
      case Data.Date(v) => Data.Int(v.getDayOfWeek().getValue() % 7)
      case _ => undefined
    }).right
    case ExtractDayOfYear(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getDayOfYear()))
      case Data.Date(v) => Data.Int(v.getDayOfYear())
      case _ => undefined
    }).right
    case ExtractEpoch(f) => (f >>> {
      case Data.Timestamp(v) => Data.Int(v.toEpochMilli() / 1000)
      case Data.Date(v) => Data.Int(v.atStartOfDay(ZoneOffset.UTC).toEpochSecond())
      case _ => undefined
    }).right
    case ExtractHour(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getHour()))
      case Data.Time(v) => Data.Int(v.getHour())
      case Data.Date(_) => Data.Int(0)
      case _ => undefined
    }).right
    case ExtractIsoDayOfWeek(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getDayOfWeek().getValue()))
      case Data.Date(v) => Data.Int(v.getDayOfWeek().getValue())
      case _ => undefined
    }).right
    case ExtractIsoYear(f) => (f >>> {
      case _ => undefined
    }).right
    case ExtractMicroseconds(f) => (f >>> {
      case Data.Timestamp(v) =>
        fromDateTime(v) { dt =>
          val sec = dt.getSecond() * 1000000
          val milli = dt.getNano() / 1000
          Data.Int(sec + milli)
        }
      case Data.Time(v) =>
        val sec = v.getSecond() * 1000000
        val milli = v.getNano() / 1000
        Data.Int(sec + milli)
      case Data.Date(_) => Data.Dec(0)
      case _ => undefined
    }).right
    case ExtractMillennium(f) => (f >>> {
      case Data.Timestamp(v) =>
        fromDateTime(v)(dt => Data.Int(((dt.getYear() - 1) / 1000) + 1))
      case Data.Date(v) => Data.Int(((v.getYear() - 1) / 1000) + 1)
      case _ => undefined
    }).right
    case ExtractMilliseconds(f) => (f >>> {
      case Data.Timestamp(v) =>
        fromDateTime(v) { dt =>
          val sec = dt.getSecond() * 1000
          val milli = dt.getNano() / 1000000
          Data.Int(sec + milli)
        }
      case Data.Time(v) =>
        val sec = v.getSecond() * 1000
        val milli = v.getNano() / 1000000
        Data.Int(sec + milli)
      case Data.Date(_) => Data.Dec(0)
      case _ => undefined
    }).right
    case ExtractMinute(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getMinute()))
      case Data.Time(v) => Data.Int(v.getMinute())
      case Data.Date(_) => Data.Int(0)
      case _ => undefined
    }).right
    case ExtractMonth(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getMonth().getValue()))
      case Data.Date(v) => Data.Int(v.getMonth().getValue())
      case _ => undefined
    }).right
    case ExtractQuarter(f) => (f >>> {
      case Data.Timestamp(v) =>
        fromDateTime(v)(dt => Data.Int(((dt.getMonth().getValue - 1) / 3) + 1))
      case Data.Date(v) => Data.Int(((v.getMonth().getValue - 1) / 3) + 1)
      case _ => undefined
    }).right
    case ExtractSecond(f) => (f >>> {
      case Data.Timestamp(v) =>
        fromDateTime(v) { dt =>
          val sec = dt.getSecond()
          val milli = dt.getNano() / 1000
          Data.Dec(BigDecimal(s"$sec.$milli"))
        }
      case Data.Time(v) =>
        val sec = v.getSecond()
        val milli = v.getNano() / 1000
        Data.Dec(BigDecimal(s"$sec.$milli"))
      case Data.Date(_) => Data.Dec(0)
      case _ => undefined
    }).right
    case ExtractWeek(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getDayOfYear() / 7))
      case Data.Date(v) => Data.Int(v.getDayOfYear() / 7)
      case _ => undefined
    }).right
    case ExtractYear(f) => (f >>> {
      case Data.Timestamp(v) => fromDateTime(v)(dt => Data.Int(dt.getYear()))
      case Data.Date(v) => Data.Int(v.getYear())
      case _ => undefined
    }).right
    case Now() => κ(Data.Timestamp(Instant.now())).right

    case Negate(f) => (f >>> {
      case Data.Int(v) => Data.Int(-v)
      case Data.Dec(v) => Data.Dec(-v)
      case Data.Interval(v) => Data.Interval(v.negated())
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
    case ProjectField(fSrc, fField) => ((x: A) => (fSrc(x), fField(x)) match {
      case (Data.Obj(m), Data.Str(field)) if m.isDefinedAt(field) => m(field)
      case _ => undefined
    }).right
    case DeleteField(fSrc, fField) =>  ((x: A) => (fSrc(x), fField(x)) match {
      case (Data.Obj(m), Data.Str(field)) if m.isDefinedAt(field) => Data.Obj(m - field)
      case _ => undefined
    }).right
    case Range(fFrom, fTo) => ((x: A) => (fFrom(x), fTo(x)) match {
      case (Data.Int(a), Data.Int(b)) if(a <= b) => Data.Set((a to b).map(Data.Int(_)).toList)
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
    case (Data.Timestamp(a), Data.Interval(b)) => Data.Timestamp(a.plus(b))
    case (Data.Date(a), Data.Interval(b)) => Data.Date(a.plus(b))
    case (Data.Time(a), Data.Interval(b)) => Data.Time(a.plus(b))
    case _ => undefined
  }

  private def subtract(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a - b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) - b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a - BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a - b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Interval(a.minus(b))
    case (Data.Date(a), Data.Interval(b)) => Data.Date(a.minus(b))
    case (Data.Time(a), Data.Interval(b)) => Data.Time(a.minus(b))
    case (Data.Timestamp(a), Data.Interval(b)) => Data.Timestamp(a.minus(b))
    case _ => undefined
  }

  private def multiply(d1: Data, d2: Data): Data = (d1, d2)  match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a * b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) * b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a * BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a * b)
    case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case _ => undefined
  }

  private def divide(d1: Data, d2: Data): Data = (d1, d2) match {
    case (_, Data.Dec(b)) if b === BigDecimal(0) => Data.NA
    case (_, Data.Int(b)) if b === 0 => Data.NA
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a(BigDecimal.defaultMathContext) / b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) / b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a(BigDecimal.defaultMathContext) / BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Dec(BigDecimal(a) / BigDecimal(b))
    case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case _ => undefined
  }

  // TODO interval cases?
  private def modulo(d1: Data, d2: Data) = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Int(a % b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a).remainder(b))
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a.remainder(BigDecimal(b)))
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a.remainder(b))
    case (Data.Interval(a), Data.Int(b)) => ???
    case (Data.Interval(a), Data.Dec(b)) => ???
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
    case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) < 0)
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
    case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) <= 0)
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
    case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) > 0)
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
    case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) >= 0)
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
    case (Data.Timestamp(a), Data.Timestamp(b), Data.Timestamp(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Date(a), Data.Date(b), Data.Date(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Time(a), Data.Time(b), Data.Time(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Bool(a), Data.Bool(b), Data.Bool(c)) => (a,b,c) match {
      case (false, false, true) => Data.Bool(true)
      case (false, true, true) => Data.Bool(true)
      case (true, true, true) => Data.Bool(true)
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
    case Data.Timestamp(v) => Data.Str(v.atZone(UTC).format(DataCodec.dateTimeFormatter))
    case Data.Date(v) => Data.Str(v.toString)
    case Data.Time(v) => Data.Str(v.format(DataCodec.timeFormatter))
    case Data.Interval(v) => Data.Str(v.toString)
    case Data.Binary(v) => Data.Str(v.toList.mkString(""))
    case Data.Id(s) => Data.Str(s)
    case _ => undefined
  }

  private def century(year: Int): Data = Data.Int(((year - 1) / 100) + 1)

  private def temporalTrunc(part: TemporalPart, src: Data): PlannerError \/ Data =
    (src match {
      case d @ Data.Date(_)      => DateLib.truncDate(part, d)
      case t @ Data.Time(_)      => DateLib.truncTime(part, t)
      case t @ Data.Timestamp(_) => DateLib.truncTimestamp(part, t)
      case _ =>
        undefined.right
    }).leftMap(e => InternalError.fromMsg(e.shows))

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

  private def fromDateTime(v: Instant)(f: ZonedDateTime => Data): Data =
    \/.fromTryCatchNonFatal(v.atZone(ZoneOffset.UTC)).fold(κ(Data.NA), f)

}
