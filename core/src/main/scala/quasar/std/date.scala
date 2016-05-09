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

package quasar.std

import quasar.Predef._
import quasar.{Data, Func, UnaryFunc, BinaryFunc, GenericFunc, Mapping, Type, SemanticError}, SemanticError._
import quasar.fp._

import org.threeten.bp.{Duration, Instant, LocalDate, LocalTime, Period, ZoneOffset}
import scalaz._, Validation.success
import shapeless.{Data => _, _}

trait DateLib extends Library {
  def parseTimestamp(str: String): SemanticError \/ Data.Timestamp =
    \/.fromTryCatchNonFatal(Instant.parse(str)).bimap(
      κ(DateFormatError(Timestamp, str, None)),
      Data.Timestamp.apply)

  def parseDate(str: String): SemanticError \/ Data.Date =
    \/.fromTryCatchNonFatal(LocalDate.parse(str)).bimap(
      κ(DateFormatError(Date, str, None)),
      Data.Date.apply)

  def parseTime(str: String): SemanticError \/ Data.Time =
    \/.fromTryCatchNonFatal(LocalTime.parse(str)).bimap(
      κ(DateFormatError(Time, str, None)),
      Data.Time.apply)

  def parseInterval(str: String): SemanticError \/ Data.Interval = {
    \/.fromTryCatchNonFatal(Duration.parse(str)).bimap(
      κ(DateFormatError(Interval, str, Some("expected, e.g. P3DT12H30M15.0S; note: year/month not currently supported"))),
      Data.Interval.apply)
  }

  private def startOfDayInstant(date: LocalDate): Instant =
    date.atStartOfDay.atZone(ZoneOffset.UTC).toInstant

  def startOfDay(date: Data.Date): Data.Timestamp =
    Data.Timestamp(startOfDayInstant(date.value))

  def startOfNextDay(date: Data.Date): Data.Timestamp =
    Data.Timestamp(startOfDayInstant(date.value.plus(Period.ofDays(1))))

  // NB: SQL specifies a function called `extract`, but that doesn't have comma-
  //     separated arguments. `date_part` is Postgres’ name for the same thing
  //     with commas.
  val Extract = BinaryFunc(
    Mapping,
    "date_part",
    "Pulls out a part of the date. The first argument is one of the strings defined for Postgres’ `date_type function. This is a partial function – using an unsupported string has undefined results.",
    Type.Numeric,
    Func.Input2(Type.Str, Type.Temporal),
    noSimplification,
    constTyper[nat._2](Type.Numeric),
    basicUntyper)

  val Date = UnaryFunc(
    Mapping,
    "date",
    "Converts a string in the format (YYYY-MM-DD) to a date value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Date,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseDate(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.Date)
    },
    basicUntyper)

  val Time = UnaryFunc(
    Mapping,
    "time",
    "Converts a string in the format (HH:MM:SS[.SSS]) to a time value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Time,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseTime(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.Time)
    },
    basicUntyper)

  val Timestamp = UnaryFunc(
    Mapping,
    "timestamp",
    "Converts a string in the format (ISO 8601, UTC, e.g. 2015-05-12T12:22:00Z) to a timestamp value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Timestamp,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseTimestamp(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str) => success(Type.Timestamp)
    },
    basicUntyper)

  val Interval = UnaryFunc(
    Mapping,
    "interval",
    "Converts a string in the format (ISO 8601, e.g. P3DT12H30M15.0S) to an interval value. Note: year/month not currently supported. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Interval,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseInterval(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str) => success(Type.Interval)
    },
    basicUntyper)

  val TimeOfDay = UnaryFunc(
    Mapping,
    "time_of_day",
    "Extracts the time of day from a (UTC) timestamp value.",
    Type.Time,
    Func.Input1(Type.Timestamp),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Timestamp(value))) => Type.Const(Data.Time(value.atZone(ZoneOffset.UTC).toLocalTime))
      case Sized(Type.Timestamp) => Type.Time
    },
    basicUntyper)

  val ToTimestamp = UnaryFunc(
    Mapping,
    "to_timestamp",
    "Converts an integer epoch time value (i.e. milliseconds since 1 Jan. 1970, UTC) to a timestamp constant.",
    Type.Timestamp,
    Func.Input1(Type.Int),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Int(millis))) => Type.Const(Data.Timestamp(Instant.ofEpochMilli(millis.toLong)))
      case Sized(Type.Int) => Type.Timestamp
    },
    basicUntyper)

  def unaryFunctions: List[GenericFunc[nat._1]] =
    Date :: Time :: Timestamp :: Interval :: TimeOfDay :: ToTimestamp :: Nil

  def binaryFunctions: List[GenericFunc[nat._2]] = Extract :: Nil
  def ternaryFunctions: List[GenericFunc[nat._3]] = Nil
}

object DateLib extends DateLib
