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

package quasar.std

import slamdata.Predef._
import quasar.{Data, Func, NullaryFunc, UnaryFunc, Mapping, Type, SemanticError}, SemanticError._
import quasar.fp.ski._

import java.time._
import java.time.temporal.{ChronoUnit => CU}
import java.time.ZoneOffset.UTC
import scalaz._, Validation.success
import scalaz.syntax.bind._
import shapeless.{Data => _, _}

sealed abstract class TemporalPart extends Serializable

object TemporalPart {
  final case object Century     extends TemporalPart
  final case object Day         extends TemporalPart
  final case object Decade      extends TemporalPart
  final case object Hour        extends TemporalPart
  final case object Microsecond extends TemporalPart
  final case object Millennium  extends TemporalPart
  final case object Millisecond extends TemporalPart
  final case object Minute      extends TemporalPart
  final case object Month       extends TemporalPart
  final case object Quarter     extends TemporalPart
  final case object Second      extends TemporalPart
  final case object Week        extends TemporalPart
  final case object Year        extends TemporalPart

  implicit val equal: Equal[TemporalPart] = Equal.equalRef
  implicit val show: Show[TemporalPart] = Show.showFromToString
}


trait DateLib extends Library with Serializable {
  import TemporalPart._

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

  def startOfDayInstant(date: LocalDate): Instant =
    date.atStartOfDay.atZone(ZoneOffset.UTC).toInstant

  def startOfDay(date: Data.Date): Data.Timestamp =
    Data.Timestamp(startOfDayInstant(date.value))

  def startOfNextDay(date: Data.Date): Data.Timestamp =
    Data.Timestamp(startOfDayInstant(date.value.plus(Period.ofDays(1))))

  def truncLocalTime(part: TemporalPart, t: LocalTime): SemanticError \/ LocalTime =
    \/.fromTryCatchNonFatal(
      part match {
        case Century | Day | Decade | Millennium |
             Month | Quarter | Week | Year       => t.truncatedTo(CU.DAYS)
        case Hour                                => t.truncatedTo(CU.HOURS)
        case Microsecond                         => t.truncatedTo(CU.MICROS)
        case Millisecond                         => t.truncatedTo(CU.MILLIS)
        case Minute                              => t.truncatedTo(CU.MINUTES)
        case Second                              => t.truncatedTo(CU.SECONDS)
      }
    ).leftMap(err => GenericError(s"truncLocalTime: $err"))

  def truncZonedDateTime(part: TemporalPart, zdt: ZonedDateTime):  SemanticError \/ ZonedDateTime =
    truncLocalTime(part, zdt.toLocalTime) >>= (t => \/.fromTryCatchNonFatal {
      val truncTime: ZonedDateTime =
        ZonedDateTime.of(zdt.toLocalDate, t, zdt.getZone)

      part match {
        case Century =>
          truncTime
            .withDayOfMonth(1)
            .withMonth(1)
            .withYear((zdt.getYear / 100) * 100)
        case Day =>
          truncTime
        case Decade =>
          truncTime
            .withDayOfMonth(1)
            .withMonth(1)
            .withYear((zdt.getYear / 10) * 10)
        case Hour =>
          truncTime
        case Microsecond =>
          truncTime
        case Millennium =>
          truncTime
            .withDayOfMonth(1)
            .withMonth(1)
            .withYear((zdt.getYear / 1000) * 1000)
        case Millisecond =>
          truncTime
        case Minute =>
          truncTime
        case Month =>
          truncTime
            .withDayOfMonth(1)
        case Quarter =>
          truncTime
            .withDayOfMonth(1)
            .withMonth(zdt.getMonth.firstMonthOfQuarter().getValue)
        case Second =>
          truncTime
        case Week =>
          truncTime
            .`with`(java.time.DayOfWeek.MONDAY)
        case Year =>
          truncTime
            .withDayOfMonth(1)
            .withMonth(1)
      }
    }.leftMap(err => GenericError(s"truncZonedDateTime: $err")))

  def truncDate(part: TemporalPart, d: Data.Date): SemanticError \/ Data.Date =
    truncZonedDateTime(part, d.value.atStartOfDay(UTC)) ∘ (t => Data.Date(t.toLocalDate))

  def truncTime(part: TemporalPart, t: Data.Time): SemanticError \/ Data.Time =
    truncLocalTime(part, t.value) ∘ (Data.Time(_))

  def truncTimestamp(part: TemporalPart, ts: Data.Timestamp): SemanticError \/ Data.Timestamp =
    truncZonedDateTime(part, ts.value.atZone(UTC)) ∘ (t => Data.Timestamp(t.toInstant))


  // NB: SQL specifies a function called `extract`, but that doesn't have comma-
  //     separated arguments. `date_part` is Postgres’ name for the same thing
  //     with commas.

  private def extract(help: String) =
    UnaryFunc(
      Mapping, help,
      Type.Numeric,
      Func.Input1(Type.Temporal),
      noSimplification,
      constTyper[nat._1](Type.Numeric),
      basicUntyper)

  val ExtractCentury      = extract(
    "Pulls out the century subfield from a date/time value (currently year/100).")
  val ExtractDayOfMonth   = extract(
    "Pulls out the day of month (`day`) subfield from a date/time value (1-31).")
  val ExtractDecade       = extract(
    "Pulls out the decade subfield from a date/time value (year/10).")
  val ExtractDayOfWeek    = extract(
    "Pulls out the day of week (`dow`) subfield from a date/time value " +
    "(Sunday: 0 to Saturday: 7).")
  val ExtractDayOfYear    = extract(
    "Pulls out the day of year (`doy`) subfield from a date/time value (1-365 or -366).")
  val ExtractEpoch        = extract(
    "Pulls out the epoch subfield from a date/time value. For dates and " +
    "timestamps, this is the number of seconds since midnight, 1970-01-01. " +
    "For intervals, the number of seconds in the interval.")
  val ExtractHour         = extract(
    "Pulls out the hour subfield from a date/time value (0-23).")
  val ExtractIsoDayOfWeek       = extract(
    "Pulls out the ISO day of week (`isodow`) subfield from a date/time value " +
    "(Monday: 1 to Sunday: 7).")
  val ExtractIsoYear      = extract(
    "Pulls out the ISO year (`isoyear`) subfield from a date/time value (based " +
    "on the first week containing Jan. 4).")
  val ExtractMicroseconds = extract(
    "Pulls out the microseconds subfield from a date/time value (including seconds).")
  val ExtractMillennium    = extract(
    "Pulls out the millennium subfield from a date/time value (currently year/1000).")
  val ExtractMilliseconds = extract(
    "Pulls out the milliseconds subfield from a date/time value (including seconds).")
  val ExtractMinute       = extract(
    "Pulls out the minute subfield from a date/time value (0-59).")
  val ExtractMonth        = extract(
    "Pulls out the month subfield from a date/time value (1-12).")
  val ExtractQuarter      = extract(
    "Pulls out the quarter subfield from a date/time value (1-4).")
  val ExtractSecond = extract(
    "Pulls out the second subfield from a date/time value (0-59, with fractional parts).")
  val ExtractTimezone = extract(
    "Pulls out the timezone subfield from a date/time value (in seconds east of UTC).")
  val ExtractTimezoneHour = extract(
    "Pulls out the hour component of the timezone subfield from a date/time value.")
  val ExtractTimezoneMinute = extract(
    "Pulls out the minute component of the timezone subfield from a date/time value.")
  val ExtractWeek = extract(
    "Pulls out the week subfield from a date/time value (1-53).")
  val ExtractYear = extract(
    "Pulls out the year subfield from a date/time value.")

  val Date = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DD) to a date value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Date,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseDate(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.Date)
    },
    basicUntyper)

  val Now = NullaryFunc(
    Mapping,
    "Returns the current timestamp – this must always return the same value within the same execution of a query.",
    Type.Timestamp,
    noSimplification)

  val Time = UnaryFunc(
    Mapping,
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
    "Converts a string in the format (ISO 8601, e.g. P3DT12H30M15.0S) to an interval value. Note: year/month not currently supported. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Interval,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseInterval(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str) => success(Type.Interval)
    },
    basicUntyper)

  val StartOfDay = UnaryFunc(
    Mapping,
    "Converts a Date or Timestamp to a Timestamp at the start of that day.",
    Type.Timestamp,
    Func.Input1(Type.Date ⨿ Type.Timestamp),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Date(v))) =>
        success(Type.Const(Data.Timestamp(startOfDayInstant(v))))
      case Sized(Type.Date) =>
        success(Type.Timestamp)
      case Sized(Type.Const(Data.Timestamp(v))) =>
        truncZonedDateTime(TemporalPart.Day, v.atZone(UTC))
          .map(t => Type.Const(Data.Timestamp(t.toInstant))).validation.toValidationNel
      case Sized(Type.Timestamp) =>
        success(Type.Timestamp)
    },
    untyper[nat._1] {
      case Type.Const(Data.Timestamp(ts)) => success(Func.Input1(Type.Const(Data.Date(ts.atZone(UTC).toLocalDate))))
      case Type.Timestamp                 => success(Func.Input1(Type.Date))
      case t                              => success(Func.Input1(t))
    })

  val TimeOfDay = UnaryFunc(
    Mapping,
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
    "Converts an integer epoch time value (i.e. milliseconds since 1 Jan. 1970, UTC) to a timestamp constant.",
    Type.Timestamp,
    Func.Input1(Type.Int),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Int(millis))) => Type.Const(Data.Timestamp(Instant.ofEpochMilli(millis.toLong)))
      case Sized(Type.Int) => Type.Timestamp
    },
    basicUntyper)
}

object DateLib extends DateLib
