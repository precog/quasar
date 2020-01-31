/*
 * Copyright 2014–2020 SlamData Inc.
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
import quasar._
import quasar.common.data.Data
import quasar.ArgumentError._
import quasar.fp.ski._
import qdata.time.{DateTimeInterval, OffsetDate => QOffsetDate}

import java.time.{
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
}

import scalaz._
import scalaz.syntax.either._

trait DateLib extends Library with Serializable {

  def parseOffsetDateTime(str: String): ArgumentError \/ Data.OffsetDateTime =
    \/.fromTryCatchNonFatal(JOffsetDateTime.parse(str)).bimap(
      κ(temporalFormatError(OffsetDateTime, str, None)),
      Data.OffsetDateTime.apply)

  def parseOffsetTime(str: String): ArgumentError \/ Data.OffsetTime =
    \/.fromTryCatchNonFatal(JOffsetTime.parse(str)).bimap(
      κ(temporalFormatError(OffsetTime, str, None)),
      Data.OffsetTime.apply)

  def parseOffsetDate(str: String): ArgumentError \/ Data.OffsetDate =
    \/.fromTryCatchNonFatal(QOffsetDate.parse(str)).bimap(
      κ(temporalFormatError(OffsetDate, str, None)),
      Data.OffsetDate.apply)

  def parseLocalDateTime(str: String): ArgumentError \/ Data.LocalDateTime =
    \/.fromTryCatchNonFatal(JLocalDateTime.parse(str)).bimap(
      κ(temporalFormatError(OffsetDate, str, None)),
      Data.LocalDateTime.apply)

  def parseLocalTime(str: String): ArgumentError \/ Data.LocalTime =
    \/.fromTryCatchNonFatal(JLocalTime.parse(str)).bimap(
      κ(temporalFormatError(LocalTime, str, None)),
      Data.LocalTime.apply)

  def parseLocalDate(str: String): ArgumentError \/ Data.LocalDate =
    \/.fromTryCatchNonFatal(JLocalDate.parse(str)).bimap(
      κ(temporalFormatError(LocalDate, str, None)),
      Data.LocalDate.apply)

  def parseInterval(str: String): ArgumentError \/ Data.Interval =
    DateTimeInterval.parse(str) match {
      case Some(i) => Data.Interval(i).right
      case None => temporalFormatError(Interval, str, Some("expected, e.g. P3DT12H30M15.0S")).left
    }

  // NB: SQL specifies a function called `extract`, but that doesn't have comma-
  //     separated arguments. `date_part` is Postgres’ name for the same thing
  //     with commas.

  private def unaryFunc(help: String) =
    UnaryFunc(
      Mapping, help,
      noSimplification)

  private def binaryFunc(help: String) =
    BinaryFunc(
      Mapping, help,
      noSimplification)

  val ExtractCentury = unaryFunc(
    "Pulls out the century subfield from a date/time value (currently (year - 1)/100 + 1).")

  val ExtractDayOfMonth = unaryFunc(
    "Pulls out the day of month (`day`) subfield from a date/time value (1-31).")

  val ExtractDecade = unaryFunc(
    "Pulls out the decade subfield from a date/time value (year/10).")

  val ExtractDayOfWeek = unaryFunc(
    "Pulls out the day of week (`dow`) subfield from a date/time value " + "(Sunday: 0 to Saturday: 6).")

  val ExtractDayOfYear = unaryFunc(
    "Pulls out the day of year (`doy`) subfield from a date/time value (1-365 or -366).")

  val ExtractEpoch = unaryFunc(
    "Pulls out the epoch subfield from a datetime value with timezone offset. " +
    "This is the number of seconds since midnight, 1970-01-01.")

  val ExtractHour = unaryFunc(
    "Pulls out the hour subfield from a date/time value (0-23).")

  val ExtractIsoDayOfWeek = unaryFunc(
    "Pulls out the ISO day of week (`isodow`) subfield from a date/time value (Monday: 1 to Sunday: 7).")

  val ExtractIsoYear = unaryFunc(
    "Pulls out the ISO year (`isoyear`) subfield from a date/time value (based on the first week (Monday is the first day of the week) containing Jan. 4).")

  val ExtractMicrosecond = unaryFunc(
    "Computes the microseconds of a date/time value (including seconds).")

  val ExtractMillennium = unaryFunc(
    "Pulls out the millennium subfield from a date/time value (currently (year - 1)/1000 + 1).")

  val ExtractMillisecond = unaryFunc(
    "Computes the milliseconds of a date/time value (including seconds).")

  val ExtractMinute = unaryFunc(
    "Pulls out the minute subfield from a date/time value (0-59).")

  val ExtractMonth = unaryFunc(
    "Pulls out the month subfield from a date/time value (1-12).")

  val ExtractQuarter = unaryFunc(
    "Pulls out the quarter subfield from a date/time value (1-4).")

  val ExtractSecond = unaryFunc(
    "Pulls out the second subfield from a date/time value (0-59, with fractional parts).")

  val ExtractTimeZone = unaryFunc(
    "Pulls out the timezone subfield from a date/time value (in seconds east of UTC).")

  val ExtractTimeZoneHour = unaryFunc(
    "Pulls out the hour component of the timezone subfield from a date/time value.")

  val ExtractTimeZoneMinute = unaryFunc(
    "Pulls out the minute component of the timezone subfield from a date/time value.")

  val ExtractWeek = unaryFunc(
    "Pulls out the week subfield from a date/time value (1-53).")

  val ExtractYear = unaryFunc(
    "Pulls out the year subfield from a date/time value.")

  // FIXME `ZoneOffset.ofTotalSeconds` throws an exception if the integer
  // input is not in the range [-64800, 64800]
  val SetTimeZone = binaryFunc(
    "Sets the timezone subfield in a date/time value (in seconds east of UTC).")

  val SetTimeZoneMinute = binaryFunc(
    "Sets the minute component of the timezone subfield in a date/time value.")

  val SetTimeZoneHour = binaryFunc(
    "Sets the hour component of the timezone subfield in a date/time value.")

  val Now = NullaryFunc(
    Mapping,
    "Returns the current datetime in the current time zone – this must always return the same value within the same execution of a query.",
    noSimplification)

  val NowTime = NullaryFunc(
    Mapping,
    "Returns the current time in the current time zone – this must always return the same value within the same execution of a query.",
    noSimplification)

  val NowDate = NullaryFunc(
    Mapping,
    "Returns the current date in the current time zone – this must always return the same value within the same execution of a query.",
    noSimplification)

  val CurrentTimeZone = NullaryFunc(
    Mapping,
    "Returns the current time zone offset in total seconds - this must always return the same value within the same execution of a query.",
    noSimplification)

  val OffsetDateTime = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DDTHH:MM:SS((+/-)HH[:MM[:SS]])/Z) to a timestamp value with a time zone offset. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    noSimplification)

  val OffsetTime = UnaryFunc(
    Mapping,
    "Converts a string in the format (HH:MM:SS[.SSS]((+/-)HH:MM:SS)/Z) to a time value with a time zone offset. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    noSimplification)

  val OffsetDate = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DD((+/-)HH:MM:SS)/Z) to a date value with a time zone offset. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    noSimplification)

  val LocalDateTime = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DDTHH:MM:SS) to a date value paired with a time. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    noSimplification)

  val LocalTime = UnaryFunc(
    Mapping,
    "Converts a string in the format (HH:MM:SS[.SSS]) to a time value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    noSimplification)

  val LocalDate = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DD) to a date value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    noSimplification)

  /**
    * TODO: document behavior change, now that years and months work
    */
  val Interval = UnaryFunc(
    Mapping,
    "Converts a string in the format (ISO 8601, e.g. P3DT12H30M15.0S) to an interval value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    noSimplification)

  /**
   * TODO: document behavior change, `StartOfDay` only makes `OffsetDateTime`s out of other `OffsetDateTime`s.
   */
  val StartOfDay = UnaryFunc(
    Mapping,
    "Converts a DateTime or Date to a DateTime at the start of that day.",
    noSimplification)

  val TimeOfDay = UnaryFunc(
    Mapping,
    "Extracts the time of day from a datetime value. Preserves time zone information.",
    noSimplification)

  val ToTimestamp = UnaryFunc(
    Mapping,
    "Converts an integer epoch time value (i.e. milliseconds since 1 Jan. 1970, UTC) to a timestamp constant.",
    noSimplification)

  val ToLocal = BinaryFunc(
    Mapping,
    "Converts an offset date, time, or datetime and offset string to a local date, time, or datetime.",
    noSimplification)

  val ToOffset = BinaryFunc(
    Mapping,
    "Converts a local date, time, or datetime and offset string to an offset date, time, or datetime.",
    noSimplification)
}

object DateLib extends DateLib
