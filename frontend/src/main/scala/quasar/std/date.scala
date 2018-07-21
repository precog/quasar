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

package quasar.std

import slamdata.Predef._
import quasar._
import quasar.common.data.{Data, DataDateTimeExtractors}
import quasar.common.data.DataDateTimeExtractors._
import quasar.ArgumentError._
import quasar.fp.ski._
import quasar.time._
import qdata.time.{DateTimeInterval, OffsetDate => QOffsetDate}

import java.time.{
  Instant,
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
  ZoneOffset
}

import scalaz._
import scalaz.Validation.success
import scalaz.syntax.either._
import shapeless.{Data => _, _}

trait DateLib extends Library with Serializable {

  // legacy function for parsing Instants
  def parseTimestamp(str: String): ArgumentError \/ Data.OffsetDateTime =
    \/.fromTryCatchNonFatal(Instant.parse(str).atOffset(ZoneOffset.UTC)).bimap(
      κ(temporalFormatError(OffsetDateTime, str, None)),
      Data.OffsetDateTime.apply)

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

  def startOfDayInstant(date: JLocalDate): Instant =
    date.atStartOfDay.atZone(ZoneOffset.UTC).toInstant

  def startOfDay[I, O](in: I)(setter: SetTime[I, O]): O =
    setter(in, JLocalTime.MIN)

  def startOfNextDay[I, O](in: I)(dateLens: LensDate[I], setTime: SetTime[I, O]): O = {
    val updateDate = dateLens.modify(_.plusDays(1))(in)
    val updateTime = startOfDay[I, O](updateDate)(setTime)
    updateTime
  }

  // NB: SQL specifies a function called `extract`, but that doesn't have comma-
  //     separated arguments. `date_part` is Postgres’ name for the same thing
  //     with commas.

  private def dateFunc(help: String, outputType: Type, f: JLocalDate => Data) =
    UnaryFunc(
      Mapping, help,
      outputType,
      Func.Input1(Type.HasDate),
      noSimplification,
      partialTyper[nat._1] {
        case Sized(Type.Const(DataDateTimeExtractors.CanLensDate(i))) => Type.Const(f(i.pos))
        case Sized(t) if Type.HasDate contains t => Type.Numeric
      },
      basicUntyper)

  private def timeFunc(help: String, outputType: Type, f: JLocalTime => Data) =
    UnaryFunc(
      Mapping, help,
      outputType,
      Func.Input1(Type.HasTime),
      noSimplification,
      partialTyper[nat._1] {
        case Sized(Type.Const(DataDateTimeExtractors.CanLensTime(i))) => Type.Const(f(i.pos))
        case Sized(t) if Type.HasTime contains t => Type.Numeric
      },
      basicUntyper)

  private def timeZoneFunc(help: String, f: ZoneOffset => Data) =
    UnaryFunc(
      Mapping, help,
      Type.Int,
      Func.Input1(Type.HasOffset),
      noSimplification,
      partialTyper[nat._1] {
        case Sized(Type.Const(DataDateTimeExtractors.CanLensTimeZone(i))) => Type.Const(f(i.pos))
        case Sized(t) if Type.HasOffset contains t => Type.Numeric
      },
      basicUntyper)

  val ExtractCentury = dateFunc(
    "Pulls out the century subfield from a date/time value (currently (year - 1)/100 + 1).",
    Type.Int,
    (extractCentury _).andThen(Data.Int(_)))

  val ExtractDayOfMonth = dateFunc(
    "Pulls out the day of month (`day`) subfield from a date/time value (1-31).",
    Type.Int,
    (extractDayOfMonth _).andThen(Data.Int(_)))

  val ExtractDecade = dateFunc(
    "Pulls out the decade subfield from a date/time value (year/10).",
    Type.Int,
    (extractDecade _).andThen(Data.Int(_)))

  val ExtractDayOfWeek = dateFunc(
    "Pulls out the day of week (`dow`) subfield from a date/time value " + "(Sunday: 0 to Saturday: 6).",
    Type.Int,
    (extractDayOfWeek  _).andThen(Data.Int(_)))

  val ExtractDayOfYear = dateFunc(
    "Pulls out the day of year (`doy`) subfield from a date/time value (1-365 or -366).",
    Type.Int,
    (extractDayOfYear  _).andThen(Data.Int(_)))

  val ExtractEpoch = UnaryFunc(
    Mapping,
    "Pulls out the epoch subfield from a datetime value with timezone offset. " +
    "This is the number of seconds since midnight, 1970-01-01.",
    Type.Dec,
    Func.Input1(Type.OffsetDateTime),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.OffsetDateTime(k))) => Type.Const(Data.Dec(time.extractEpoch(k)))
      case Sized(Type.OffsetDateTime) => Type.Dec
    },
    basicUntyper)

  val ExtractHour = timeFunc(
    "Pulls out the hour subfield from a date/time value (0-23).",
    Type.Int,
    (extractHour _).andThen(Data.Int(_)))

  val ExtractIsoDayOfWeek = dateFunc(
    "Pulls out the ISO day of week (`isodow`) subfield from a date/time value (Monday: 1 to Sunday: 7).",
    Type.Int,
    (extractIsoDayOfWeek _).andThen(Data.Int(_)))

  val ExtractIsoYear = dateFunc(
    "Pulls out the ISO year (`isoyear`) subfield from a date/time value (based on the first week (Monday is the first day of the week) containing Jan. 4).",
    Type.Int,
    (extractIsoYear _).andThen(Data.Int(_)))

  val ExtractMicrosecond = timeFunc(
    "Computes the microseconds of a date/time value (including seconds).",
    Type.Int, (extractMicrosecond _).andThen(Data.Int(_)))

  val ExtractMillennium = dateFunc(
    "Pulls out the millennium subfield from a date/time value (currently (year - 1)/1000 + 1).",
    Type.Int,
    (extractMillennium _).andThen(Data.Int(_)))

  val ExtractMillisecond = timeFunc(
    "Computes the milliseconds of a date/time value (including seconds).",
    Type.Int, (extractMillisecond _).andThen(Data.Int(_)))

  val ExtractMinute = timeFunc(
    "Pulls out the minute subfield from a date/time value (0-59).",
    Type.Int, (extractMinute _).andThen(Data.Int(_)))

  val ExtractMonth = dateFunc(
    "Pulls out the month subfield from a date/time value (1-12).",
    Type.Int,
    (extractMonth _).andThen(Data.Int(_)))

  val ExtractQuarter = dateFunc(
    "Pulls out the quarter subfield from a date/time value (1-4).",
    Type.Int,
    (extractQuarter _).andThen(Data.Int(_)))

  val ExtractSecond = timeFunc(
    "Pulls out the second subfield from a date/time value (0-59, with fractional parts).",
    Type.Dec,
    (extractSecond _).andThen(Data.Dec(_)))

  val ExtractTimeZone = timeZoneFunc(
    "Pulls out the timezone subfield from a date/time value (in seconds east of UTC).",
    (extractTimeZone _).andThen(Data.Int(_)))

  val ExtractTimeZoneHour = timeZoneFunc(
    "Pulls out the hour component of the timezone subfield from a date/time value.",
    (extractTimeZoneHour _).andThen(Data.Int(_)))

  val ExtractTimeZoneMinute = timeZoneFunc(
    "Pulls out the minute component of the timezone subfield from a date/time value.",
    (extractTimeZoneMinute _).andThen(Data.Int(_)))

  val ExtractWeek = dateFunc(
    "Pulls out the week subfield from a date/time value (1-53).",
    Type.Int,
    (extractWeek _).andThen(Data.Int(_)))

  val ExtractYear = dateFunc(
    "Pulls out the year subfield from a date/time value.",
    Type.Int,
    (extractYear _).andThen(Data.Int(_)))

  private def setTimeZone(help: String, out: Int => ZoneOffset, outTimeZone: (Int, ZoneOffset) => ZoneOffset) =
    BinaryFunc(
      Mapping, help,
      Type.Temporal,
      Func.Input2(Type.Temporal, Type.Int),
      noSimplification,
      partialTyper[nat._2] {
        case Sized(Type.OffsetDate | Type.OffsetDateTime | Type.OffsetTime, Type.Numeric) => Type.Numeric
        case Sized(Type.Const(DataDateTimeExtractors.CanLensTimeZone(i)), Type.Const(Data.Int(input))) =>
          Type.Const(i.peeks(outTimeZone(input.toInt, _)))
        case Sized(Type.Const(DataDateTimeExtractors.CanSetTimeZone(k)), Type.Const(Data.Int(input))) =>
          Type.Const(k(out(input.toInt)))
      },
      basicUntyper)

  // FIXME `ZoneOffset.ofTotalSeconds` throws an exception if the integer
  // input is not in the range [-64800, 64800]
  val SetTimeZone = setTimeZone(
    "Sets the timezone subfield in a date/time value (in seconds east of UTC).",
    ZoneOffset.ofTotalSeconds,
    (i, _) => ZoneOffset.ofTotalSeconds(i))

  val SetTimeZoneMinute = setTimeZone(
    "Sets the minute component of the timezone subfield in a date/time value.",
    ZoneOffset.ofHoursMinutes(0, _),
    (i, zo) => setTimeZoneMinute(zo, i))

  val SetTimeZoneHour = setTimeZone(
    "Sets the hour component of the timezone subfield in a date/time value.",
    ZoneOffset.ofHours,
    (i, zo) => setTimeZoneHour(zo, i))

  val Now = NullaryFunc(
    Mapping,
    "Returns the current datetime in the current time zone – this must always return the same value within the same execution of a query.",
    Type.OffsetDateTime,
    noSimplification)

  val NowTime = NullaryFunc(
    Mapping,
    "Returns the current time in the current time zone – this must always return the same value within the same execution of a query.",
    Type.OffsetTime,
    noSimplification)

  val NowDate = NullaryFunc(
    Mapping,
    "Returns the current date in the current time zone – this must always return the same value within the same execution of a query.",
    Type.OffsetDate,
    noSimplification)

  val CurrentTimeZone = NullaryFunc(
    Mapping,
    "Returns the current time zone offset in total seconds - this must always return the same value within the same execution of a query.",
    Type.Int,
    noSimplification)

  val OffsetDateTime = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DDTHH:MM:SS((+/-)HH[:MM[:SS]])/Z) to a timestamp value with a time zone offset. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.OffsetDateTime,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseOffsetDateTime(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.OffsetDateTime)
    },
    basicUntyper)

  val OffsetTime = UnaryFunc(
    Mapping,
    "Converts a string in the format (HH:MM:SS[.SSS]((+/-)HH:MM:SS)/Z) to a time value with a time zone offset. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.OffsetTime,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseOffsetTime(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.OffsetTime)
    },
    basicUntyper)

  val OffsetDate = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DD((+/-)HH:MM:SS)/Z) to a date value with a time zone offset. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.OffsetDate,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseOffsetDate(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.OffsetDate)
    },
    basicUntyper)

  val LocalDateTime = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DDTHH:MM:SS) to a date value paired with a time. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.LocalDateTime,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseLocalDateTime(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.LocalDateTime)
    },
    basicUntyper)

  val LocalTime = UnaryFunc(
    Mapping,
    "Converts a string in the format (HH:MM:SS[.SSS]) to a time value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.LocalTime,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseLocalTime(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.LocalTime)
    },
    basicUntyper)

  val LocalDate = UnaryFunc(
    Mapping,
    "Converts a string in the format (YYYY-MM-DD) to a date value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.LocalDate,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseLocalDate(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.LocalDate)
    },
    basicUntyper)

  /**
    * TODO: document behavior change, now that years and months work
    */
  val Interval = UnaryFunc(
    Mapping,
    "Converts a string in the format (ISO 8601, e.g. P3DT12H30M15.0S) to an interval value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Interval,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseInterval(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str) => success(Type.Interval)
    },
    basicUntyper)

  /**
   * TODO: document behavior change, `StartOfDay` only makes `OffsetDateTime`s out of other `OffsetDateTime`s.
   */
  val StartOfDay = UnaryFunc(
    Mapping,
    "Converts a DateTime or Date to a DateTime at the start of that day.",
    Type.LocalDateTime ⨿ Type.OffsetDateTime,
    Func.Input1(Type.HasDate),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(CanAddTime(f))) =>
        success(Type.Const(f(JLocalTime.MIN)))
      case Sized(Type.LocalDate | Type.LocalDateTime) =>
        success(Type.LocalDateTime)
      case Sized(Type.OffsetDate | Type.OffsetDateTime) =>
        success(Type.OffsetDateTime)
    },
    partialUntyper[nat._1] {
      case Type.OffsetDateTime => Func.Input1(Type.OffsetDate ⨿ Type.OffsetDateTime)
      case Type.LocalDateTime  => Func.Input1(Type.LocalDate ⨿ Type.LocalDateTime)
    })

  val TimeOfDay = UnaryFunc(
    Mapping,
    "Extracts the time of day from a datetime value. Preserves time zone information.",
    Type.LocalTime ⨿ Type.OffsetTime,
    Func.Input1(Type.LocalDateTime ⨿ Type.OffsetDateTime),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.OffsetDateTime(odt))) => Type.Const(Data.OffsetTime(odt.toOffsetTime))
      case Sized(Type.Const(Data.LocalDateTime(ldt))) => Type.Const(Data.LocalTime(ldt.toLocalTime))
      case Sized(Type.LocalDateTime) => Type.LocalTime
      case Sized(Type.OffsetDateTime) => Type.OffsetTime
    },
    partialUntyper[nat._1] {
      case Type.OffsetTime => Func.Input1(Type.OffsetDateTime)
      case Type.LocalTime => Func.Input1(Type.LocalDateTime)
    })

  val ToTimestamp = UnaryFunc(
    Mapping,
    "Converts an integer epoch time value (i.e. milliseconds since 1 Jan. 1970, UTC) to a timestamp constant.",
    Type.OffsetDateTime,
    Func.Input1(Type.Int),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Int(millis))) => Type.Const(Data.OffsetDateTime(JOffsetDateTime.ofInstant(Instant.ofEpochMilli(millis.toLong), ZoneOffset.UTC)))
      case Sized(Type.Int) => Type.OffsetDateTime
    },
    partialUntyperV[nat._1] {
      case Type.OffsetDateTime => success(Func.Input1(Type.Int))
    })

  val ToLocal = UnaryFunc(
    Mapping,
    "Removes the time zone offset from a date, time, or datetime value.",
    Type.LocalDateTime ⨿ Type.LocalTime ⨿ Type.LocalDate,
    Func.Input1(Type.OffsetDateTime ⨿ Type.OffsetTime ⨿ Type.OffsetDate),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.OffsetDate(od))) => Type.Const(Data.LocalDate(od.date))
      case Sized(Type.Const(Data.OffsetTime(ot))) => Type.Const(Data.LocalTime(ot.toLocalTime))
      case Sized(Type.Const(Data.OffsetDateTime(odt))) => Type.Const(Data.LocalDateTime(odt.toLocalDateTime))
      case Sized(Type.OffsetDateTime) => Type.LocalDateTime
      case Sized(Type.OffsetTime) => Type.LocalTime
      case Sized(Type.OffsetDate) => Type.LocalDate
    },
    partialUntyperV[nat._1] {
      case Type.LocalDateTime => success(Func.Input1(Type.OffsetDateTime))
      case Type.LocalTime => success(Func.Input1(Type.OffsetTime))
      case Type.LocalDate => success(Func.Input1(Type.OffsetDate))
    })
}

object DateLib extends DateLib
