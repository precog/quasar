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

package quasar

import slamdata.Predef._

import java.time._
import java.time.temporal.{ChronoField, ChronoUnit}

import monocle.Lens

import scalaz.{Lens => _, _}
import Scalaz._

package object time {

  type LensDate[I] = Lens[I, LocalDate]
  type SetDate[I, O] = (I, LocalDate) => O
  type LensTime[I] = Lens[I, LocalTime]
  type SetTime[I, O] = (I, LocalTime) => O
  type LensTimeZone[I] = Lens[I, ZoneOffset]
  type SetTimeZone[I, O] = (I, ZoneOffset) => O
  type LensDateTime[I] = Lens[I, LocalDateTime]
  type SetDateTime[I, O] = (I, LocalDateTime) => O


  // Lens

  def toLensDateTime[I](ld: LensDate[I], lt: LensTime[I]): LensDateTime[I] =
    Lens[I, LocalDateTime](
      i => LocalDateTime.of(ld.get(i), lt.get(i)))(
      ldt => (i => ld.set(ldt.toLocalDate)(lt.set(ldt.toLocalTime)(i))))

  def toLensDate[I](ldt: LensDateTime[I]): LensDate[I] =
    ldt composeLens lensDateLocalDateTime

  def toLensTime[I](ldt: LensDateTime[I]): LensTime[I] =
    ldt composeLens lensTimeLocalDateTime

  val lensDateLocalDate: LensDate[LocalDate] = Lens.id[LocalDate]

  val lensDateLocalDateTime: LensDate[LocalDateTime] =
    Lens[LocalDateTime, LocalDate](
      _.toLocalDate)(
      ld => (ldt => LocalDateTime.of(ld, ldt.toLocalTime)))

  val lensDateOffsetDate: LensDate[OffsetDate] =
    Lens[OffsetDate, LocalDate](
      _.date)(
      ld => (od => OffsetDate(ld, od.offset)))

  val lensDateOffsetDateTime: LensDate[OffsetDateTime] =
    Lens[OffsetDateTime, LocalDate](
      _.toLocalDate)(
      ld => (odt => OffsetDateTime.of(ld, odt.toLocalTime, odt.getOffset)))

  val lensTimeLocalDateTime: LensTime[LocalDateTime] =
    Lens[LocalDateTime, LocalTime](
      _.toLocalTime)(
      lt => (ldt => LocalDateTime.of(ldt.toLocalDate, lt)))

  val lensTimeLocalTime: LensTime[LocalTime] = Lens.id[LocalTime]

  val lensTimeOffsetDateTime: LensTime[OffsetDateTime] =
    Lens[OffsetDateTime, LocalTime](
      _.toLocalTime)(
      lt => (odt => OffsetDateTime.of(odt.toLocalDate, lt, odt.getOffset)))

  val lensTimeOffsetTime: LensTime[OffsetTime] =
    Lens[OffsetTime, LocalTime](
      _.toLocalTime)(
      lt => (ot => OffsetTime.of(lt, ot.getOffset)))

  val lensDateTimeLocalDateTime = Lens.id[LocalDateTime]

  val lensDateTimeOffsetDateTime =
    toLensDateTime(lensDateOffsetDateTime, lensTimeOffsetDateTime)

  val lensTimeZoneOffsetDateTime: LensTimeZone[OffsetDateTime] =
    Lens[OffsetDateTime, ZoneOffset](
      _.getOffset)(
      o => (odt => OffsetDateTime.of(odt.toLocalDateTime, o)))

  val lensTimeZoneOffsetTime: LensTimeZone[OffsetTime] =
    Lens[OffsetTime, ZoneOffset](
      _.getOffset)(
      o => (ot => OffsetTime.of(ot.toLocalTime, o)))

  val lensTimeZoneOffsetDate: LensTimeZone[OffsetDate] =
    Lens[OffsetDate, ZoneOffset](
      _.offset)(
      o => (od => OffsetDate(od.date, o)))


  // Set

  val setTimeZoneLocalTime: SetTimeZone[LocalTime, OffsetTime] = OffsetTime.of
  val setTimeZoneLocalDate: SetTimeZone[LocalDate, OffsetDate] = OffsetDate(_, _)
  val setTimeZoneLocalDateTime: SetTimeZone[LocalDateTime, OffsetDateTime] = OffsetDateTime.of

  val setDateLocalTime: SetDate[LocalTime, LocalDateTime] =
    (lt, ld) => LocalDateTime.of(ld, lt)

  val setTimeLocalDate: SetTime[LocalDate, LocalDateTime] =
    (ld, lt) => LocalDateTime.of(ld, lt)

  val setDateOffsetTime: SetDate[OffsetTime, OffsetDateTime] =
    (ot, ld) => OffsetDateTime.of(ld, ot.toLocalTime, ot.getOffset)

  val setTimeOffsetDate: SetTime[OffsetDate, OffsetDateTime] =
    (od, lt) => OffsetDateTime.of(od.date, lt, od.offset)

  def setTimeZoneMinute(offset: ZoneOffset, minutes: Int): ZoneOffset = {
    val totalSeconds: Int = offset.getTotalSeconds
    val minuteField: Int = (totalSeconds % 3600) / 60
    ZoneOffset.ofTotalSeconds(totalSeconds - (minuteField * 60) + (minutes * 60))
  }

  def setTimeZoneHour(offset: ZoneOffset, hours: Int): ZoneOffset =
    ZoneOffset.ofTotalSeconds(hours * 3600 + offset.getTotalSeconds % 3600)


  // Trunc

  def truncHour(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.HOURS)
  def truncMicrosecond(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.MICROS)
  def truncMillisecond(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.MILLIS)
  def truncMinute(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.MINUTES)
  def truncSecond(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.SECONDS)

  def truncCentury(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)
      .withMonth(1)
      .withYear((i.getYear / 100) * 100)

  def truncDecade(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)
      .withMonth(1)
      .withYear((i.getYear / 10) * 10)

  def truncMillennium(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)
      .withMonth(1)
      .withYear((i.getYear / 1000) * 1000)

  def truncMonth(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)

  def truncQuarter(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)
      .withMonth(i.getMonth.firstMonthOfQuarter().getValue)

  def truncWeek(i: LocalDate): LocalDate =
    i.`with`(java.time.DayOfWeek.MONDAY)

  def truncYear(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)
      .withMonth(1)

  def truncTime(part: TemporalPart, i: LocalTime): LocalTime =
      part match {
        case TemporalPart.Hour        => truncHour(i)
        case TemporalPart.Microsecond => truncMicrosecond(i)
        case TemporalPart.Millisecond => truncMillisecond(i)
        case TemporalPart.Minute      => truncMinute(i)
        case TemporalPart.Second      => truncSecond(i)
        case _                        => i
      }

  def truncDateTime(part: TemporalPart, i: LocalDateTime): LocalDateTime =
    if (part === TemporalPart.Day)
      LocalDateTime.of(i.toLocalDate, LocalTime.MIN)
    else if (part gt TemporalPart.Day)
      LocalDateTime.of(truncDate(part, i.toLocalDate), LocalTime.MIN)
    else
      LocalDateTime.of(i.toLocalDate, truncTime(part, i.toLocalTime))

  def truncDate(part: TemporalPart, date: LocalDate): LocalDate =
    part match {
      case TemporalPart.Century    => truncCentury(date)
      case TemporalPart.Decade     => truncDecade(date)
      case TemporalPart.Millennium => truncMillennium(date)
      case TemporalPart.Month      => truncMonth(date)
      case TemporalPart.Quarter    => truncQuarter(date)
      case TemporalPart.Week       => truncWeek(date)
      case TemporalPart.Year       => truncYear(date)
      case _                       => date
    }


  // Extract

  def extractCentury(ld: LocalDate): Long =
    (ld.getLong(ChronoField.YEAR_OF_ERA) - 1) / 100 + 1

  def extractDayOfMonth(ld: LocalDate): Long =
    ld.getLong(ChronoField.DAY_OF_MONTH)

  def extractDecade(ld: LocalDate): Long =
    ld.getLong(ChronoField.YEAR_OF_ERA) / 10

  def extractDayOfYear(ld: LocalDate): Long =
    ld.getLong(ChronoField.DAY_OF_YEAR)

  def extractEpoch(odt: OffsetDateTime): Double =
    odt.toEpochSecond + odt.getNano / 1000000000.0

  def extractMillennium(ld: LocalDate): Long =
    (ld.getLong(ChronoField.YEAR) - 1) / 1000 + 1

  def extractMonth(ld: LocalDate): Int = ld.getMonthValue

  def extractQuarter(ld: LocalDate): Long =
    java.lang.Math.ceil(ld.getMonthValue / 3.0).toLong

  def extractWeek(ld: LocalDate): Long =
    if (ld.getDayOfYear === 1) 53
    else ld.getLong(ChronoField.ALIGNED_WEEK_OF_YEAR)

  def extractYear(ld: LocalDate): Int = ld.getYear

  def extractIsoYear(ld: LocalDate): Long =
    if (ld.getDayOfYear === 1) ld.getLong(ChronoField.YEAR_OF_ERA) - 1
    else ld.getLong(ChronoField.YEAR_OF_ERA)

  def extractIsoDayOfWeek(ld: LocalDate): Int =
    ld.getDayOfWeek.getValue

  def extractDayOfWeek(ld: LocalDate): Int =
    ld.getDayOfWeek.getValue % 7

  def extractHour(lt: LocalTime): Int = lt.getHour

  def extractMicrosecond(lt: LocalTime): Long =
    lt.getLong(ChronoField.MICRO_OF_SECOND) + lt.getSecond * 1000000

  def extractMillisecond(lt: LocalTime): Long =
    lt.getLong(ChronoField.MILLI_OF_SECOND) + lt.getSecond * 1000

  def extractMinute(lt: LocalTime): Long =
    lt.getLong(ChronoField.MINUTE_OF_HOUR)

  def extractSecond(lt: LocalTime): BigDecimal =
    BigDecimal(lt.getNano) / 1000000000.0 + lt.getSecond

  def extractTimeZone(zo: ZoneOffset): Int = zo.getTotalSeconds

  def extractTimeZoneHour(zo: ZoneOffset): Int =
    zo.getTotalSeconds / 3600

  def extractTimeZoneMinute(zo: ZoneOffset): Int =
    (zo.getTotalSeconds / 60) % 60
}
