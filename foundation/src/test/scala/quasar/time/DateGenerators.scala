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

package quasar.time

import slamdata.Predef._
import quasar.pkg.tests._

import java.time.{
  Duration,
  Instant,
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
  Period,
  ZoneOffset
}
import java.time.temporal.ChronoField

import scala.math

trait DateGenerators {

  implicit val arbDuration: Arbitrary[Duration] = genDuration
  implicit val arbPeriod: Arbitrary[Period] = genPeriod
  implicit val arbDateTimeInterval: Arbitrary[DateTimeInterval] = genInterval

  implicit val arbInstant: Arbitrary[Instant] =
    Arbitrary((genEpochSeconds, genNanos) >> Instant.ofEpochSecond _)

  implicit val arbLocalDate: Arbitrary[JLocalDate] = genLocalDate
  implicit val arbLocalTime: Arbitrary[JLocalTime] = genLocalTime
  implicit val arbLocalDateTime: Arbitrary[JLocalDateTime] = genLocalDateTime

  implicit val arbOffsetDate: Arbitrary[OffsetDate] = genOffsetDate
  implicit val arbOffsetTime: Arbitrary[JOffsetTime] = genOffsetTime
  implicit val arbOffsetDateTime: Arbitrary[JOffsetDateTime] = genOffsetDateTime

  implicit val arbZoneOffset: Arbitrary[ZoneOffset] = genZoneOffset
  implicit val arbTemporalPart: Arbitrary[TemporalPart] = genTemporalPart

  private def genSeconds: Gen[Long] = genInt ^^ (_.toLong)
  private def genSecondOfDay: Gen[Long] = choose(0L, 24L * 60 * 60 - 1)
  private def genMillis: Gen[Long] = choose(0L, 999L)
  private def genNanos: Gen[Long] = choose(0L, 999999999L)
  private def genYears: Gen[Int] = choose(-9999999, 9999999)
  private def genMonths: Gen[Int] = choose(-100, 100)
  private def genDays: Gen[Int] = choose(-1000, 1000)
  private def genEpochSeconds: Gen[Long] =
    choose(Instant.MIN.getEpochSecond() + 1, Instant.MAX.getEpochSecond - 1)

  // these are adjusted so that LocalDate.ofEpochDay(minLocalEpochDay) - genYears can't go below the
  // minimum year of LocalDate, vice versa for maxLocalEpochDay
  val minLocalEpochDay: Long = 365L * -9999999
  val maxLocalEpochDay: Long = 365L * 9999999

  def genZoneOffset: Gen[ZoneOffset] =
    Gen.choose[Int](ZoneOffset.MIN.getTotalSeconds, ZoneOffset.MAX.getTotalSeconds).map(ZoneOffset.ofTotalSeconds)

  def genLocalTime: Gen[JLocalTime] =
    Gen.choose[Long](ChronoField.NANO_OF_DAY.range().getLargestMinimum, ChronoField.NANO_OF_DAY.range().getMaximum).map(JLocalTime.ofNanoOfDay)

  def genLocalDate: Gen[JLocalDate] =
    Gen.choose[Long](minLocalEpochDay, maxLocalEpochDay).map(JLocalDate.ofEpochDay)

  def genOffsetDateTime: Gen[JOffsetDateTime] = for {
    datetime <- genLocalDateTime
    zo <- genZoneOffset
  } yield JOffsetDateTime.of(datetime, zo)

  def genOffsetTime: Gen[JOffsetTime] = for {
    time <- genLocalTime
    zo <- genZoneOffset
  } yield JOffsetTime.of(time, zo)

  def genOffsetDate: Gen[OffsetDate] = for {
    date <- genLocalDate
    zo <- genZoneOffset
  } yield OffsetDate(date, zo)

  def genInterval: Gen[DateTimeInterval] =
    Gen.oneOf(genDateTimeInterval, genTimeInterval, genDateInterval)

  // FIXME
  // only generate positive seconds until this is available (Java 9)
  // https://bugs.openjdk.java.net/browse/JDK-8054978
  def genDateTimeInterval: Gen[DateTimeInterval] =
    for {
      years <- genYears
      months <- genMonths
      days <- genDays
      seconds <- genSeconds
      nanos <- genNanos
    } yield DateTimeInterval.make(years, months, days, math.abs(seconds.toLong), math.abs(nanos))

  def genPeriod: Gen[Period] =
    for {
      years <- genYears
      months <- genMonths
      days <- genDays
    } yield Period.of(years, months, days)

  def genDateInterval: Gen[DateTimeInterval] =
    genPeriod.map(DateTimeInterval.ofPeriod)

  // FIXME
  // only generate positive seconds until this is available (Java 9)
  // https://bugs.openjdk.java.net/browse/JDK-8054978
  def genDuration: Gen[Duration] =
    for {
      seconds <- genSeconds
      nanos <- genNanos
    } yield Duration.ofSeconds(math.abs(seconds.toLong), math.abs(nanos))

  def genTimeInterval: Gen[DateTimeInterval] =
    genDuration.map(DateTimeInterval.ofDuration)

  def genLocalDateTime: Gen[JLocalDateTime] =
    for {
      time <- genLocalTime
      date <- genLocalDate
    } yield JLocalDateTime.of(date, time)

  def genTemporalPart: Gen[TemporalPart] = {
    import TemporalPart._

    Gen.oneOf(
      Century, Day, Decade, Hour, Microsecond, Millennium,
      Millisecond, Minute, Month, Quarter, Second, Week, Year)
  }
}

object DateGenerators extends DateGenerators
