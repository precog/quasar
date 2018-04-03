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
import quasar.time.DateGenerators._

import java.time._

import org.specs2.matcher.{Expectable, MatchResult, Matcher}

class DateTimeIntervalSpec extends quasar.Qspec {

  "parsing" should {
    "not parse just a P" in {
      DateTimeInterval.parse("P") shouldEqual None
    }

    "not parse just a fraction" in {
      DateTimeInterval.parse("PT.005S") shouldEqual None
    }

    "parse just a year" in {
      DateTimeInterval.parse("P1Y") shouldEqual
        Some(DateTimeInterval.make(1, 0, 0, 0, 0))
    }

    "parse just a month" in {
      DateTimeInterval.parse("P1M") shouldEqual
        Some(DateTimeInterval.make(0, 1, 0, 0, 0))
    }

    "parse just a week" in {
      DateTimeInterval.parse("P1W") shouldEqual
        Some(DateTimeInterval.make(0, 0, 7, 0, 0))
    }

    "parse just a day" in {
      DateTimeInterval.parse("P1D") shouldEqual
        Some(DateTimeInterval.make(0, 0, 1, 0, 0))
    }

    "parse just an hour" in {
      DateTimeInterval.parse("PT1H") shouldEqual
        Some(DateTimeInterval.make(0, 0, 0, 60L * 60, 0))
    }

    "parse just a minute" in {
      DateTimeInterval.parse("PT1M") shouldEqual
        Some(DateTimeInterval.make(0, 0, 0, 60L, 0))
    }

    "parse just a second" in {
      DateTimeInterval.parse("PT1S") shouldEqual
        Some(DateTimeInterval.make(0, 0, 0, 1, 0))
    }

    "parse a composite duration with every component" in {
      DateTimeInterval.parse("P1Y2M3W4DT5H6M7.8S") shouldEqual
        Some(DateTimeInterval.make(1, 2, 25, 18367L, 800000000))
    }

    "parse a negated composite duration with every component" in {
      DateTimeInterval.parse("-P1Y2M3W4DT5H6M7.8S") shouldEqual
        Some(DateTimeInterval.make(-1, -2, -25, -18367L, -800000000))
    }

    "parse a composite duration with every component negated" in {
      DateTimeInterval.parse("P-1Y-2M-3W-4DT-5H-6M-7.8S") shouldEqual
        Some(DateTimeInterval.make(-1, -2, -25, -18367L, -800000000))
    }

    "parse should handle negative nanos with seconds == 0" in {
      val str = "P2131840384M91413566DT-488689H-42M-0.917127768S"
      DateTimeInterval.parse(str).map(_.toString) shouldEqual Some(str)
    }.pendingUntilFixed("https://bugs.openjdk.java.net/browse/JDK-8054978")
  }

  "toString" should {
    "empty" in {
      val expected = "P0D"

      DateTimeInterval.make(0, 0, 0, 0L, 0).toString shouldEqual expected
      DateTimeInterval.zero.toString shouldEqual expected
    }

    "one year" in {
      val expected = "P1Y"

      DateTimeInterval.make(1, 0, 0, 0L, 0).toString shouldEqual expected
      DateTimeInterval.ofPeriod(Period.ofYears(1)).toString shouldEqual expected
    }

    "one month" in {
      val expected = "P1M"

      DateTimeInterval.make(0, 1, 0, 0L, 0).toString shouldEqual expected
      DateTimeInterval.ofPeriod(Period.ofMonths(1)).toString shouldEqual expected
    }

    "one day" in {
      val expected = "P1D"

      DateTimeInterval.make(0, 0, 1, 0L, 0).toString shouldEqual expected
      DateTimeInterval.ofPeriod(Period.ofDays(1)).toString shouldEqual expected
    }

    "one hour" in {
      val expected = "PT1H"

      DateTimeInterval.make(0, 0, 0, 3600L, 0).toString shouldEqual expected
      DateTimeInterval.ofDuration(Duration.ofSeconds(3600L)).toString shouldEqual expected
    }

    "one minute" in {
      val expected = "PT1M"

      DateTimeInterval.make(0, 0, 0, 60L, 0).toString shouldEqual expected
      DateTimeInterval.ofDuration(Duration.ofSeconds(60L)).toString shouldEqual expected
    }

    "one second" in {
      val expected = "PT1S"

      DateTimeInterval.make(0, 0, 0, 1L, 0).toString shouldEqual expected
      DateTimeInterval.ofDuration(Duration.ofSeconds(1L)).toString shouldEqual expected
    }

    "one tenth second" in {
      val expected = "PT0.1S"

      DateTimeInterval.make(0, 0, 0, 0L, 100000000).toString shouldEqual expected
      DateTimeInterval.ofDuration(Duration.ofNanos(100000000)).toString shouldEqual expected
    }

    "all components" in {
      val expected = "P1Y1M1DT1H1M1.1S"

      DateTimeInterval.make(1, 1, 1, 3661L, 100000000).toString shouldEqual expected
      DateTimeInterval(Period.of(1, 1, 1), Duration.ofSeconds(3661L, 100000000)).toString shouldEqual expected
    }

    "normalize" in {
      val expected = "P1Y14M400DT24H2.000000001S"
      DateTimeInterval.make(1, 14, 400, (24 * 60 * 60) + 1, 1000000001).toString shouldEqual expected
      DateTimeInterval(Period.of(1, 14, 400), Duration.ofSeconds((24 * 60 * 60) + 1, 1000000001)).toString shouldEqual expected
    }

    "negative nanos" in {
      val duration = Duration.ofSeconds(1, -100000000)
      val expected = "PT0.9S"

      duration.toString shouldEqual expected

      DateTimeInterval.make(0, 0, 0, 1L, -100000000).toString shouldEqual expected
      DateTimeInterval.ofDuration(duration).toString shouldEqual expected
    }

    "negative period" in {
      DateTimeInterval.ofPeriod(Period.of(-3, -4, -5)).toString shouldEqual "P-3Y-4M-5D"
    }

    "partially negative period" in {
      DateTimeInterval.ofPeriod(Period.of(-3, 4, -5)).toString shouldEqual "P-3Y4M-5D"
    }

    "negative duration" in {
      DateTimeInterval.ofDuration(Duration.ofSeconds(-32, -8492)).toString shouldEqual "PT-32.000008492S"
    }

    "negative period and negative duration" in {
      DateTimeInterval(
        Period.of(-3, 4, 5),
        Duration.ofSeconds(-32, -8492)).toString shouldEqual "P-3Y4M5DT-32.000008492S"
    }

    "positive period and negative duration" in {
      DateTimeInterval(
        Period.of(3, 4, 5),
        Duration.ofSeconds(-32, -8492)).toString shouldEqual "P3Y4M5DT-32.000008492S"
    }
  }

  "factories" should {
    "one year" in {
      DateTimeInterval.ofYears(1) shouldEqual DateTimeInterval.make(1, 0, 0, 0, 0)
    }

    "one month" in {
      DateTimeInterval.ofMonths(1) shouldEqual DateTimeInterval.make(0, 1, 0, 0, 0)
    }

    "one day" in {
      DateTimeInterval.ofDays(1) shouldEqual DateTimeInterval.make(0, 0, 1, 0, 0)
    }

    "one hour" in {
      DateTimeInterval.ofHours(1L) shouldEqual DateTimeInterval.make(0, 0, 0, 3600, 0)
    }

    "one minute" in {
      DateTimeInterval.ofMinutes(1L) shouldEqual DateTimeInterval.make(0, 0, 0, 60, 0)
    }

    "one second" in {
      DateTimeInterval.ofSeconds(1L) shouldEqual DateTimeInterval.make(0, 0, 0, 1, 0)
    }

    "one second and a half" in {
      DateTimeInterval.ofSecondsNanos(1L, 500000000L) shouldEqual DateTimeInterval.make(0, 0, 0, 1L, 500000000L)
    }

    "one half-second" in {
      DateTimeInterval.ofNanos(500000000L) shouldEqual DateTimeInterval.make(0, 0, 0, 0L, 500000000L)
    }

    "one millis" in {
      DateTimeInterval.ofMillis(1L) shouldEqual DateTimeInterval.make(0, 0, 0, 0L, 1000000L)
    }

    "one nano" in {
      DateTimeInterval.ofNanos(1L) shouldEqual DateTimeInterval.make(0, 0, 0, 0L, 1L)
    }

    "negative nanos" in {
      DateTimeInterval.ofNanos(-1L) shouldEqual DateTimeInterval.make(0, 0, 0, -1L, 999999999L)
    }
  }

  "multiply" >> {
    "simple" >> {
      DateTimeInterval.make(2, 2, 2, 2, 2).multiply(7) must_==
        DateTimeInterval.make(14, 14, 14, 14, 14)
    }

    "overflow" >> {
      DateTimeInterval.make(128, 8, 17, (24 * 60 * 60) - 1, 999999999).multiply(2) must_==
        DateTimeInterval.make(256, 16, 34, (24 * 60 * 60 * 2) - 1, 999999998)
    }

    "zero" >> prop { (factor: Int) =>
      DateTimeInterval.zero.multiply(factor) must_== DateTimeInterval.zero
    }
  }

  "plus/minus" >> {
    "simple plus" >> {
      DateTimeInterval.make(2, 2, 2, 2, 2).plus(DateTimeInterval.make(5, 5, 5, 5, 5)) must_==
        DateTimeInterval.make(7, 7, 7, 7, 7)
    }

    "plus overflow" >> {
      DateTimeInterval.make(2, 12, 31, (24 * 60 * 60) - 1, 999999999).plus(DateTimeInterval.make(2, 2, 2, 2, 2)) must_==
        DateTimeInterval.make(4, 14, 33, (24 * 60 * 60) + 2, 1)
    }

    "simple minus" >> {
      DateTimeInterval.make(2, 2, 2, 2, 2).minus(DateTimeInterval.make(5, 5, 5, 5, 5)) must_==
        DateTimeInterval.make(-3, -3, -3, -3, -3)
    }

    "any" >> prop { (i1: DateTimeInterval, i2: DateTimeInterval) =>
      i1.plus(i2).minus(i2) must_== i1
      i1.minus(i2).plus(i2) must_== i1
    }

    "associativity" >> prop { (i1: DateTimeInterval, i2: DateTimeInterval, i3: DateTimeInterval) =>
      i1.plus(i2).plus(i3) must_== i2.plus(i3).plus(i1)
    }

    "zero" >> prop { (i1: DateTimeInterval) =>
      DateTimeInterval.zero.plus(i1) must_== i1
      i1.minus(DateTimeInterval.zero) must_== i1
    }
  }

  "between" >> {
    val d1: LocalDate = LocalDate.of(1999, 12, 3)
    val d2: LocalDate = LocalDate.of(2004, 7, 21)

    val t1: LocalTime = LocalTime.of(14, 38, 17, 123456789)
    val t2: LocalTime = LocalTime.of(2, 59, 8, 38291)

    val dt1: LocalDateTime = LocalDateTime.of(d1, t1)
    val dt2: LocalDateTime = LocalDateTime.of(d2, t2)

    val o1: ZoneOffset = ZoneOffset.ofHoursMinutesSeconds(4, 49, 18)
    val o2: ZoneOffset = ZoneOffset.ofHoursMinutes(-8, -18)

    val od1: OffsetDate = OffsetDate(d1, o1)
    val od2: OffsetDate = OffsetDate(d2, o2)

    val ot1: OffsetTime = OffsetTime.of(t1, o1)
    val ot2: OffsetTime = OffsetTime.of(t2, o2)

    val odt1: OffsetDateTime = OffsetDateTime.of(d1, t1, o1)
    val odt2: OffsetDateTime = OffsetDateTime.of(d2, t2, o2)

    "compute difference between two LocalDate" >> {
      val diff1 = DateTimeInterval.betweenLocalDate(d1, d2)
      val diff2 = DateTimeInterval.betweenLocalDate(d2, d1)

      DateTimeInterval.addToLocalDate(d1, diff1) must_== d2
      DateTimeInterval.subtractFromLocalDate(d2, diff1) must_== d1

      DateTimeInterval.addToLocalDate(d2, diff2) must_== d1
      DateTimeInterval.subtractFromLocalDate(d1, diff2) must_== d2
    }

    "compute difference between two LocalTime" >> {
      val diff1 = DateTimeInterval.betweenLocalTime(t1, t2)
      val diff2 = DateTimeInterval.betweenLocalTime(t2, t1)

      DateTimeInterval.addToLocalTime(t1, diff1) must_== t2
      DateTimeInterval.subtractFromLocalTime(t2, diff1) must_== t1

      DateTimeInterval.addToLocalTime(t2, diff2) must_== t1
      DateTimeInterval.subtractFromLocalTime(t1, diff2) must_== t2
    }

    "compute difference between two LocalDateTime" >> {
      val diff1 = DateTimeInterval.betweenLocalDateTime(dt1, dt2)
      val diff2 = DateTimeInterval.betweenLocalDateTime(dt2, dt1)

      diff1.addToLocalDateTime(dt1) must_== dt2
      diff1.subtractFromLocalDateTime(dt2) must_== dt1

      diff2.addToLocalDateTime(dt2) must_== dt1
      diff2.subtractFromLocalDateTime(dt1) must_== dt2
    }

    "compute difference between two OffsetDate" >> {
      val diff1 = DateTimeInterval.betweenOffsetDate(od1, od2)
      val diff2 = DateTimeInterval.betweenOffsetDate(od2, od1)

      DateTimeInterval.addToOffsetDate(od1, diff1) must equalOffsetDate(od2)
      DateTimeInterval.subtractFromOffsetDate(od2, diff1) must equalOffsetDate(od1)

      DateTimeInterval.addToOffsetDate(od2, diff2) must equalOffsetDate(od1)
      DateTimeInterval.subtractFromOffsetDate(od1, diff2) must equalOffsetDate(od2)
    }

    "compute difference between two OffsetTime" >> {
      val diff1 = DateTimeInterval.betweenOffsetTime(ot1, ot2)
      val diff2 = DateTimeInterval.betweenOffsetTime(ot2, ot1)

      DateTimeInterval.addToOffsetTime(ot1, diff1) must equalOffsetTime(ot2)
      DateTimeInterval.subtractFromOffsetTime(ot2, diff1) must equalOffsetTime(ot1)

      DateTimeInterval.addToOffsetTime(ot2, diff2) must equalOffsetTime(ot1)
      DateTimeInterval.subtractFromOffsetTime(ot1, diff2) must equalOffsetTime(ot2)
    }

    "compute difference between two OffsetDateTime" >> {
      val diff1 = DateTimeInterval.betweenOffsetDateTime(odt1, odt2)
      val diff2 = DateTimeInterval.betweenOffsetDateTime(odt2, odt1)

      diff1.addToOffsetDateTime(odt1) must equalOffsetDateTime(odt2)
      diff1.subtractFromOffsetDateTime(odt2) must equalOffsetDateTime(odt1)

      diff2.addToOffsetDateTime(odt2) must equalOffsetDateTime(odt1)
      diff2.subtractFromOffsetDateTime(odt1) must equalOffsetDateTime(odt2)
    }
  }

  def equalOffsetDate(expected: OffsetDate): Matcher[OffsetDate] = {
    new Matcher[OffsetDate] {
      def apply[S <: OffsetDate](s: Expectable[S]): MatchResult[S] = {
        val msg: String = s"Actual: ${s.value}\nExpected: $expected\n"

        if (expected.date == s.value.date)
          success(msg, s)
        else
          failure(msg, s)
      }
    }
  }

  def equalOffsetTime(expected: OffsetTime): Matcher[OffsetTime] = {
    new Matcher[OffsetTime] {
      def apply[S <: OffsetTime](s: Expectable[S]): MatchResult[S] = {
        val msg: String = s"Actual: ${s.value}\nExpected: $expected\n"

        if (expected.isEqual(s.value))
          success(msg, s)
        else
          failure(msg, s)
      }
    }
  }

  def equalOffsetDateTime(expected: OffsetDateTime): Matcher[OffsetDateTime] = {
    new Matcher[OffsetDateTime] {
      def apply[S <: OffsetDateTime](s: Expectable[S]): MatchResult[S] = {
        val msg: String = s"Actual: ${s.value}\nExpected: $expected\n"

        if (expected.isEqual(s.value))
          success(msg, s)
        else
          failure(msg, s)
      }
    }
  }
}
