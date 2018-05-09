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

import slamdata.Predef._, BigDecimal.RoundingMode

import quasar.{Data, Qspec, Type}
import quasar.DataGenerators.{dataArbitrary => _, _}
import quasar.frontend.logicalplan._
import quasar.pkg.tests._
import quasar.time.{
  truncDateTime,
  truncDate,
  truncTime,
  DateGenerators,
  DateTimeInterval,
  OffsetDate => QOffsetDate,
  TemporalPart
}

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
import java.time.temporal.ChronoUnit
import scala.collection.Traversable
import scala.math.abs
import scala.util.matching.Regex

import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute.Result
import org.specs2.matcher.{Expectable, Matcher}
import org.specs2.specification.core.Fragment
import org.scalacheck.{Arbitrary, Gen}
import scalaz._, Scalaz._

/** Abstract spec for the standard library, intended to be implemented for each
  * library implementation, of which there are one or more per backend.
  */
abstract class StdLibSpec extends Qspec {
  def isPrintableAscii(c: Char): Boolean = c >= '\u0020' && c <= '\u007e'
  def isPrintableAscii(s: String): Boolean = s.forall(isPrintableAscii)

  def beCloseTo(expected: Data): Matcher[Data] = new Matcher[Data] {
    def isClose(x: BigDecimal, y: BigDecimal, err: Double): Boolean =
      x == y || ((x - y).abs/(y.abs max err)).toDouble < err

    def apply[S <: Data](s: Expectable[S]) = {
      val v = s.value
      (v, expected) match {
        case (Data.Number(x), Data.Number(exp)) =>
          result(isClose(x, exp, 1e-9),
            s"$x is a Number and matches $exp",
            s"$x is a Number but does not match $exp", s)
        case _ =>
          result(Equal[Data].equal(v, expected),
            s"$v matches $expected",
            s"$v does not match $expected", s)
      }
    }
  }

  def tests(runner: StdLibTestRunner): Fragment = {
    import runner._

    implicit val arbBigInt = Arbitrary[BigInt] { runner.intDomain }
    implicit val arbBigDecimal = Arbitrary[BigDecimal] { runner.decDomain }
    implicit val arbString = Arbitrary[String] { runner.stringDomain }

    implicit val arbData = Arbitrary[Data] {
      Gen.oneOf(
        runner.intDomain.map(Data.Int(_)),
        runner.decDomain.map(Data.Dec(_)),
        runner.stringDomain.map(Data.Str(_)))
    }

    implicit val arbZoneOffset: Arbitrary[ZoneOffset] = Arbitrary[ZoneOffset](runner.timezoneDomain)

    implicit val arbLocalDate: Arbitrary[JLocalDate] = Arbitrary[JLocalDate](runner.dateDomain)
    implicit val arbDataLocalDate: Arbitrary[Data.LocalDate] = arbLocalDate ^^ Data.LocalDate

    implicit val arbLocalTime: Arbitrary[JLocalTime] = Arbitrary[JLocalTime](runner.timeDomain)
    implicit val arbDataLocalTime: Arbitrary[Data.LocalTime] = arbLocalTime ^^ Data.LocalTime

    implicit val arbLocalDateTime: Arbitrary[JLocalDateTime] =
      Arbitrary[JLocalDateTime]((runner.dateDomain, runner.timeDomain) >> JLocalDateTime.of)
    implicit val arbDataLocalDateTime: Arbitrary[Data.LocalDateTime] =
      arbLocalDateTime ^^ Data.LocalDateTime

    implicit val arbOffsetDate: Arbitrary[QOffsetDate] =
      Arbitrary[QOffsetDate]((runner.dateDomain, runner.timezoneDomain) >> (QOffsetDate(_, _)))
    implicit val arbDataOffsetDate: Arbitrary[Data.OffsetDate] =
      arbOffsetDate ^^ Data.OffsetDate

    implicit val arbOffsetTime: Arbitrary[JOffsetTime] =
      Arbitrary[JOffsetTime]((runner.timeDomain, runner.timezoneDomain) >> JOffsetTime.of)
    implicit val arbDataOffsetTime: Arbitrary[Data.OffsetTime] =
      arbOffsetTime ^^ Data.OffsetTime

    implicit val arbOffsetDateTime: Arbitrary[JOffsetDateTime] =
      Arbitrary[JOffsetDateTime]((arbLocalDateTime.gen, runner.timezoneDomain) >> JOffsetDateTime.of)
    implicit val arbDataOffsetDateTime: Arbitrary[Data.OffsetDateTime] =
      arbOffsetDateTime ^^ Data.OffsetDateTime

    implicit val arbInterval: Arbitrary[DateTimeInterval] =
      Arbitrary[DateTimeInterval](runner.intervalDomain)
    implicit val arbDataInterval: Arbitrary[Data.Interval] =
      arbInterval ^^ Data.Interval

    implicit val arbDuration: Arbitrary[Duration] = DateGenerators.arbDuration
    implicit val arbPeriod: Arbitrary[Period] = DateGenerators.arbPeriod

    def commute(
        prg: (Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan],
        arg1: Data, arg2: Data,
        expected: Data): Result =
      binary(prg, arg1, arg2, expected) and
        binary(prg, arg2, arg1, expected)

    "StringLib" >> {
      import StringLib._

      "Concat" >> {
        "any strings" >> prop { (str1: String, str2: String) =>
          binary(Concat(_, _).embed, Data.Str(str1), Data.Str(str2), Data.Str(str1 + str2))
        }
      }

      // NB: `Like` is simplified to `Search`

      "Search" >> {
        "find contents within string when case sensitive" >> {
          ternary(Search(_, _, _).embed, Data.Str("church"), Data.Str(".*ch.*"), Data.Bool(false), Data.Bool(true)) and
            ternary(Search(_, _, _).embed, Data.Str("China"), Data.Str("^Ch.*$"), Data.Bool(false), Data.Bool(true)) and
            ternary(Search(_, _, _).embed, Data.Str("matching"), Data.Str(".*ch.*"), Data.Bool(false), Data.Bool(true))
        }

        "reject a non-matching string when case sensitive" >> {
          ternary(Search(_, _, _).embed, Data.Str("church"), Data.Str("^bs.*$"), Data.Bool(false), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("china"), Data.Str("^bs.*$"), Data.Bool(false), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("matching"), Data.Str(".*bs.*"), Data.Bool(false), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("matching"), Data.Str(".*CH.*"), Data.Bool(false), Data.Bool(false))
        }

        "find contents within string when case insensitive" >> {
          ternary(Search(_, _, _).embed, Data.Str("Church"), Data.Str(".*ch.*"), Data.Bool(true), Data.Bool(true)) and
            ternary(Search(_, _, _).embed, Data.Str("cHina"), Data.Str("^ch.*$"), Data.Bool(true), Data.Bool(true)) and
            ternary(Search(_, _, _).embed, Data.Str("matCHing"), Data.Str(".*ch.*"), Data.Bool(true), Data.Bool(true))
        }

        "reject a non-matching string when case insensitive" >> {
          ternary(Search(_, _, _).embed, Data.Str("Church"), Data.Str("^bs.*$"), Data.Bool(true), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("cHina"), Data.Str("^bs.*$"), Data.Bool(true), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("matCHing"), Data.Str(".*bs.*"), Data.Bool(true), Data.Bool(false))
        }
      }

      "Length" >> {
        "multibyte chars" >> {
          unary(Length(_).embed, Data.Str("€1"), Data.Int(2))
        }
        "any string" >> prop { (str: String) =>
          unary(Length(_).embed, Data.Str(str), Data.Int(str.length))
        }
      }

      "Lower" >> {
        "any string" >> prop { (str: String) =>
          unary(Lower(_).embed, Data.Str(str), Data.Str(str.toLowerCase))
        }
      }

      "Upper" >> {
        "any string" >> prop { (str: String) =>
          unary(Upper(_).embed, Data.Str(str), Data.Str(str.toUpperCase))
        }
      }

      "Substring" >> {
        "simple" >> {
          // NB: not consistent with PostgreSQL, which is 1-based for `start`
          ternary(Substring(_, _, _).embed, Data.Str("Thomas"), Data.Int(1), Data.Int(3), Data.Str("hom"))
        }

        "multibyte chars" >> {
          ternary(Substring(_, _, _).embed, Data.Str("cafétéria"), Data.Int(3), Data.Int(1), Data.Str("é"))
        }

        "empty string and any offsets" >> prop { (start0: Int, length0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = start0 % 1000
          val length = length0 % 1000
          ternary(Substring(_, _, _).embed, Data.Str(""), Data.Int(start), Data.Int(length), Data.Str(""))
        }

        "any string with entire range" >> prop { (str: String) =>
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(0), Data.Int(str.length), Data.Str(str))
        }

        "any string with 0 length" >> prop { (str: String, start0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = start0 % 1000
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(start), Data.Int(0), Data.Str(""))
        }

        "any string with start > str.length" >> prop { (str: String) =>
          // restrict the range to something that will actually exercise the behavior
          val start = str.length + 1
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(start), Data.Int(str.length), Data.Str(""))
        }

        "any string with start < 0" >> prop { (str: String) =>
          // restrict the range to something that will actually exercise the behavior
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(-1), Data.Int(str.length), Data.Str(""))
        }

        "any string and offsets" >> prop { (str: String, start0: Int, length0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = (start0 % 1000)
          val length = (length0 % 1000)

          // NB: this is the MongoDB behavior, for lack of a better idea
          val expected = StringLib.safeSubstring(str, start, length)
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(start), Data.Int(length), Data.Str(expected))
        }
      }

      "Split" >> {
        "some string" >> {
          binary(Split(_, _).embed, Data.Str("some string"), Data.Str(" "), Data.Arr(List("some", "string").map(Data.Str(_))))
        }
        "some string by itself" >> {
          binary(Split(_, _).embed, Data.Str("some string"), Data.Str("some string"), Data.Arr(List("", "").map(Data.Str(_))))
        }
        "any string not containing delimiter" >> prop { (s: String, d: String) =>
          (!d.isEmpty && !s.contains(d)) ==>
            binary(Split(_, _).embed, Data.Str(s), Data.Str(d), Data.Arr(List(Data.Str(s))))
        }
        "any string with non-empty delimiter" >> prop { (s: String, d: String) =>
          !d.isEmpty ==>
            binary(Split(_, _).embed, Data.Str(s), Data.Str(d), Data.Arr(s.split(Regex.quote(d), -1).toList.map(Data.Str(_))))
        }
      }

      "Boolean" >> {
        "true" >> {
          unary(Boolean(_).embed, Data.Str("true"), Data.Bool(true))
        }

        "false" >> {
          unary(Boolean(_).embed, Data.Str("false"), Data.Bool(false))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Integer" >> {
        "any BigInt in the domain" >> prop { (x: BigInt) =>
          unary(Integer(_).embed, Data.Str(x.toString), Data.Int(x))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Decimal" >> {
        "any BigDecimal in the domain" >> prop { (x: BigDecimal) =>
          unary(Decimal(_).embed, Data.Str(x.toString), Data.Dec(x))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Null" >> {
        "null" >> {
          unary(Null(_).embed, Data.Str("null"), Data.Null)
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "ToString" >> {
        "string" >> prop { (str: String) =>
          unary(ToString(_).embed, Data.Str(str), Data.Str(str))
        }

        "null" >> {
          unary(ToString(_).embed, Data.Null, Data.Str("null"))
        }

        "true" >> {
          unary(ToString(_).embed, Data.Bool(true), Data.Str("true"))
        }

        "false" >> {
          unary(ToString(_).embed, Data.Bool(false), Data.Str("false"))
        }

        "int" >> prop { (x: BigInt) =>
          unary(ToString(_).embed, Data.Int(x), Data.Str(x.toString))
        }

        // TODO: re-parse and compare the resulting value approximately. It's
        // not reasonable to expect a perfect match on formatted values,
        // because of trailing zeros, round-off, and choive of notation.
        // "dec" >> prop { (x: BigDecimal) =>
        //   unary(ToString(_).embed, Data.Dec(x), Data.Str(x.toString))
        // }

        "OffsetDate" >> {
          def test(x: QOffsetDate) = unary(
            ToString(_).embed,
            Data.OffsetDate(x),
            Data.Str(x.toString))

          "any" >> prop (test(_: QOffsetDate))
        }

        "OffsetDateTime" >> {
          def test(x: JOffsetDateTime) = unary(
            ToString(_).embed,
            Data.OffsetDateTime(x),
            Data.Str(x.toString))

          "zero fractional seconds" >> test(Instant.EPOCH.atOffset(ZoneOffset.UTC))

          "any" >> prop (test(_: JOffsetDateTime))
        }

        "OffsetTime" >> {
          def test(x: JOffsetTime) = unary(
            ToString(_).embed,
            Data.OffsetTime(x),
            Data.Str(x.toString))

          "zero fractional seconds" >> test(JOffsetTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))

          "any" >> prop (test(_: JOffsetTime))
        }

        "LocalDate" >> prop { (x: JLocalDate) =>
          unary(
            ToString(_).embed,
            Data.LocalDate(x),
            Data.Str(x.toString))
        }

        "LocalDateTime" >> {
          def test(x: JLocalDateTime) = unary(
            ToString(_).embed,
            Data.LocalDateTime(x),
            Data.Str(x.toString))

          "zero fractional seconds" >> test(JLocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))

          "any" >> prop (test(_: JLocalDateTime))
        }

        "LocalTime" >> {
          def test(x: JLocalTime) = unary(
            ToString(_).embed,
            Data.LocalTime(x),
            Data.Str(x.toString))

          "zero fractional seconds" >> test(JLocalTime.NOON)

          "any" >> prop (test(_: JLocalTime))
        }

        "Interval" >> prop { (x: DateTimeInterval) =>
          unary(ToString(_).embed, Data.Interval(x), Data.Str(x.toString))
        }
      }
    }

    "DateLib" >> {
      import DateLib._

      val genOffset = Gen.choose(-64800, 64800)
      val genOffsetMinute = Gen.choose(-59, 59)
      val genOffsetPositiveMinute = Gen.choose(0, 59)
      val genOffsetHour = Gen.choose(-18, 18)
      val genOffsetPositiveHour = Gen.choose(0, 17) // max is +18:00:00

      "SetTimeZone" >> {
        "LocalDateTime" >> prop { (offset: Int, dt: JLocalDateTime) =>
          binary(
            SetTimeZone(_, _).embed,
            Data.LocalDateTime(dt),
            Data.Int(offset),
            Data.OffsetDateTime(dt.atOffset(ZoneOffset.ofTotalSeconds(offset))))
        }.setGens(genOffset, arbLocalDateTime.gen)

        "LocalDate" >> prop { (offset: Int, dt: JLocalDate) =>
          binary(
            SetTimeZone(_, _).embed,
            Data.LocalDate(dt),
            Data.Int(offset),
            Data.OffsetDate(QOffsetDate(dt, ZoneOffset.ofTotalSeconds(offset))))
        }.setGens(genOffset, arbLocalDate.gen)

        "LocalTime" >> prop { (offset: Int, dt: JLocalTime) =>
          binary(
            SetTimeZone(_, _).embed,
            Data.LocalTime(dt),
            Data.Int(offset),
            Data.OffsetTime(dt.atOffset(ZoneOffset.ofTotalSeconds(offset))))
        }.setGens(genOffset, arbLocalTime.gen)

        "OffsetDateTime" >> prop { (offset: Int, dt: JOffsetDateTime) =>
          binary(
            SetTimeZone(_, _).embed,
            Data.OffsetDateTime(dt),
            Data.Int(offset),
            Data.OffsetDateTime(dt.withOffsetSameLocal(ZoneOffset.ofTotalSeconds(offset))))
        }.setGens(genOffset, arbOffsetDateTime.gen)

        "OffsetDate" >> prop { (offset: Int, dt: QOffsetDate) =>
          binary(
            SetTimeZone(_, _).embed,
            Data.OffsetDate(dt),
            Data.Int(offset),
            Data.OffsetDate(dt.copy(offset = ZoneOffset.ofTotalSeconds(offset))))
        }.setGens(genOffset, arbOffsetDate.gen)

        "OffsetTime" >> prop { (offset: Int, dt: JOffsetTime) =>
          binary(
            SetTimeZone(_, _).embed,
            Data.OffsetTime(dt),
            Data.Int(offset),
            Data.OffsetTime(dt.withOffsetSameLocal(ZoneOffset.ofTotalSeconds(offset))))
        }.setGens(genOffset, arbOffsetTime.gen)
      }

      "SetTimeZoneMinute" >> {
        "LocalDateTime" >> prop { (offset: Int, dt: JLocalDateTime) =>
          binary(
            SetTimeZoneMinute(_, _).embed,
            Data.LocalDateTime(dt),
            Data.Int(offset),
            Data.OffsetDateTime(dt.atOffset(ZoneOffset.ofHoursMinutes(0, offset))))
        }.setGens(genOffsetMinute, arbLocalDateTime.gen)

        "LocalDate" >> prop { (offset: Int, dt: JLocalDate) =>
          binary(
            SetTimeZoneMinute(_, _).embed,
            Data.LocalDate(dt),
            Data.Int(offset),
            Data.OffsetDate(QOffsetDate(dt, ZoneOffset.ofHoursMinutes(0, offset))))
        }.setGens(genOffsetMinute, arbLocalDate.gen)

        "LocalTime" >> prop { (offset: Int, dt: JLocalTime) =>
          binary(
            SetTimeZoneMinute(_, _).embed,
            Data.LocalTime(dt),
            Data.Int(offset),
            Data.OffsetTime(dt.atOffset(ZoneOffset.ofHoursMinutes(0, offset))))
        }.setGens(genOffsetMinute, arbLocalTime.gen)

        "OffsetDateTime" >> prop { (offset: Int, dt: JLocalDateTime) =>
          binary(
            SetTimeZoneMinute(_, _).embed,
            Data.OffsetDateTime(dt.atOffset(ZoneOffset.ofHoursMinutesSeconds(5, 17, 23))),
            Data.Int(offset),
            Data.OffsetDateTime(dt.atOffset(ZoneOffset.ofHoursMinutesSeconds(5, offset, 23))))
        }.setGens(genOffsetPositiveMinute, arbLocalDateTime.gen)

        "OffsetDate" >> prop { (offset: Int, dt: JLocalDate) =>
          binary(
            SetTimeZoneMinute(_, _).embed,
            Data.OffsetDate(QOffsetDate(dt, ZoneOffset.ofHoursMinutesSeconds(5, 17, 23))),
            Data.Int(offset),
            Data.OffsetDate(QOffsetDate(dt, ZoneOffset.ofHoursMinutesSeconds(5, offset, 23))))
        }.setGens(genOffsetPositiveMinute, arbLocalDate.gen)

        "OffsetTime" >> prop { (offset: Int, dt: JLocalTime) =>
          binary(
            SetTimeZoneMinute(_, _).embed,
            Data.OffsetTime(dt.atOffset(ZoneOffset.ofHoursMinutesSeconds(5, 17, 23))),
            Data.Int(offset),
            Data.OffsetTime(dt.atOffset(ZoneOffset.ofHoursMinutesSeconds(5, offset, 23))))
        }.setGens(genOffsetPositiveMinute, arbLocalTime.gen)
      }

      "SetTimeZoneHour" >> {
        "LocalDateTime" >> prop { (offset: Int, dt: JLocalDateTime) =>
          binary(
            SetTimeZoneHour(_, _).embed,
            Data.LocalDateTime(dt),
            Data.Int(offset),
            Data.OffsetDateTime(dt.atOffset(ZoneOffset.ofHoursMinutes(offset, 0))))
        }.setGens(genOffsetHour, arbLocalDateTime.gen)

        "LocalDate" >> prop { (offset: Int, dt: JLocalDate) =>
          binary(
            SetTimeZoneHour(_, _).embed,
            Data.LocalDate(dt),
            Data.Int(offset),
            Data.OffsetDate(QOffsetDate(dt, ZoneOffset.ofHoursMinutes(offset, 0))))
        }.setGens(genOffsetHour, arbLocalDate.gen)

        "LocalTime" >> prop { (offset: Int, dt: JLocalTime) =>
          binary(
            SetTimeZoneHour(_, _).embed,
            Data.LocalTime(dt),
            Data.Int(offset),
            Data.OffsetTime(dt.atOffset(ZoneOffset.ofHoursMinutes(offset, 0))))
        }.setGens(genOffsetHour, arbLocalTime.gen)

        "OffsetDateTime" >> prop { (offset: Int, dt: JLocalDateTime) =>
          binary(
            SetTimeZoneHour(_, _).embed,
            Data.OffsetDateTime(dt.atOffset(ZoneOffset.ofHoursMinutesSeconds(5, 17, 23))),
            Data.Int(offset),
            Data.OffsetDateTime(dt.atOffset(ZoneOffset.ofHoursMinutesSeconds(offset, 17, 23))))
        }.setGens(genOffsetPositiveHour, arbLocalDateTime.gen)

        "OffsetDate" >> prop { (offset: Int, dt: JLocalDate) =>
          binary(
            SetTimeZoneHour(_, _).embed,
            Data.OffsetDate(QOffsetDate(dt, ZoneOffset.ofHoursMinutesSeconds(5, 17, 23))),
            Data.Int(offset),
            Data.OffsetDate(QOffsetDate(dt, ZoneOffset.ofHoursMinutesSeconds(offset, 17, 23))))
        }.setGens(genOffsetPositiveHour, arbLocalDate.gen)

        "OffsetTime" >> prop { (offset: Int, dt: JLocalTime) =>
          binary(
            SetTimeZoneHour(_, _).embed,
            Data.OffsetTime(dt.atOffset(ZoneOffset.ofHoursMinutesSeconds(5, 17, 23))),
            Data.Int(offset),
            Data.OffsetTime(dt.atOffset(ZoneOffset.ofHoursMinutesSeconds(offset, 17, 23))))
        }.setGens(genOffsetPositiveHour, arbLocalTime.gen)
      }

      "ExtractCentury" >> {
        "LocalDate -1" >> prop { (x: JLocalDate) =>
          unary(ExtractCentury(_).embed, Data.LocalDate(x.withYear(-1)), Data.Int(1))
        }

        "LocalDate 1" >> prop { (x: JLocalDate) =>
          unary(ExtractCentury(_).embed, Data.LocalDate(x.withYear(1)), Data.Int(1))
        }

        "LocalDateTime 1" >> prop { (x: JLocalDateTime) =>
          unary(ExtractCentury(_).embed, Data.LocalDateTime(x.withYear(1)), Data.Int(1))
        }

        "OffsetDate 1" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractCentury(_).embed,
            Data.OffsetDate(QOffsetDate(x.withYear(1), offset)),
            Data.Int(1))
        }

        "OffsetDateTime 1" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractCentury(_).embed, Data.OffsetDateTime(x.withYear(1)), Data.Int(1))
        }

        "LocalDate 20" >> prop { (x: JLocalDate) =>
          unary(ExtractCentury(_).embed, Data.LocalDate(x.withYear(2000)), Data.Int(20))
        }

        "LocalDateTime 20" >> prop { (x: JLocalDateTime) =>
          unary(ExtractCentury(_).embed, Data.LocalDateTime(x.withYear(2000)), Data.Int(20))
        }

        "OffsetDate 20" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractCentury(_).embed,
            Data.OffsetDate(QOffsetDate(x.withYear(2000), offset)),
            Data.Int(20))
        }

        "OffsetDateTime 20" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractCentury(_).embed, Data.OffsetDateTime(x.withYear(2000)), Data.Int(20))
        }

        "LocalDate 21" >> prop { (x: JLocalDate) =>
          unary(ExtractCentury(_).embed, Data.LocalDate(x.withYear(2001)), Data.Int(21))
        }

        "LocalDateTime 21" >> prop { (x: JLocalDateTime) =>
          unary(ExtractCentury(_).embed, Data.LocalDateTime(x.withYear(2001)), Data.Int(21))
        }

        "OffsetDate 21" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractCentury(_).embed,
            Data.OffsetDate(QOffsetDate(x.withYear(2001), offset)),
            Data.Int(21))
        }

        "OffsetDateTime 21" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractCentury(_).embed, Data.OffsetDateTime(x.withYear(2001)), Data.Int(21))
        }
      }

      "ExtractDayOfMonth" >> {
        "LocalDate" >> prop { (x: JLocalDate) =>
          unary(ExtractDayOfMonth(_).embed, Data.LocalDate(x.withDayOfMonth(7)), Data.Int(7))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime) =>
          unary(ExtractDayOfMonth(_).embed, Data.LocalDateTime(x.withDayOfMonth(7)), Data.Int(7))
        }

        "OffsetDate" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractDayOfMonth(_).embed,
            Data.OffsetDate(QOffsetDate(x.withDayOfMonth(7), offset)),
            Data.Int(7))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractDayOfMonth(_).embed, Data.OffsetDateTime(x.withDayOfMonth(7)), Data.Int(7))
        }

        "LocalDate (leap year)" >> {
          unary(
            ExtractDayOfMonth(_).embed,
            Data.LocalDate(JLocalDate.of(2016, 2, 29)),
            Data.Int(29))
        }

        "LocalDateTime (leap year)" >> prop { (time: JLocalTime) =>
          unary(
            ExtractDayOfMonth(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(JLocalDate.of(2016, 2, 29), time)),
            Data.Int(29))
        }

        "OffsetDate (leap year)" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractDayOfMonth(_).embed,
            Data.OffsetDate(QOffsetDate(JLocalDate.of(2016, 2, 29), offset)),
            Data.Int(29))
        }

        "OffsetDateTime (leap year)" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractDayOfMonth(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(JLocalDate.of(2016, 2, 29), time, offset)),
            Data.Int(29))
        }
      }

      "ExtractDecade" >> {
        "LocalDate -1" >> prop { (x: JLocalDate) =>
          unary(ExtractDecade(_).embed, Data.LocalDate(x.withYear(-1)), Data.Int(0))
        }

        "LocalDate 1" >> prop { (x: JLocalDate) =>
          unary(ExtractDecade(_).embed, Data.LocalDate(x.withYear(1)), Data.Int(0))
        }

        "LocalDate" >> prop { (x: JLocalDate) =>
          unary(ExtractDecade(_).embed, Data.LocalDate(x.withYear(1999)), Data.Int(199))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime) =>
          unary(ExtractDecade(_).embed, Data.LocalDateTime(x.withYear(1999)), Data.Int(199))
        }

        "OffsetDate" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractDecade(_).embed,
            Data.OffsetDate(QOffsetDate(x.withYear(1999), offset)),
            Data.Int(199))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractDecade(_).embed, Data.OffsetDateTime(x.withYear(1999)), Data.Int(199))
        }
      }

      "ExtractDayOfWeek" >> {
        val day0: JLocalDate = JLocalDate.parse("2016-10-02") // Sunday
        val day6: JLocalDate = JLocalDate.parse("2016-10-08") // Saturday

        "LocalDate day 0" >> {
          unary(ExtractDayOfWeek(_).embed, Data.LocalDate(day0), Data.Int(0))
        }

        "OffsetDate day 0" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractDayOfWeek(_).embed,
            Data.OffsetDate(QOffsetDate(day0, offset)),
            Data.Int(0))
        }

        "LocalDateTime day 0" >> prop { (time: JLocalTime) =>
          unary(
            ExtractDayOfWeek(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(day0, time)),
            Data.Int(0))
        }

        "OffsetDateTime day 0" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractDayOfWeek(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(day0, time, offset)),
            Data.Int(0))
        }

        "LocalDate day 6" >> {
          unary(ExtractDayOfWeek(_).embed, Data.LocalDate(day6), Data.Int(6))
        }

        "OffsetDate day 6" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractDayOfWeek(_).embed,
            Data.OffsetDate(QOffsetDate(day6, offset)),
            Data.Int(6))
        }

        "LocalDateTime day 6" >> prop { (time: JLocalTime) =>
          unary(
            ExtractDayOfWeek(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(day6, time)),
            Data.Int(6))
        }

        "OffsetDateTime day 6" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractDayOfWeek(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(day6, time, offset)),
            Data.Int(6))
        }
      }

      "ExtractDayOfYear" >> {
        val day60: JLocalDate = JLocalDate.parse("2017-03-01") // non leap year
        val day61: JLocalDate = JLocalDate.parse("2016-03-01") // leap year

        "LocalDate day 60" >> {
          unary(ExtractDayOfYear(_).embed, Data.LocalDate(day60), Data.Int(60))
        }

        "OffsetDate day 60" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractDayOfYear(_).embed,
            Data.OffsetDate(QOffsetDate(day60, offset)),
            Data.Int(60))
        }

        "LocalDateTime day 60" >> prop { (time: JLocalTime) =>
          unary(
            ExtractDayOfYear(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(day60, time)),
            Data.Int(60))
        }

        "OffsetDateTime day 60" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractDayOfYear(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(day60, time, offset)),
            Data.Int(60))
        }

        "LocalDate day 61" >> {
          unary(ExtractDayOfYear(_).embed, Data.LocalDate(day61), Data.Int(61))
        }

        "OffsetDate day 61" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractDayOfYear(_).embed,
            Data.OffsetDate(QOffsetDate(day61, offset)),
            Data.Int(61))
        }

        "LocalDateTime day 61" >> prop { (time: JLocalTime) =>
          unary(
            ExtractDayOfYear(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(day61, time)),
            Data.Int(61))
        }

        "OffsetDateTime day 61" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractDayOfYear(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(day61, time, offset)),
            Data.Int(61))
        }
      }

      "ExtractEpoch" >> {
        "2016-09-29 12:34:56.789" >> {
          unary(ExtractEpoch(_).embed, Data.OffsetDateTime(JOffsetDateTime.parse("2016-09-29T12:34:56.789Z")), Data.Dec(1475152496.789))
        }
      }

      "ExtractHour" >> {
        "LocalTime" >> prop { (x: JLocalTime) =>
          unary(ExtractHour(_).embed, Data.LocalTime(x.withHour(7)), Data.Int(7))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime) =>
          unary(ExtractHour(_).embed, Data.LocalDateTime(x.withHour(7)), Data.Int(7))
        }

        "OffsetTime" >> prop { (x: JOffsetTime) =>
          unary(ExtractHour(_).embed, Data.OffsetTime(x.withHour(7)), Data.Int(7))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractHour(_).embed, Data.OffsetDateTime(x.withHour(7)), Data.Int(7))
        }
      }

      "ExtractIsoDayOfWeek" >> {
        val day1: JLocalDate = JLocalDate.parse("2016-09-26") // Monday
        val day7: JLocalDate = JLocalDate.parse("2016-10-02") // Sunday

        "LocalDate day 1" >> {
          unary(ExtractIsoDayOfWeek(_).embed, Data.LocalDate(day1), Data.Int(1))
        }

        "OffsetDate day 1" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractIsoDayOfWeek(_).embed,
            Data.OffsetDate(QOffsetDate(day1, offset)),
            Data.Int(1))
        }

        "LocalDateTime day 1" >> prop { (time: JLocalTime) =>
          unary(
            ExtractIsoDayOfWeek(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(day1, time)),
            Data.Int(1))
        }

        "OffsetDateTime day 1" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractIsoDayOfWeek(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(day1, time, offset)),
            Data.Int(1))
        }

        "LocalDate day 7" >> {
          unary(ExtractIsoDayOfWeek(_).embed, Data.LocalDate(day7), Data.Int(7))
        }

        "OffsetDate day 7" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractIsoDayOfWeek(_).embed,
            Data.OffsetDate(QOffsetDate(day7, offset)),
            Data.Int(7))
        }

        "LocalDateTime day 7" >> prop { (time: JLocalTime) =>
          unary(
            ExtractIsoDayOfWeek(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(day7, time)),
            Data.Int(7))
        }

        "OffsetDateTime day 7" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractIsoDayOfWeek(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(day7, time, offset)),
            Data.Int(7))
        }
      }

      "ExtractIsoYear" >> {
        val year2016: JLocalDate = JLocalDate.parse("2017-01-01") // day of year = 1
        val year2017: JLocalDate = JLocalDate.parse("2017-01-02") // first week containing Jan. 4

        "LocalDate 0000-2-3" >> {
          val year: JLocalDate = JLocalDate.of(0, 2, 3)
          unary(ExtractIsoYear(_).embed, Data.LocalDate(year), Data.Int(1))
        }

        "LocalDate -0001-2-3" >> {
          val year: JLocalDate = JLocalDate.of(-1, 2, 3)
          unary(ExtractIsoYear(_).embed, Data.LocalDate(year), Data.Int(2))
        }

        "LocalDate year 2016" >> {
          unary(ExtractIsoYear(_).embed, Data.LocalDate(year2016), Data.Int(2016))
        }

        "OffsetDate year 2016" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractIsoYear(_).embed,
            Data.OffsetDate(QOffsetDate(year2016, offset)),
            Data.Int(2016))
        }

        "LocalDateTime year 2016" >> prop { (time: JLocalTime) =>
          unary(
            ExtractIsoYear(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(year2016, time)),
            Data.Int(2016))
        }

        "OffsetDateTime year 2016" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractIsoYear(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(year2016, time, offset)),
            Data.Int(2016))
        }

        "LocalDate year 2017" >> {
          unary(ExtractIsoYear(_).embed, Data.LocalDate(year2017), Data.Int(2017))
        }

        "OffsetDate year 2017" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractIsoYear(_).embed,
            Data.OffsetDate(QOffsetDate(year2017, offset)),
            Data.Int(2017))
        }

        "LocalDateTime year 2017" >> prop { (time: JLocalTime) =>
          unary(
            ExtractIsoYear(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(year2017, time)),
            Data.Int(2017))
        }

        "OffsetDateTime year 2017" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractIsoYear(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(year2017, time, offset)),
            Data.Int(2017))
        }
      }

      "ExtractMicrosecond" >> {
        "LocalTime 0" >> prop { (x: JLocalTime) =>
          unary(
            ExtractMicrosecond(_).embed,
            Data.LocalTime(x.withSecond(0).withNano(0)),
            Data.Dec(0))
        }

        "LocalDateTime 0" >> prop { (x: JLocalDateTime) =>
          unary(
            ExtractMicrosecond(_).embed,
            Data.LocalDateTime(x.withSecond(0).withNano(0)),
            Data.Dec(0))
        }

        "OffsetTime 0" >> prop { (x: JOffsetTime) =>
          unary(
            ExtractMicrosecond(_).embed,
            Data.OffsetTime(x.withSecond(0).withNano(0)),
            Data.Dec(0))
        }

        "OffsetDateTime 0" >> prop { (x: JOffsetDateTime) =>
          unary(
            ExtractMicrosecond(_).embed,
            Data.OffsetDateTime(x.withSecond(0).withNano(0)),
            Data.Dec(0))
        }

        "LocalTime 56789000" >> prop { (x: JLocalTime) =>
          unary(
            ExtractMicrosecond(_).embed,
            Data.LocalTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56789000))
        }

        "LocalDateTime 56789000" >> prop { (x: JLocalDateTime) =>
          unary(
            ExtractMicrosecond(_).embed,
            Data.LocalDateTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56789000))
        }

        "OffsetTime 56789000" >> prop { (x: JOffsetTime) =>
          unary(
            ExtractMicrosecond(_).embed,
            Data.OffsetTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56789000))
        }

        "OffsetDateTime 56789000" >> prop { (x: JOffsetDateTime) =>
          unary(
            ExtractMicrosecond(_).embed,
            Data.OffsetDateTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56789000))
        }
      }

      "ExtractMillennium" >> {
        "LocalDate -1" >> prop { (x: JLocalDate) =>
          unary(ExtractMillennium(_).embed, Data.LocalDate(x.withYear(-1)), Data.Int(1))
        }

        "LocalDate 1" >> prop { (x: JLocalDate) =>
          unary(ExtractMillennium(_).embed, Data.LocalDate(x.withYear(1)), Data.Int(1))
        }

        "LocalDateTime 1" >> prop { (x: JLocalDateTime) =>
          unary(ExtractMillennium(_).embed, Data.LocalDateTime(x.withYear(1)), Data.Int(1))
        }

        "OffsetDate 1" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractMillennium(_).embed,
            Data.OffsetDate(QOffsetDate(x.withYear(1), offset)),
            Data.Int(1))
        }

        "OffsetDateTime 1" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractMillennium(_).embed, Data.OffsetDateTime(x.withYear(1)), Data.Int(1))
        }

        "LocalDate 2" >> prop { (x: JLocalDate) =>
          unary(ExtractMillennium(_).embed, Data.LocalDate(x.withYear(2000)), Data.Int(2))
        }

        "LocalDateTime 2" >> prop { (x: JLocalDateTime) =>
          unary(ExtractMillennium(_).embed, Data.LocalDateTime(x.withYear(2000)), Data.Int(2))
        }

        "OffsetDate 2" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractMillennium(_).embed,
            Data.OffsetDate(QOffsetDate(x.withYear(2000), offset)),
            Data.Int(2))
        }

        "OffsetDateTime 2" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractMillennium(_).embed, Data.OffsetDateTime(x.withYear(2000)), Data.Int(2))
        }

        "LocalDate 3" >> prop { (x: JLocalDate) =>
          unary(ExtractMillennium(_).embed, Data.LocalDate(x.withYear(2001)), Data.Int(3))
        }

        "LocalDateTime 3" >> prop { (x: JLocalDateTime) =>
          unary(ExtractMillennium(_).embed, Data.LocalDateTime(x.withYear(2001)), Data.Int(3))
        }

        "OffsetDate 3" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractMillennium(_).embed,
            Data.OffsetDate(QOffsetDate(x.withYear(2001), offset)),
            Data.Int(3))
        }

        "OffsetDateTime 3" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractMillennium(_).embed, Data.OffsetDateTime(x.withYear(2001)), Data.Int(3))
        }
      }

      "ExtractMillisecond" >> {
        "LocalTime 0" >> prop { (x: JLocalTime) =>
          unary(ExtractMillisecond(_).embed, Data.LocalTime(x.withSecond(0).withNano(0)), Data.Dec(0))
        }

        "LocalDateTime 0" >> prop { (x: JLocalDateTime) =>
          unary(ExtractMillisecond(_).embed, Data.LocalDateTime(x.withSecond(0).withNano(0)), Data.Dec(0))
        }

        "OffsetTime 0" >> prop { (x: JOffsetTime) =>
          unary(ExtractMillisecond(_).embed, Data.OffsetTime(x.withSecond(0).withNano(0)), Data.Dec(0))
        }

        "OffsetDateTime 0" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractMillisecond(_).embed, Data.OffsetDateTime(x.withSecond(0).withNano(0)), Data.Dec(0))
        }

        "LocalTime 56789" >> prop { (x: JLocalTime) =>
          unary(
            ExtractMillisecond(_).embed,
            Data.LocalTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56789))
        }

        "LocalDateTime 56789" >> prop { (x: JLocalDateTime) =>
          unary(
            ExtractMillisecond(_).embed,
            Data.LocalDateTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56789))
        }

        "OffsetTime 56789" >> prop { (x: JOffsetTime) =>
          unary(
            ExtractMillisecond(_).embed,
            Data.OffsetTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56789))
        }

        "OffsetDateTime 56789" >> prop { (x: JOffsetDateTime) =>
          unary(
            ExtractMillisecond(_).embed,
            Data.OffsetDateTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56789))
        }
      }

      "ExtractMinute" >> {
        "LocalTime" >> prop { (x: JLocalTime) =>
          unary(ExtractMinute(_).embed, Data.LocalTime(x.withMinute(7)), Data.Int(7))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime) =>
          unary(ExtractMinute(_).embed, Data.LocalDateTime(x.withMinute(7)), Data.Int(7))
        }

        "OffsetTime" >> prop { (x: JOffsetTime) =>
          unary(ExtractMinute(_).embed, Data.OffsetTime(x.withMinute(7)), Data.Int(7))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractMinute(_).embed, Data.OffsetDateTime(x.withMinute(7)), Data.Int(7))
        }
      }

      "ExtractMonth" >> {
        "LocalDate" >> prop { (x: JLocalDate) =>
          unary(ExtractMonth(_).embed, Data.LocalDate(x.withMonth(7)), Data.Int(7))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime) =>
          unary(ExtractMonth(_).embed, Data.LocalDateTime(x.withMonth(7)), Data.Int(7))
        }

        "OffsetDate" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractMonth(_).embed,
            Data.OffsetDate(QOffsetDate(x.withMonth(7), offset)),
            Data.Int(7))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractMonth(_).embed, Data.OffsetDateTime(x.withMonth(7)), Data.Int(7))
        }
      }

      "ExtractQuarter" >> {
        "LocalDate Q1" >> prop { (x: JLocalDate) =>
          unary(
            ExtractQuarter(_).embed,
            Data.LocalDate(x.withMonth(3).withDayOfMonth(31)),
            Data.Int(1))
        }

        "OffsetDate Q2" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractQuarter(_).embed,
            Data.OffsetDate(QOffsetDate(x.withMonth(6).withDayOfMonth(30), offset)),
            Data.Int(2))
        }

        "LocalDateTime Q3" >> prop { (x: JLocalDateTime) =>
          unary(
            ExtractQuarter(_).embed,
            Data.LocalDateTime(x.withMonth(9).withDayOfMonth(30)),
            Data.Int(3))
        }

        "OffsetDateTime Q4" >> prop { (x: JOffsetDateTime) =>
          unary(
            ExtractQuarter(_).embed,
            Data.OffsetDateTime(x.withMonth(12).withDayOfMonth(31)),
            Data.Int(4))
        }

        "LocalDate Q1 (leap year)" >> {
          unary(
            ExtractQuarter(_).embed,
            Data.LocalDate(JLocalDate.of(2016, 2, 29)),
            Data.Int(1))
        }

        "OffsetDate Q1 (leap year)" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractQuarter(_).embed,
            Data.OffsetDate(QOffsetDate(JLocalDate.of(2016, 2, 29), offset)),
            Data.Int(1))
        }

        "LocalDateTime Q1 (leap year)" >> prop { (time: JLocalTime) =>
          unary(
            ExtractQuarter(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(JLocalDate.of(2016, 2, 29), time)),
            Data.Int(1))
        }

        "OffsetDateTime Q1 (leap year)" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractQuarter(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(JLocalDate.of(2016, 2, 29), time, offset)),
            Data.Int(1))
        }
      }

      "ExtractSecond" >> {
        "LocalTime 0" >> prop { (x: JLocalTime) =>
          unary(ExtractSecond(_).embed, Data.LocalTime(x.withSecond(0).withNano(0)), Data.Dec(0))
        }

        "LocalDateTime 0" >> prop { (x: JLocalDateTime) =>
          unary(ExtractSecond(_).embed, Data.LocalDateTime(x.withSecond(0).withNano(0)), Data.Dec(0))
        }

        "OffsetTime 0" >> prop { (x: JOffsetTime) =>
          unary(ExtractSecond(_).embed, Data.OffsetTime(x.withSecond(0).withNano(0)), Data.Dec(0))
        }

        "OffsetDateTime 0" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractSecond(_).embed, Data.OffsetDateTime(x.withSecond(0).withNano(0)), Data.Dec(0))
        }

        "LocalTime 56.789" >> prop { (x: JLocalTime) =>
          unary(
            ExtractSecond(_).embed,
            Data.LocalTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56.789))
        }

        "LocalDateTime 56.789" >> prop { (x: JLocalDateTime) =>
          unary(
            ExtractSecond(_).embed,
            Data.LocalDateTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56.789))
        }

        "OffsetTime 56.789" >> prop { (x: JOffsetTime) =>
          unary(
            ExtractSecond(_).embed,
            Data.OffsetTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56.789))
        }

        "OffsetDateTime 56.789" >> prop { (x: JOffsetDateTime) =>
          unary(
            ExtractSecond(_).embed,
            Data.OffsetDateTime(x.withSecond(56).withNano(789000000)),
            Data.Dec(56.789))
        }
      }

      "ExtractTimeZone" >> {
        "OffsetDate" >> prop { (x: JLocalDate) =>
          unary(
            ExtractTimeZone(_).embed,
            Data.OffsetDate(QOffsetDate(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(11597))
        }

        "OffsetTime" >> prop { (x: JLocalTime) =>
          unary(
            ExtractTimeZone(_).embed,
            Data.OffsetTime(JOffsetTime.of(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(11597))
        }

        "OffsetDateTime" >> prop { (x: JLocalDateTime) =>
          unary(
            ExtractTimeZone(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(11597))
        }
      }

      "ExtractTimeZoneMinute" >> {
        "OffsetDate" >> prop { (x: JLocalDate) =>
          unary(
            ExtractTimeZoneMinute(_).embed,
            Data.OffsetDate(QOffsetDate(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(13))
        }

        "OffsetTime" >> prop { (x: JLocalTime) =>
          unary(
            ExtractTimeZoneMinute(_).embed,
            Data.OffsetTime(JOffsetTime.of(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(13))
        }

        "OffsetDateTime" >> prop { (x: JLocalDateTime) =>
          unary(
            ExtractTimeZoneMinute(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(13))
        }
      }

      "ExtractTimeZoneHour" >> {
        "OffsetDate" >> prop { (x: JLocalDate) =>
          unary(
            ExtractTimeZoneHour(_).embed,
            Data.OffsetDate(QOffsetDate(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(3))
        }

        "OffsetTime" >> prop { (x: JLocalTime) =>
          unary(
            ExtractTimeZoneHour(_).embed,
            Data.OffsetTime(JOffsetTime.of(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(3))
        }

        "OffsetDateTime" >> prop { (x: JLocalDateTime) =>
          unary(
            ExtractTimeZoneHour(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(x, ZoneOffset.ofHoursMinutesSeconds(3, 13, 17))),
            Data.Int(3))
        }
      }

      "ExtractWeek" >> {
        val week7: JLocalDate = JLocalDate.parse("2001-02-16")
        val week53: JLocalDate = JLocalDate.parse("2016-01-01") // day of year = 1

        "LocalDate week 7" >> {
          unary(ExtractWeek(_).embed, Data.LocalDate(week7), Data.Int(7))
        }

        "OffsetDate week 7" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractWeek(_).embed,
            Data.OffsetDate(QOffsetDate(week7, offset)),
            Data.Int(7))
        }

        "LocalDateTime week 7" >> prop { (time: JLocalTime) =>
          unary(
            ExtractWeek(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(week7, time)),
            Data.Int(7))
        }

        "OffsetDateTime week 7" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractWeek(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(week7, time, offset)),
            Data.Int(7))
        }

        "LocalDate week 53" >> {
          unary(ExtractWeek(_).embed, Data.LocalDate(week53), Data.Int(53))
        }

        "OffsetDate week 53" >> prop { (offset: ZoneOffset) =>
          unary(
            ExtractWeek(_).embed,
            Data.OffsetDate(QOffsetDate(week53, offset)),
            Data.Int(53))
        }

        "LocalDateTime week 53" >> prop { (time: JLocalTime) =>
          unary(
            ExtractWeek(_).embed,
            Data.LocalDateTime(JLocalDateTime.of(week53, time)),
            Data.Int(53))
        }

        "OffsetDateTime week 53" >> prop { (time: JLocalTime, offset: ZoneOffset) =>
          unary(
            ExtractWeek(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.of(week53, time, offset)),
            Data.Int(53))
        }
      }

      "ExtractYear" >> {
        "LocalDate" >> prop { (x: JLocalDate) =>
          unary(ExtractYear(_).embed, Data.LocalDate(x.withYear(1999)), Data.Int(1999))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime) =>
          unary(ExtractYear(_).embed, Data.LocalDateTime(x.withYear(1999)), Data.Int(1999))
        }

        "OffsetDate" >> prop { (x: JLocalDate, offset: ZoneOffset) =>
          unary(
            ExtractYear(_).embed,
            Data.OffsetDate(QOffsetDate(x.withYear(1999), offset)),
            Data.Int(1999))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime) =>
          unary(ExtractYear(_).embed, Data.OffsetDateTime(x.withYear(1999)), Data.Int(1999))
        }
      }

      "Interval" >> {
        unary(
          Interval(_).embed,
          Data.Str("P7Y2M4W3DT5H6M9.1409S"),
          Data.Interval(DateTimeInterval.make(7, 2, (4*7)+3, (5*60*60)+(6*60)+9, 140900000)))
      }

      "StartOfDay" >> {
        "LocalDateTime" >> prop { (x: JLocalDateTime) =>
          unary(
            StartOfDay(_).embed,
            Data.LocalDateTime(x),
            Data.LocalDateTime(truncDateTime(TemporalPart.Day, x)))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime) =>
          unary(
            StartOfDay(_).embed,
            Data.OffsetDateTime(x),
            Data.OffsetDateTime(JOffsetDateTime.of(truncDateTime(TemporalPart.Day, x.toLocalDateTime), x.getOffset)))
        }

        "LocalDate" >> prop { (x: JLocalDate) =>
          unary(
            StartOfDay(_).embed,
            Data.LocalDate(x),
            Data.LocalDateTime(JLocalDateTime.of(x, JLocalTime.MIN)))
        }

        "OffsetDate" >> prop { (x: QOffsetDate) =>
          unary(
            StartOfDay(_).embed,
            Data.OffsetDate(x),
            Data.OffsetDateTime(JOffsetDateTime.of(JLocalDateTime.of(x.date, JLocalTime.MIN), x.offset)))
        }
      }

      "ToTimestamp" >> {
        "epoch time" >> {
          unary(
            ToTimestamp(_).embed,
            Data.Int(0),
            Data.OffsetDateTime(JOffsetDateTime.parse("1970-01-01T00:00:00Z")))
        }

        "pre epoch" >> {
          unary(
            ToTimestamp(_).embed,
            Data.Int(BigInt(-1234567890011L)),
            Data.OffsetDateTime(JOffsetDateTime.parse("1930-11-18T00:28:29.989Z")))
        }

        "post epoch" >> {
          unary(
            ToTimestamp(_).embed,
            Data.Int(BigInt(1234567890011L)),
            Data.OffsetDateTime(JOffsetDateTime.parse("2009-02-13T23:31:30.011Z")))
        }
      }

      "ToLocal" >> {
        "OffsetDateTime" >> {
          unary(
            ToLocal(_).embed,
            Data.OffsetDateTime(JOffsetDateTime.parse("2009-02-13T23:31:30.011+12:15:18")),
            Data.LocalDateTime(JLocalDateTime.parse("2009-02-13T23:31:30.011")))
        }

        "OffsetDate" >> {
          unary(
            ToLocal(_).embed,
            Data.OffsetDate(QOffsetDate.parse("2009-02-13+12:15:18")),
            Data.LocalDate(JLocalDate.parse("2009-02-13")))
        }

        "OffsetTime" >> {
          unary(
            ToLocal(_).embed,
            Data.OffsetTime(JOffsetTime.parse("23:31:30.011+12:15:18")),
            Data.LocalTime(JLocalTime.parse("23:31:30.011")))
        }
      }

      "Now" >> {
        import MathLib.Subtract

        "Now" >> prop { (_: Int) =>
          val now = Now[Fix[LogicalPlan]]
          nullary(Subtract(now.embed, now.embed).embed, Data.Interval(DateTimeInterval.zero))
        }

        "NowTime" >> prop { (_: Int) =>
          val now = NowTime[Fix[LogicalPlan]]
          nullary(Subtract(now.embed, now.embed).embed, Data.Interval(DateTimeInterval.zero))
        }

        "NowDate" >> prop { (_: Int) =>
          val now = NowDate[Fix[LogicalPlan]]
          nullary(Subtract(now.embed, now.embed).embed, Data.Interval(DateTimeInterval.zero))
        }
      }

      "CurrentTimeZone" >> prop { (_: Int) =>
        val tz = CurrentTimeZone[Fix[LogicalPlan]]
        nullary(MathLib.Subtract(tz.embed, tz.embed).embed, Data.Interval(DateTimeInterval.zero))
      }

      "OffsetDate" >> prop { (v: QOffsetDate) =>
        unary(OffsetDate(_).embed, Data.Str(v.toString), Data.OffsetDate(v))
      }

      "OffsetDateTime" >> prop { (v: JOffsetDateTime) =>
        unary(OffsetDateTime(_).embed, Data.Str(v.toString), Data.OffsetDateTime(v))
      }

      "OffsetTime" >> prop { (v: JOffsetTime) =>
        unary(OffsetTime(_).embed, Data.Str(v.toString), Data.OffsetTime(v))
      }

      "LocalDate" >> {
        "0053-04-02" >> {
          unary(LocalDate(_).embed, Data.Str("0053-04-02"), Data.LocalDate(JLocalDate.of(53, 4, 2)))
        }

        "arbitrary string" >> prop { (v: JLocalDate) =>
          unary(LocalDate(_).embed, Data.Str(v.toString), Data.LocalDate(v))
        }
      }

      "LocalDateTime" >> {
        def test(x: JLocalDateTime) = unary(
          LocalDateTime(_).embed,
          Data.Str(x.toString),
          Data.LocalDateTime(x))

        "precision minutes" >> prop { (v: JLocalDateTime) => test(v.truncatedTo(ChronoUnit.MINUTES)) }

        "precision seconds" >> prop { (v: JLocalDateTime) => test(v.truncatedTo(ChronoUnit.SECONDS)) }

        "precision millis" >> prop { (v: JLocalDateTime) => test(v.truncatedTo(ChronoUnit.MILLIS)) }

        "precision micros" >> prop { (v: JLocalDateTime) => test(v.truncatedTo(ChronoUnit.MICROS)) }

        "full precision" >> prop (test(_: JLocalDateTime))
      }

      "LocalTime" >> {
        "07:47:18" >> {
          unary(LocalTime(_).embed, Data.Str("07:47:18"), Data.LocalTime(JLocalTime.parse("07:47:18")))
        }

        "07:47:18.041593" >> {
          unary(LocalTime(_).embed, Data.Str("07:47:18.041593"), Data.LocalTime(JLocalTime.parse("07:47:18.041593")))
        }

        "arbitrary string" >> prop { (v: JLocalTime) =>
          unary(LocalTime(_).embed, Data.Str(v.toString), Data.LocalTime(v))
        }
      }

      "TemporalTrunc" >> {
        def truncOffsetDateTime(p: TemporalPart, i: JOffsetDateTime): Result =
          unary(
            TemporalTrunc(p, _).embed,
            Data.OffsetDateTime(i),
            Data.OffsetDateTime(JOffsetDateTime.of(truncDateTime(p, i.toLocalDateTime), i.getOffset)))

        "Q#1966" >>
          truncOffsetDateTime(
            TemporalPart.Century,
            JOffsetDateTime.parse("2000-03-01T06:15:45.204Z"))

        "offset datetime Century" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Century, x)
        }

        "offset datetime Day" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Day, x)
        }

        "offset datetime Decade" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Decade, x)
        }

        "offset datetime Hour" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Hour, x)
        }

        "offset datetime Microsecond" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Microsecond, x)
        }

        "offset datetime Millennium" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Millennium, x)
        }

        "offset datetime Millisecond" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Millisecond, x)
        }

        "offset datetime Minute" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Minute, x)
        }

        "offset datetime Month" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Month, x)
        }

        "offset datetime Quarter" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Quarter, x)
        }

        "offset datetime Second" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Second, x)
        }

        "offset datetime Week" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Week, x)
        }

        "offset datetime Year" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Year, x)
        }

        def truncLocalDateTime(p: TemporalPart, i: JLocalDateTime): Result =
          unary(
            TemporalTrunc(p, _).embed,
            Data.LocalDateTime(i),
            Data.LocalDateTime(truncDateTime(p, i)))

        "datetime Century" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Century, x)
        }

        "datetime Day" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Day, x)
        }

        "datetime Decade" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Decade, x)
        }

        "datetime Hour" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Hour, x)
        }

        "datetime Microsecond" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Microsecond, x)
        }

        "datetime Millennium" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Millennium, x)
        }

        "datetime Millisecond" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Millisecond, x)
        }

        "datetime Minute" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Minute, x)
        }

        "datetime Month" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Month, x)
        }

        "datetime Quarter" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Quarter, x)
        }

        "datetime Second" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Second, x)
        }

        "datetime Week" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Week, x)
        }

        "datetime Year" >> prop { x: JLocalDateTime =>
          truncLocalDateTime(TemporalPart.Year, x)
        }

        def truncDateʹ(p: TemporalPart, d: JLocalDate): Result =
          unary(
            TemporalTrunc(p, _).embed,
            Data.LocalDate(d),
            Data.LocalDate(truncDate(p, d)))

        "date Century" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Century, d)
        }

        "date Day" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Day, d)
        }

        "date Decade" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Decade, d)
        }

        "date Hour" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Hour, d)
        }

        "date Microsecond" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Microsecond, d)
        }

        "date Millennium" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Millennium, d)
        }

        "date Millisecond" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Millisecond, d)
        }

        "date Minute" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Minute, d)
        }

        "date Month" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Month, d)
        }

        "date Quarter" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Quarter, d)
        }

        "date Second" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Second, d)
        }

        "date Week" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Week, d)
        }

        "date Year" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Year, d)
        }

        def truncLocalTimeʹ(p: TemporalPart, t: JLocalTime): Result =
          unary(
            TemporalTrunc(p, _).embed,
            Data.LocalTime(t),
            Data.LocalTime(truncTime(p, t)))

        "time Hour" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Hour, t)
        }

        "time Microsecond" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Microsecond, t)
        }

        "time Millisecond" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Millisecond, t)
        }

        "time Minute" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Minute, t)
        }

        "time Second" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Second, t)
        }
      }

      "TimeOfDay" >> {
        "OffsetDateTime" >> {
          val now = JOffsetDateTime.now
          val expected = now.toOffsetTime
          unary(TimeOfDay(_).embed, Data.OffsetDateTime(now), Data.OffsetTime(expected))
        }

        "LocalDateTime" >> {
          val now = JLocalDateTime.now
          val expected = now.toLocalTime
          unary(TimeOfDay(_).embed, Data.LocalDateTime(now), Data.LocalTime(expected))
        }
      }
    }

    "MathLib" >> {
      import MathLib._

      // NB: testing only (32-bit) ints, to avoid overflowing 64-bit longs
      // and the 53 bits of integer precision in a 64-bit double.

      // TODO: BigDecimals (which can under/overflow)
      // TODO: mixed BigInt/BigDecimal (which can explode)

      "Add" >> {
        "any ints" >> prop { (x: Int, y: Int) =>
          binary(Add(_, _).embed, Data.Int(x), Data.Int(y), Data.Int(x.toLong + y.toLong))
        }

        "any doubles" >> prop { (x: Double, y: Double) =>
          binary(Add(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x + y))
        }

        "mixed int/double" >> prop { (x: Int, y: Double) =>
          commute(Add(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(x + y))
        }

        "OffsetDate/Interval" >> prop { (x: QOffsetDate, i: Period) =>
          val result = DateTimeInterval.addToOffsetDate(x, i)
          commute(Add(_, _).embed, Data.OffsetDate(x), Data.Interval(DateTimeInterval.ofPeriod(i)), Data.OffsetDate(result))
        }

        "OffsetTime/Interval" >> prop { (x: JOffsetTime, i: Duration) =>
          val result = DateTimeInterval.addToOffsetTime(x, i)
          commute(Add(_, _).embed, Data.OffsetTime(x), Data.Interval(DateTimeInterval.ofDuration(i)), Data.OffsetTime(result))
        }

        "OffsetDateTime/Interval" >> prop { (x: JOffsetDateTime, i: DateTimeInterval) =>
          val result = i.addToOffsetDateTime(x)
          commute(Add(_, _).embed, Data.OffsetDateTime(x), Data.Interval(i), Data.OffsetDateTime(result))
        }

        "LocalDate/Interval" >> {
          "Feb 29" >> {
            val x: JLocalDate = JLocalDate.of(2016, 2, 29)
            val i: Period = Period.of(-101, -57, -4)
            val result: JLocalDate = DateTimeInterval.addToLocalDate(x, i)
            commute(Add(_, _).embed, Data.LocalDate(x), Data.Interval(DateTimeInterval.ofPeriod(i)), Data.LocalDate(result))
          }

          "any" >> prop { (x: JLocalDate, i: Period) =>
            val result: JLocalDate = DateTimeInterval.addToLocalDate(x, i)
            commute(Add(_, _).embed, Data.LocalDate(x), Data.Interval(DateTimeInterval.ofPeriod(i)), Data.LocalDate(result))
          }
        }

        "LocalTime/Interval" >> prop { (x: JLocalTime, i: Duration) =>
          val result: JLocalTime = DateTimeInterval.addToLocalTime(x, i)
          commute(Add(_, _).embed, Data.LocalTime(x), Data.Interval(DateTimeInterval.ofDuration(i)), Data.LocalTime(result))
        }

        "LocalDateTime/Interval" >> {
          "Feb 29" >> {
            val x: JLocalDateTime = JLocalDateTime.of(2016, 2, 29, 3, 7, 11)
            val i: DateTimeInterval = DateTimeInterval.make(-101, -57, -4, 5, 7)
            val result: JLocalDateTime = i.addToLocalDateTime(x)
            commute(Add(_, _).embed, Data.LocalDateTime(x), Data.Interval(i), Data.LocalDateTime(result))
          }

          "any" >> prop { (x: JLocalDateTime, i: DateTimeInterval) =>
            val result: JLocalDateTime = i.addToLocalDateTime(x)
            commute(Add(_, _).embed, Data.LocalDateTime(x), Data.Interval(i), Data.LocalDateTime(result))
          }
        }

        "add and subtract 1 month from Jan 31" >> {
          val x: JLocalDate = JLocalDate.of(2016, 1, 31)
          val i: DateTimeInterval = DateTimeInterval.ofMonths(1)

          binary(
            (date, interval) => Subtract(Add(date, interval).embed, interval).embed,
            Data.LocalDate(x),
            Data.Interval(i),
            Data.LocalDate(JLocalDate.of(2016, 1, 29)))
        }

        "add and subtract 1 year from Feb 29" >> {
          val x: JLocalDate = JLocalDate.of(2016, 2, 29)
          val i: DateTimeInterval = DateTimeInterval.ofYears(1)

          binary(
            (date, interval) => Subtract(Add(date, interval).embed, interval).embed,
            Data.LocalDate(x),
            Data.Interval(i),
            Data.LocalDate(JLocalDate.of(2016, 2, 28)))
        }
      }

      "Multiply" >> {
        "any ints" >> prop { (x: Int, y: Int) =>
          commute(Multiply(_, _).embed, Data.Int(x), Data.Int(y), Data.Int(x.toLong * y.toLong))
        }

        "Interval/Int" >> prop { (x: DateTimeInterval, y: Int) =>
          val expected = x.multiply(y)
          commute(Multiply(_, _).embed, Data.Interval(x), Data.Int(y), Data.Interval(expected))
        }.setGens(DateGenerators.genInterval, Gen.choose(-10, 10)) // avoid integer overflow

        // TODO: figure out what domain can be tested here (tends to overflow)
        // "any doubles" >> prop { (x: Double, y: Double) =>
        //   binary(Multiply(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x * y))
        // }

        // TODO: figure out what domain can be tested here
        // "mixed int/double" >> prop { (x: Int, y: Double) =>
        //   commute(Multiply(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(x * y))
        // }
      }

      "Power" >> {
        "Int to 0" >> prop { (x: BigInt) =>
          binary(Power(_, _).embed, Data.Int(x), Data.Int(0), Data.Int(1))
        }

        "Dec to 0" >> prop { (x: BigDecimal) =>
          binary(Power(_, _).embed, Data.Dec(x), Data.Int(0), Data.Int(1))
        }

        "Int to 1" >> prop { (x: BigInt) =>
          binary(Power(_, _).embed, Data.Int(x), Data.Int(1), Data.Int(x))
        }

        "Dec to 1" >> prop { (x: BigDecimal) =>
          binary(Power(_, _).embed, Data.Dec(x), Data.Int(1), Data.Dec(x))
        }

        "0 to small positive int" >> prop { (y0: Int) =>
          val y = abs(y0 % 10)
          y != 0 ==>
            binary(Power(_, _).embed, Data.Int(0), Data.Int(y), Data.Int(0))
        }

        // TODO: figure out what domain can be tested here (negatives?)
        // "0 to Dec" >> prop { (y: BigDecimal) =>
        //   y != 0 ==>
        //     binary(Power(_, _).embed, Data.Int(0), Data.Dec(y), Data.Int(0))
        // }

        "Int squared" >> prop { (x: Int) =>
          binary(Power(_, _).embed, Data.Int(x), Data.Int(2), Data.Int(x.toLong * x.toLong))
        }
      }

      "Subtract" >> {
        "any ints" >> prop { (x: Int, y: Int) =>
          binary(Subtract(_, _).embed, Data.Int(x), Data.Int(y), Data.Int(x.toLong - y.toLong))
        }

        "any doubles" >> prop { (x: Double, y: Double) =>
          binary(Subtract(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x - y))
        }

        "mixed int/double" >> prop { (x: Int, y: Double) =>
          binary(Subtract(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(x - y)) and
            binary(Subtract(_, _).embed, Data.Dec(y), Data.Int(x), Data.Dec(y - x))
        }

        "LocalDateTime/Interval" >> prop { (x: JLocalDateTime, y: DateTimeInterval) =>
          val expected = y.subtractFromLocalDateTime(x)
          binary(Subtract(_, _).embed, Data.LocalDateTime(x), Data.Interval(y), Data.LocalDateTime(expected))
        }

        "LocalDate/Interval" >> prop { (x: JLocalDate, y: Period) =>
          val expected = DateTimeInterval.subtractFromLocalDate(x, y)
          binary(Subtract(_, _).embed, Data.LocalDate(x), Data.Interval(DateTimeInterval.ofPeriod(y)), Data.LocalDate(expected))
        }

        "LocalTime/Interval" >> prop { (x: JLocalTime, y: Duration) =>
          val expected = DateTimeInterval.subtractFromLocalTime(x, y)
          binary(Subtract(_, _).embed, Data.LocalTime(x), Data.Interval(DateTimeInterval.ofDuration(y)), Data.LocalTime(expected))
        }

        "OffsetDateTime/Interval" >> prop { (x: JOffsetDateTime, y: DateTimeInterval) =>
          val expected = y.subtractFromOffsetDateTime(x)
          binary(Subtract(_, _).embed, Data.OffsetDateTime(x), Data.Interval(y), Data.OffsetDateTime(expected))
        }

        "OffsetDate/Interval" >> prop { (x: QOffsetDate, y: Period) =>
          val expected = DateTimeInterval.subtractFromOffsetDate(x, y)
          binary(Subtract(_, _).embed, Data.OffsetDate(x), Data.Interval(DateTimeInterval.ofPeriod(y)), Data.OffsetDate(expected))
        }

        "OffsetTime/Interval" >> prop { (x: JOffsetTime, y: Duration) =>
          val expected = DateTimeInterval.subtractFromOffsetTime(x, y)
          binary(Subtract(_, _).embed, Data.OffsetTime(x), Data.Interval(DateTimeInterval.ofDuration(y)), Data.OffsetTime(expected))
        }

        "LocalDateTime/LocalDateTime" >> prop { (x: JLocalDateTime, y: JLocalDateTime) =>
          val expected = DateTimeInterval.betweenLocalDateTime(x, y)
          binary(Subtract(_, _).embed, Data.LocalDateTime(x), Data.LocalDateTime(y), Data.Interval(expected))
        }

        "LocalDate/LocalDate" >> prop { (x: JLocalDate, y: JLocalDate) =>
          val expected = DateTimeInterval.ofPeriod(DateTimeInterval.betweenLocalDate(x, y))
          binary(Subtract(_, _).embed, Data.LocalDate(x), Data.LocalDate(y), Data.Interval(expected))
        }

        "LocalTime/LocalTime" >> prop { (x: JLocalTime, y: JLocalTime) =>
          val expected = DateTimeInterval.ofDuration(DateTimeInterval.betweenLocalTime(x, y))
          binary(Subtract(_, _).embed, Data.LocalTime(x), Data.LocalTime(y), Data.Interval(expected))
        }

        "OffsetDateTime/OffsetDateTime" >> prop { (x: JOffsetDateTime, y: JOffsetDateTime) =>
          val expected = DateTimeInterval.betweenOffsetDateTime(x, y)
          binary(Subtract(_, _).embed, Data.OffsetDateTime(x), Data.OffsetDateTime(y), Data.Interval(expected))
        }

        "OffsetDate/OffsetDate" >> prop { (x: QOffsetDate, y: QOffsetDate) =>
          val expected = DateTimeInterval.ofPeriod(DateTimeInterval.betweenOffsetDate(x, y))
          binary(Subtract(_, _).embed, Data.OffsetDate(x), Data.OffsetDate(y), Data.Interval(expected))
        }

        "OffsetTime/OffsetTime" >> prop { (x: JOffsetTime, y: JOffsetTime) =>
          val expected = DateTimeInterval.ofDuration(DateTimeInterval.betweenOffsetTime(x, y))
          binary(Subtract(_, _).embed, Data.OffsetTime(x), Data.OffsetTime(y), Data.Interval(expected))
        }

        "Interval/Interval" >> prop { (x: DateTimeInterval, y: DateTimeInterval) =>
          val expected = x.minus(y)
          binary(Subtract(_, _).embed, Data.Interval(x), Data.Interval(y), Data.Interval(expected))
        }
      }

      "Divide" >> {
        "any ints" >> prop { (x: Int, y: Int) =>
          y != 0 ==>
            binary(Divide(_, _).embed, Data.Int(x), Data.Int(y), Data.Dec(x.toDouble / y.toDouble))
        }

        "any int by 0" >> prop { (x: Int) =>
            binary(Divide(_, _).embed, Data.Int(x), Data.Int(0), Data.NA)
        }

        // TODO: figure out what domain can be tested here
        // "any doubles" >> prop { (x: Double, y: Double) =>
        //   binary(Divide(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x / y))
        // }

        // TODO: figure out what domain can be tested here
        // "mixed int/double" >> prop { (x: Int, y: Double) =>
        //   commute(Divide(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(x / y))
        // }
      }

      "Negate" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Negate(_).embed, Data.Int(x), Data.Int(-x))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Negate(_).embed, Data.Dec(x), Data.Dec(-x))
        }
      }

      "Abs" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Abs(_).embed, Data.Int(x), Data.Int(x.abs))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Abs(_).embed, Data.Dec(x), Data.Dec(x.abs))
        }
      }

      "Trunc" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Trunc(_).embed, Data.Int(x), Data.Int(x))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Trunc(_).embed, Data.Dec(x), Data.Dec(x.setScale(0, RoundingMode.DOWN)))
        }
      }

      "Ceil" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Ceil(_).embed, Data.Int(x), Data.Int(x))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Ceil(_).embed, Data.Dec(x), Data.Dec(x.setScale(0, RoundingMode.CEILING)))
        }
      }

      "Floor" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Floor(_).embed, Data.Int(x), Data.Int(x))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Floor(_).embed, Data.Dec(x), Data.Dec(x.setScale(0, RoundingMode.FLOOR)))
        }
      }

      "Round" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Round(_).embed, Data.Int(x), Data.Int(x))
        }

        "0.5 -> 0" >> {
          unary(Round(_).embed, Data.Dec(0.5), Data.Int(0))
        }

        "1.5 -> 2" >> {
          unary(Round(_).embed, Data.Dec(1.5), Data.Int(2))
        }

        "1.75 -> 2" >> {
          unary(Round(_).embed, Data.Dec(1.75), Data.Int(2))
        }

        "2.5 -> 2" >> {
          unary(Round(_).embed, Data.Dec(2.5), Data.Int(2))
        }

        "2.75 -> 3" >> {
          unary(Round(_).embed, Data.Dec(2.75), Data.Int(3))
        }

        "-0.5 -> 0" >> {
          unary(Round(_).embed, Data.Dec(-0.5), Data.Int(0))
        }

        "-1.5 -> -2" >> {
          unary(Round(_).embed, Data.Dec(-1.5), Data.Int(-2))
        }

        "-2.5 -> -2" >> {
          unary(Round(_).embed, Data.Dec(-2.5), Data.Int(-2))
        }
      }

      "CeilScale" >> {
        "scale 0" >> {
          val scale = 0

          "any Int" >> prop { (x: BigInt) =>
            binary(CeilScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 1" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Int(1))
          }

          "1.5 -> 2" >> {
            binary(CeilScale(_, _).embed, Data.Dec(1.5), Data.Int(scale), Data.Int(2))
          }

          "2.5 -> 3" >> {
            binary(CeilScale(_, _).embed, Data.Dec(2.5), Data.Int(scale), Data.Int(3))
          }

          "-0.5 -> 0" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Int(0))
          }

          "-1.5 -> -1" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-1.5), Data.Int(scale), Data.Int(-1))
          }

          "-2.5 -> -2" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-2.5), Data.Int(scale), Data.Int(-2))
          }
        }

        "scale 1" >> {
          val scale = 1

          "any Int" >> prop { (x: BigInt) =>
            binary(CeilScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "0.25 -> 0.3" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.25), Data.Int(scale), Data.Dec(0.3))
          }

          "-0.5 -> -0.5" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-0.25 -> 0.2" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.25), Data.Int(scale), Data.Dec(-0.2))
          }
        }

        "scale 2" >> {
          val scale = 2

          "any Int" >> prop { (x: BigInt) =>
            binary(CeilScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "0.25 -> 0.25" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.25), Data.Int(scale), Data.Dec(0.25))
          }

          "0.125 -> 0.13" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.125), Data.Int(scale), Data.Dec(0.13))
          }

          "-0.5 -> -0.5" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-0.25 -> 0.25" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.25), Data.Int(scale), Data.Dec(-0.25))
          }

          "-0.125 -> -0.12" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.125), Data.Int(scale), Data.Dec(-0.12))
          }
        }

        "scale -1" >> {
          val scale = -1

          "1 -> 10" >> {
            binary(CeilScale(_, _).embed, Data.Int(1), Data.Int(scale), Data.Int(10))
          }

          "10 -> 10" >> {
            binary(CeilScale(_, _).embed, Data.Int(10), Data.Int(scale), Data.Int(10))
          }

          "12345 -> 12350" >> {
            binary(CeilScale(_, _).embed, Data.Int(12345), Data.Int(scale), Data.Int(12350))
          }
        }
      }

      "FloorScale" >> {
        "scale 0" >> {
          val scale = 0

          "any Int" >> prop { (x: BigInt) =>
            binary(FloorScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Int(0))
          }

          "1.5 -> 1" >> {
            binary(FloorScale(_, _).embed, Data.Dec(1.5), Data.Int(scale), Data.Int(1))
          }

          "2.5 -> 2" >> {
            binary(FloorScale(_, _).embed, Data.Dec(2.5), Data.Int(scale), Data.Int(2))
          }

          "-0.5 -> -1" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Int(-1))
          }

          "-1.5 -> -2" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-1.5), Data.Int(scale), Data.Int(-2))
          }

          "-2.5 -> -3" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-2.5), Data.Int(scale), Data.Int(-3))
          }
        }

        "scale 1" >> {
          val scale = 1

          "any Int" >> prop { (x: BigInt) =>
            binary(FloorScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "0.25 -> 0.2" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.25), Data.Int(scale), Data.Dec(0.2))
          }

          "-0.5 -> -0.5" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-0.25 -> -0.3" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.25), Data.Int(scale), Data.Dec(-0.3))
          }
        }

        "scale 2" >> {
          val scale = 2

          "any Int" >> prop { (x: BigInt) =>
            binary(FloorScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "0.25 -> 0.25" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.25), Data.Int(scale), Data.Dec(0.25))
          }

          "0.125 -> 0.12" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.125), Data.Int(scale), Data.Dec(0.12))
          }

          "-0.5 -> -0.5" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-0.25 -> 0.25" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.25), Data.Int(scale), Data.Dec(-0.25))
          }

          "-0.125 -> -0.13" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.125), Data.Int(scale), Data.Dec(-0.13))
          }
        }

        "scale -1" >> {
          val scale = -1

          "1 -> 0" >> {
            binary(FloorScale(_, _).embed, Data.Int(1), Data.Int(scale), Data.Int(0))
          }

          "10 -> 10" >> {
            binary(FloorScale(_, _).embed, Data.Int(10), Data.Int(scale), Data.Int(10))
          }

          "12345 -> 12340" >> {
            binary(FloorScale(_, _).embed, Data.Int(12345), Data.Int(scale), Data.Int(12340))
          }
        }
      }

      "RoundScale" >> {
        "scale 0" >> {
          val scale = 0

          "any Int" >> prop { (x: BigInt) =>
            binary(RoundScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0" >> {
            binary(RoundScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Int(0))
          }

          "1.5 -> 2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.5), Data.Int(scale), Data.Int(2))
          }

          "1.75 -> 2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.75), Data.Int(scale), Data.Int(2))
          }

          "2.5 -> 2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(2.5), Data.Int(scale), Data.Int(2))
          }

          "2.75 -> 3" >> {
            binary(RoundScale(_, _).embed, Data.Dec(2.75), Data.Int(scale), Data.Int(3))
          }

          "-0.5 -> 0" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Int(0))
          }

          "-1.5 -> -2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-1.5), Data.Int(scale), Data.Int(-2))
          }

          "-2.5 -> -2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-2.5), Data.Int(scale), Data.Int(-2))
          }
        }

        "scale 1" >> {
          val scale = 1

          "any Int" >> prop { (x: BigInt) =>
            binary(RoundScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(RoundScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "1.5 -> 1.5" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.5), Data.Int(scale), Data.Dec(1.5))
          }

          "1.75 -> 1.8" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.75), Data.Int(scale), Data.Dec(1.8))
          }

          "1.65 -> 1.6" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.65), Data.Int(scale), Data.Dec(1.6))
          }

          "-0.5 -> -0.5" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-1.5 -> -1.5" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-1.5), Data.Int(scale), Data.Dec(-1.5))
          }

          "-1.75 -> -1.8" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-1.75), Data.Int(scale), Data.Dec(-1.8))
          }

          "-1.85 -> -1.8" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-1.85), Data.Int(scale), Data.Dec(-1.8))
          }
        }

        "scale -1" >> {
          val scale = -1

          "1 -> 0" >> {
            binary(RoundScale(_, _).embed, Data.Int(1), Data.Int(scale), Data.Int(0))
          }

          "5 -> 0" >> {
            binary(RoundScale(_, _).embed, Data.Int(5), Data.Int(scale), Data.Int(0))
          }

          "10 -> 10" >> {
            binary(RoundScale(_, _).embed, Data.Int(10), Data.Int(scale), Data.Int(10))
          }

          "12345 -> 12340" >> {
            binary(RoundScale(_, _).embed, Data.Int(12345), Data.Int(scale), Data.Int(12340))
          }

          "12335 -> 12340" >> {
            binary(RoundScale(_, _).embed, Data.Int(12335), Data.Int(scale), Data.Int(12340))
          }

          "123 -> 120" >> {
            binary(RoundScale(_, _).embed, Data.Int(123), Data.Int(scale), Data.Int(120))
          }
        }
      }

      "Modulo" >> {
        "any int by 1" >> prop { (x: Int) =>
          binary(Modulo(_, _).embed, Data.Int(x), Data.Int(1), Data.Int(0))
        }

        "any ints" >> prop { (x: Int, y: Int) =>
          y != 0 ==>
            binary(Modulo(_, _).embed, Data.Int(x), Data.Int(y), Data.Int(BigInt(x) % BigInt(y)))
        }

        // TODO analyze and optionally shortCircuit per connector
        // "any doubles" >> prop { (x: Double, y: Double) =>
        //   y != 0 ==>
        //     binary(Modulo(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(BigDecimal(x).remainder(BigDecimal(y))))
        // }
        //
        // "any big decimals" >> prop { (x: BigDecimal, y: BigDecimal) =>
        //   !y.equals(0.0) ==>
        //     binary(Modulo(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x.remainder(y)))
        // }
        //
        // "mixed int/double" >> prop { (x: Int, y: Double) =>
        //   y != 0 ==>
        //     binary(Modulo(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(BigDecimal(y).remainder(BigDecimal(x))))
        //   x != 0 ==>
        //     binary(Modulo(_, _).embed, Data.Dec(y), Data.Int(x), Data.Dec(BigDecimal(y).remainder(BigDecimal(x))))
        // }
      }
    }

    "RelationsLib" >> {
      import RelationsLib._

      "Eq" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          commute(Eq(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x == y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          commute(Eq(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x == y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          commute(Eq(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x == y))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime, y: JLocalDateTime) =>
          commute(Eq(_, _).embed, Data.LocalDateTime(x), Data.LocalDateTime(y), Data.Bool(x == y))
        }

        "LocalDate" >> prop { (x: JLocalDate, y: JLocalDate) =>
          commute(Eq(_, _).embed, Data.LocalDate(x), Data.LocalDate(y), Data.Bool(x == y))
        }

        "LocalTime" >> prop { (x: JLocalTime, y: JLocalTime) =>
          commute(Eq(_, _).embed, Data.LocalTime(x), Data.LocalTime(y), Data.Bool(x == y))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime, y: JOffsetDateTime) =>
          commute(Eq(_, _).embed, Data.OffsetDateTime(x), Data.OffsetDateTime(y), Data.Bool(x == y))
        }

        "OffsetDate" >> prop { (x: QOffsetDate, y: QOffsetDate) =>
          commute(Eq(_, _).embed, Data.OffsetDate(x), Data.OffsetDate(y), Data.Bool(x == y))
        }

        "OffsetTime" >> prop { (x: JOffsetTime, y: JOffsetTime) =>
          commute(Eq(_, _).embed, Data.OffsetTime(x), Data.OffsetTime(y), Data.Bool(x == y))
        }

        "Interval" >> prop { (x: DateTimeInterval, y: DateTimeInterval) =>
          commute(Eq(_, _).embed, Data.Interval(x), Data.Interval(y), Data.Bool(x == y))
        }

        "any value with self" >> prop { (x: Data) =>
          commute(Eq(_, _).embed, x, x, Data.Bool(true))
        }

        "any values with different types" >> prop { (x: Data, y: Data) =>
          // ...provided they are not both Numeric (Int | Dec)
          (x.dataType != y.dataType &&
            !((Type.Numeric contains x.dataType) &&
              (Type.Numeric contains y.dataType))) ==>
            commute(Eq(_, _).embed, x, y, Data.Bool(false))
        }
      }

      "Neq" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          commute(Neq(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x != y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          commute(Neq(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x != y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          commute(Neq(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x != y))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime, y: JLocalDateTime) =>
          commute(Neq(_, _).embed, Data.LocalDateTime(x), Data.LocalDateTime(y), Data.Bool(x != y))
        }

        "LocalDate" >> prop { (x: JLocalDate, y: JLocalDate) =>
          commute(Neq(_, _).embed, Data.LocalDate(x), Data.LocalDate(y), Data.Bool(x != y))
        }

        "LocalTime" >> prop { (x: JLocalTime, y: JLocalTime) =>
          commute(Neq(_, _).embed, Data.LocalTime(x), Data.LocalTime(y), Data.Bool(x != y))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime, y: JOffsetDateTime) =>
          commute(Neq(_, _).embed, Data.OffsetDateTime(x), Data.OffsetDateTime(y), Data.Bool(x != y))
        }

        "OffsetDate" >> prop { (x: QOffsetDate, y: QOffsetDate) =>
          commute(Neq(_, _).embed, Data.OffsetDate(x), Data.OffsetDate(y), Data.Bool(x != y))
        }

        "OffsetTime" >> prop { (x: JOffsetTime, y: JOffsetTime) =>
          commute(Neq(_, _).embed, Data.OffsetTime(x), Data.OffsetTime(y), Data.Bool(x != y))
        }

        "Interval" >> prop { (x: DateTimeInterval, y: DateTimeInterval) =>
          commute(Neq(_, _).embed, Data.Interval(x), Data.Interval(y), Data.Bool(x != y))
        }

        "any value with self" >> prop { (x: Data) =>
          commute(Neq(_, _).embed, x, x, Data.Bool(false))
        }

        "any values with different types" >> prop { (x: Data, y: Data) =>
          // ...provided they are not both Numeric (Int | Dec)
          (x.dataType != y.dataType &&
            !((Type.Numeric contains x.dataType) &&
              (Type.Numeric contains y.dataType))) ==>
            commute(Neq(_, _).embed, x, y, Data.Bool(true))
        }
      }

      "Lt" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Lt(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x < y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Lt(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x < y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Lt(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x < y))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime, y: JLocalDateTime) =>
          binary(Lt(_, _).embed, Data.LocalDateTime(x), Data.LocalDateTime(y), Data.Bool(x.compareTo(y) < 0))
        }

        "LocalDate" >> prop { (x: JLocalDate, y: JLocalDate) =>
          binary(Lt(_, _).embed, Data.LocalDate(x), Data.LocalDate(y), Data.Bool(x.compareTo(y) < 0))
        }

        "LocalTime" >> prop { (x: JLocalTime, y: JLocalTime) =>
          binary(Lt(_, _).embed, Data.LocalTime(x), Data.LocalTime(y), Data.Bool(x.compareTo(y) < 0))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime, y: JOffsetDateTime) =>
          binary(Lt(_, _).embed, Data.OffsetDateTime(x), Data.OffsetDateTime(y), Data.Bool(x.compareTo(y) < 0))
        }

        "OffsetDate" >> prop { (x: QOffsetDate, y: QOffsetDate) =>
          binary(Lt(_, _).embed, Data.OffsetDate(x), Data.OffsetDate(y), Data.Bool(x.compareTo(y) < 0))
        }

        "OffsetTime" >> prop { (x: JOffsetTime, y: JOffsetTime) =>
          binary(Lt(_, _).embed, Data.OffsetTime(x), Data.OffsetTime(y), Data.Bool(x.compareTo(y) < 0))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Lt(_, _).embed, x, x, Data.Bool(false))
        }
      }

      "Lte" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Lte(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x <= y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Lte(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x <= y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Lte(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x <= y))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime, y: JLocalDateTime) =>
          binary(Lte(_, _).embed, Data.LocalDateTime(x), Data.LocalDateTime(y), Data.Bool(x.compareTo(y) <= 0))
        }

        "LocalDate" >> prop { (x: JLocalDate, y: JLocalDate) =>
          binary(Lte(_, _).embed, Data.LocalDate(x), Data.LocalDate(y), Data.Bool(x.compareTo(y) <= 0))
        }

        "LocalTime" >> prop { (x: JLocalTime, y: JLocalTime) =>
          binary(Lte(_, _).embed, Data.LocalTime(x), Data.LocalTime(y), Data.Bool(x.compareTo(y) <= 0))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime, y: JOffsetDateTime) =>
          binary(Lte(_, _).embed, Data.OffsetDateTime(x), Data.OffsetDateTime(y), Data.Bool(x.compareTo(y) <= 0))
        }

        "OffsetDate" >> prop { (x: QOffsetDate, y: QOffsetDate) =>
          binary(Lte(_, _).embed, Data.OffsetDate(x), Data.OffsetDate(y), Data.Bool(x.compareTo(y) <= 0))
        }

        "OffsetTime" >> prop { (x: JOffsetTime, y: JOffsetTime) =>
          binary(Lte(_, _).embed, Data.OffsetTime(x), Data.OffsetTime(y), Data.Bool(x.compareTo(y) <= 0))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Lte(_, _).embed, x, x, Data.Bool(true))
        }
      }

      "Gt" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Gt(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x > y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Gt(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x > y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Gt(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x > y))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime, y: JLocalDateTime) =>
          binary(Gt(_, _).embed, Data.LocalDateTime(x), Data.LocalDateTime(y), Data.Bool(x.compareTo(y) > 0))
        }

        "LocalDate" >> prop { (x: JLocalDate, y: JLocalDate) =>
          binary(Gt(_, _).embed, Data.LocalDate(x), Data.LocalDate(y), Data.Bool(x.compareTo(y) > 0))
        }

        "LocalTime" >> prop { (x: JLocalTime, y: JLocalTime) =>
          binary(Gt(_, _).embed, Data.LocalTime(x), Data.LocalTime(y), Data.Bool(x.compareTo(y) > 0))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime, y: JOffsetDateTime) =>
          binary(Gt(_, _).embed, Data.OffsetDateTime(x), Data.OffsetDateTime(y), Data.Bool(x.compareTo(y) > 0))
        }

        "OffsetDate" >> prop { (x: QOffsetDate, y: QOffsetDate) =>
          binary(Gt(_, _).embed, Data.OffsetDate(x), Data.OffsetDate(y), Data.Bool(x.compareTo(y) > 0))
        }

        "OffsetTime" >> prop { (x: JOffsetTime, y: JOffsetTime) =>
          binary(Gt(_, _).embed, Data.OffsetTime(x), Data.OffsetTime(y), Data.Bool(x.compareTo(y) > 0))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Gt(_, _).embed, x, x, Data.Bool(false))
        }
      }

      "Gte" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Gte(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x >= y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Gte(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x >= y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Gte(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x >= y))
        }

        "LocalDateTime" >> prop { (x: JLocalDateTime, y: JLocalDateTime) =>
          binary(Gte(_, _).embed, Data.LocalDateTime(x), Data.LocalDateTime(y), Data.Bool(x.compareTo(y) >= 0))
        }

        "LocalDate" >> prop { (x: JLocalDate, y: JLocalDate) =>
          binary(Gte(_, _).embed, Data.LocalDate(x), Data.LocalDate(y), Data.Bool(x.compareTo(y) >= 0))
        }

        "LocalTime" >> prop { (x: JLocalTime, y: JLocalTime) =>
          binary(Gte(_, _).embed, Data.LocalTime(x), Data.LocalTime(y), Data.Bool(x.compareTo(y) >= 0))
        }

        "OffsetDateTime" >> prop { (x: JOffsetDateTime, y: JOffsetDateTime) =>
          binary(Gte(_, _).embed, Data.OffsetDateTime(x), Data.OffsetDateTime(y), Data.Bool(x.compareTo(y) >= 0))
        }

        "OffsetDate" >> prop { (x: QOffsetDate, y: QOffsetDate) =>
          binary(Gte(_, _).embed, Data.OffsetDate(x), Data.OffsetDate(y), Data.Bool(x.compareTo(y) >= 0))
        }

        "OffsetTime" >> prop { (x: JOffsetTime, y: JOffsetTime) =>
          binary(Gte(_, _).embed, Data.OffsetTime(x), Data.OffsetTime(y), Data.Bool(x.compareTo(y) >= 0))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Gte(_, _).embed, x, x, Data.Bool(true))
        }
      }

      "Between" >> {
        "any three Ints" >> prop { (lo: BigInt, mid: BigInt, hi: BigInt) =>
          val result = lo <= mid && mid <= hi
          ternary(Between(_, _, _).embed, Data.Int(mid), Data.Int(lo), Data.Int(hi), Data.Bool(result))
        }

        "any three Decs" >> prop { (lo: BigDecimal, mid: BigDecimal, hi: BigDecimal) =>
          val result = lo <= mid && mid <= hi
          ternary(Between(_, _, _).embed, Data.Dec(mid), Data.Dec(lo), Data.Dec(hi), Data.Bool(result))
        }

        "any three Strs" >> prop { (lo: String, mid: String, hi: String) =>
          val result = lo <= mid && mid <= hi
          ternary(Between(_, _, _).embed, Data.Str(mid), Data.Str(lo), Data.Str(hi), Data.Bool(result))
        }

        "any three Bools" >> prop { (lo: Boolean, mid: Boolean, hi: Boolean) =>
          val result = (lo, mid, hi) match {
            case (false, true, true)   => true
            case (false, false, true)  => true
            case (true, true, true)    => true
            case (false, false, false) => true
            case _ => false
          }
          ternary(Between(_, _, _).embed, Data.Bool(mid), Data.Bool(lo), Data.Bool(hi), Data.Bool(result))
        }

        "any three OffsetDateTimes" >> prop { (lo: JOffsetDateTime, mid: JOffsetDateTime, hi: JOffsetDateTime) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.OffsetDateTime(mid), Data.OffsetDateTime(lo), Data.OffsetDateTime(hi), Data.Bool(result))
        }

        "any three OffsetDates" >> prop { (lo: QOffsetDate, mid: QOffsetDate, hi: QOffsetDate) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.OffsetDate(mid), Data.OffsetDate(lo), Data.OffsetDate(hi), Data.Bool(result))
        }

        "any three OffsetTimes" >> prop { (lo: JOffsetTime, mid: JOffsetTime, hi: JOffsetTime) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.OffsetTime(mid), Data.OffsetTime(lo), Data.OffsetTime(hi), Data.Bool(result))
        }

        "any three LocalDateTimes" >> prop { (lo: JLocalDateTime, mid: JLocalDateTime, hi: JLocalDateTime) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.LocalDateTime(mid), Data.LocalDateTime(lo), Data.LocalDateTime(hi), Data.Bool(result))
        }

        "any three LocalDates" >> prop { (lo: JLocalDate, mid: JLocalDate, hi: JLocalDate) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.LocalDate(mid), Data.LocalDate(lo), Data.LocalDate(hi), Data.Bool(result))
        }

        "any three LocalTimes" >> prop { (lo: JLocalTime, mid: JLocalTime, hi: JLocalTime) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.LocalTime(mid), Data.LocalTime(lo), Data.LocalTime(hi), Data.Bool(result))
        }

        // TODO: Cross-type comparison
      }

      "IfUndefined" >> {
        """NA ?? 42""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.NA,
            Data.Int(42),
            Data.Int(42))
        }

        """1 ?? 2""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.Int(1),
            Data.Int(2),
            Data.Int(1))
        }

        """{"a": 1} ?? 2""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.Obj("a" -> Data.Int(1)),
            Data.Int(2),
            Data.Obj("a" -> Data.Int(1)))
        }

        """{"a": NA, "b": 2} ?? 3""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.Obj("a" -> Data.NA, "b" -> Data.Int(2)),
            Data.Int(3),
            Data.Obj("b" -> Data.Int(2)))
        }

        """[NA, 2] ?? 3""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.Arr(Data.NA :: Data.Int(2) :: Nil),
            Data.Int(3),
            Data.Arr(Data.Int(2) :: Nil))
        }
      }

      "And" >> {
        "false, false" >> {
          binary(And(_, _).embed, Data.Bool(false), Data.Bool(false), Data.Bool(false))
        }

        "false, true" >> {
          commute(And(_, _).embed, Data.Bool(false), Data.Bool(true), Data.Bool(false))
        }

        "true, true" >> {
          binary(And(_, _).embed, Data.Bool(true), Data.Bool(true), Data.Bool(true))
        }

        "undefined, false" >> {
          commute(And(_, _).embed, Data.NA, Data.Bool(false), Data.Bool(false))
        }

        "undefined, true" >> {
          commute(And(_, _).embed, Data.NA, Data.Bool(true), Data.NA)
        }
      }

      "Or" >> {
        "false, false" >> {
          binary(Or(_, _).embed, Data.Bool(false), Data.Bool(false), Data.Bool(false))
        }

        "false, true" >> {
          commute(Or(_, _).embed, Data.Bool(false), Data.Bool(true), Data.Bool(true))
        }

        "true, true" >> {
          binary(Or(_, _).embed, Data.Bool(true), Data.Bool(true), Data.Bool(true))
        }

        "undefined, false" >> {
          commute(Or(_, _).embed, Data.NA, Data.Bool(false), Data.NA)
        }

        "undefined, true" >> {
          commute(Or(_, _).embed, Data.NA, Data.Bool(true), Data.Bool(true))
        }
      }

      "Not" >> {
        "false" >> {
          unary(Not(_).embed, Data.Bool(false), Data.Bool(true))
        }

        "true" >> {
          unary(Not(_).embed, Data.Bool(true), Data.Bool(false))
        }
      }

      "Cond" >> {
        "true" >> prop { (x: Data, y: Data) =>
          ternary(Cond(_, _, _).embed, Data.Bool(true), x, y, x)
        }

        "false" >> prop { (x: Data, y: Data) =>
          ternary(Cond(_, _, _).embed, Data.Bool(false), x, y, y)
        }
      }
    }

    "StructuralLib" >> {
      import StructuralLib._

      // FIXME: No idea why this is necessary, but ScalaCheck arbContainer
      //        demands it and can't seem to find one in this context.
      implicit def listToTraversable[A](as: List[A]): Traversable[A] = as

      "ConcatOp" >> {
        "array  || array" >> prop { (xs: List[BigInt], ys: List[BigInt]) =>
          val (xints, yints) = (xs map (Data._int(_)), ys map (Data._int(_)))
          binary(ConcatOp(_, _).embed, Data._arr(xints), Data._arr(yints), Data._arr(xints ::: yints))
        }

        "array  || string" >> prop { (xs: List[BigInt], y: String) =>
          val (xints, ystrs) = (xs map (Data._int(_)), y.toList map (c => Data._str(c.toString)))
          binary(ConcatOp(_, _).embed, Data._arr(xints), Data._str(y), Data._arr(xints ::: ystrs))
        }

        "string || array" >> prop { (x: String, ys: List[BigInt]) =>
          val (xstrs, yints) = (x.toList map (c => Data._str(c.toString)), ys map (Data._int(_)))
          binary(ConcatOp(_, _).embed, Data._str(x), Data._arr(yints), Data._arr(xstrs ::: yints))
        }

        "string || string" >> prop { (x: String, y: String) =>
          binary(ConcatOp(_, _).embed, Data._str(x), Data._str(y), Data._str(x + y))
        }
      }

      "MapProject" >> {
        """({"a":1}).a""" >> {
          binary(
            MapProject(_, _).embed,
            Data.Obj("a" -> Data.Int(1)),
            Data.Str("a"),
            Data.Int(1))
        }

        """({"a":1, "b":2}).b""" >> {
          binary(
            MapProject(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("b"),
            Data.Int(2))
        }

        """({"a":1, "b":2}).c""" >> {
          binary(
            MapProject(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("c"),
            Data.NA)
        }

        """({}).c""" >> {
          binary(
            MapProject(_, _).embed,
            Data.Obj(),
            Data.Str("c"),
            Data.NA)
        }
      }

      "DeleteKey" >> {
        "{a:1, b:2} delete .a" >> {
          binary(
            DeleteKey(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("a"),
            Data.Obj("b" -> Data.Int(2)))
        }

        "{a:1, b:2} delete .b" >> {
          binary(
            DeleteKey(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("b"),
            Data.Obj("a" -> Data.Int(1)))
        }

        "{a:1, b:2} delete .c" >> {
          binary(
            DeleteKey(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("c"),
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)))
        }
      }

      "Meta" >> {
        // FIXME: Implement once we've switched to EJson in LogicalPlan.
        "returns metadata associated with a value" >> pending("Requires EJson.")
      }
    }

    "SetLib" >> {
      import SetLib._

      "Within" >> {
        "0 in [1, 2, 3]" >> {
          binary(Within(_, _).embed, Data.Int(0), Data.Arr(List(Data.Int(1), Data.Int(2), Data.Int(3))), Data.False)
        }

        "1 in [1, 2, 3]" >> {
          binary(Within(_, _).embed, Data.Int(1), Data.Arr(List(Data.Int(1), Data.Int(2), Data.Int(3))), Data.True)
        }

        "0 in []" >> {
          binary(Within(_, _).embed, Data.Int(0), Data.Arr(Nil), Data.False)
        }

        "[0] in [[1], 2, {a:3}, [0]]" >> {
          binary(
            Within(_, _).embed,
            Data.Arr(List(Data.Int(0))),
            Data.Arr(
              List(
                Data.Arr(List(Data.Int(1))),
                Data.Int(2),
                Data.Obj(ListMap("a" -> Data.Int(3))),
                Data.Arr(List(Data.Int(0))))),
            Data.True)
        }

        "[0, 1] in [[1], 2, {a:3}, [0, 1]]" >> {
          binary(
            Within(_, _).embed,
            Data.Arr(List(Data.Int(0), Data.Int(1))),
            Data.Arr(
              List(
                Data.Arr(List(Data.Int(1))),
                Data.Int(2),
                Data.Obj(ListMap("a" -> Data.Int(3))),
                Data.Arr(List(Data.Int(0), Data.Int(1))))),
            Data.True)
        }
      }

      "Range" >> {
        "Range(1, 3) is [1, 2, 3]" >> {
          binary(Range(_, _).embed, Data.Int(1), Data.Int(3), Data.Arr(List(Data.Int(1), Data.Int(2), Data.Int(3))))
        }
        "Range(1, 1) is [1]" >> {
          binary(Range(_, _).embed, Data.Int(1), Data.Int(1), Data.Arr(List(Data.Int(1))))
        }
        "Range(2, 1) is undefined" >> {
          binary(Range(_, _).embed, Data.Int(2), Data.Int(1), Data.NA)
        }
      }
    }
  }
}
