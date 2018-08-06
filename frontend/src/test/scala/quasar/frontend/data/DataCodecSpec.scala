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

package quasar.frontend.data

import slamdata.Predef._
import DataEncodingError.{UnrepresentableDataError, UnescapedKeyError}
import quasar.common.data._
import qdata.time.{DateTimeInterval, OffsetDate}

import java.time._

import argonaut._, Argonaut._
import scalaz._, Scalaz._

class DataCodecSpecs extends quasar.Qspec {
  import DataGenerators._, RepresentableDataGenerators._
  import DataCodec.PreciseKeys._

  implicit val DataShow = new Show[Data] { override def show(v: Data) = v.toString }
  implicit val ShowStr = new Show[String] { override def show(v: String) = v }

  def roundTrip(d: Data)(implicit C: DataCodec)
      : Option[DataEncodingError \/ Data] =
    DataCodec.render(d).map(DataCodec.parse(_))

  "Precise" should {
    implicit val codec = DataCodec.Precise

    "render" should {
      // NB: these tests verify that all the formatting matches our documentation

      "encode null" in { DataCodec.render(Data.Null) must beSome("null") }
      "encode true" in { DataCodec.render(Data.True) must beSome("true") }
      "encode false" in { DataCodec.render(Data.False) must beSome("false") }
      "encode int" in { DataCodec.render(Data.Int(0)) must beSome("0") }
      "encode dec" in { DataCodec.render(Data.Dec(1.1)) must beSome("1.1") }
      "encode dec with no fractional part" in { DataCodec.render(Data.Dec(2.0)) must beSome("2.0") }

      "encode localdatetime" in {
        DataCodec.render(Data.LocalDateTime(LocalDateTime.parse("2015-01-31T10:30:00"))) must
          beSome(s"""{ "$LocalDateTimeKey": "2015-01-31T10:30:00.000000000" }""")
      }

      "encode localdate" in {
        DataCodec.render(Data.LocalDate(LocalDate.parse("2015-01-31"))) must
          beSome(s"""{ "$LocalDateKey": "2015-01-31" }""")
      }

      "encode localtime" in {
        DataCodec.render(Data.LocalTime(LocalTime.parse("10:30:00"))) must
          beSome(s"""{ "$LocalTimeKey": "10:30:00.000000000" }""")
      }

      "encode offsetdatetime" in {
        DataCodec.render(Data.OffsetDateTime(OffsetDateTime.parse("2015-01-31T10:30:00Z"))) must
          beSome(s"""{ "$OffsetDateTimeKey": "2015-01-31T10:30:00.000000000Z" }""")
      }

      "encode offsetdate" in {
        DataCodec.render(Data.OffsetDate(OffsetDate.parse("2015-01-31Z"))) must
          beSome(s"""{ "$OffsetDateKey": "2015-01-31Z" }""")
      }

      "encode offsettime" in {
        DataCodec.render(Data.OffsetTime(OffsetTime.parse("10:30:00.000Z"))) must
          beSome(s"""{ "$OffsetTimeKey": "10:30:00.000000000Z" }""")
      }

      "encode interval"  in {
        (for {
          interval <- DateTimeInterval.parse("PT12H34M")
          rendered <- DataCodec.render(Data.Interval(interval))
        } yield rendered) must beSome(s"""{ "$IntervalKey": "PT12H34M" }""")
      }

      "encode obj" in {
        // NB: more than 4, to verify order is preserved
        DataCodec.render(Data.Obj(ListMap(
          "a" -> Data.Int(1),
          "b" -> Data.Int(2),
          "c" -> Data.Int(3),
          "d" -> Data.Int(4),
          "e" -> Data.Int(5)))) must beSome(
          """{ "a": 1, "b": 2, "c": 3, "d": 4, "e": 5 }""")
      }

      "encode obj with leading '$'s" in {
        DataCodec.render(Data.Obj(ListMap(
          "$a" -> Data.Int(1),
          LocalDateKey -> Data.LocalDateTime(LocalDateTime.parse("2015-01-31T10:30"))))) must beSome(

            s"""{ "$ObjKey": { "$$a": 1, "$LocalDateKey": { "$LocalDateTimeKey": "2015-01-31T10:30:00.000000000" } } }""")
      }

      "encode obj with $obj" in {
        DataCodec.render(Data.Obj(ListMap(ObjKey -> Data.Obj(ListMap(ObjKey -> Data.Int(1)))))) must
          beSome(s"""{ "$ObjKey": { "$ObjKey": { "$ObjKey": { "$ObjKey": 1 } } } }""")
      }

      "encode array" in {
        DataCodec.render(Data.Arr(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must
          beSome("[ 0, 1, 2 ]")
      }

      "encode NA" in {
        DataCodec.render(Data.NA) must beNone
      }
    }

    // NB. We don't use `RepresentableData` because it does not generate ID which
    // we want to test here
    "round-trip" >> prop { data: Data =>
      DataCodec.representable(data, codec) ==> {
        roundTrip(data) must beSome(data.right[DataEncodingError])
      }
    }

    "parse" should {
      // These types get lost on the way through rendering and re-parsing:
      "re-parse very large Int value as Dec" in {
        roundTrip(Data.Int(LargeInt)) must
          beSome(Data.Dec(new java.math.BigDecimal(LargeInt.underlying)).right[DataEncodingError])
      }

      "decode localdatetime" in {
        DataCodec.parse(s"""{ "$LocalDateTimeKey": "2015-01-31T10:30" }""").toOption must
          beSome(Data.LocalDateTime(LocalDateTime.parse("2015-01-31T10:30:00")))
      }

      "decode localdate" in {
        DataCodec.parse(s"""{ "$LocalDateKey": "2015-01-31" }""").toOption must
          beSome(Data.LocalDate(LocalDate.parse("2015-01-31")))
      }

      "decode localtime" in {
        DataCodec.parse(s"""{ "$LocalTimeKey": "10:30" }""").toOption must
          beSome(Data.LocalTime(LocalTime.parse("10:30:00.000")))
      }

      "decode offsetdatetime" in {
        DataCodec.parse(s"""{ "$OffsetDateTimeKey": "2015-01-31T10:30Z" }""").toOption must
          beSome(Data.OffsetDateTime(OffsetDateTime.parse("2015-01-31T10:30:00Z")))
      }

      "decode offsetdate" in {
        DataCodec.parse(s"""{ "$OffsetDateKey": "2015-01-31Z" }""").toOption must
          beSome(Data.OffsetDate(OffsetDate.parse("2015-01-31Z")))
      }

      "decode offsettime" in {
        DataCodec.parse(s"""{ "$OffsetTimeKey": "10:30Z" }""").toOption must
          beSome(Data.OffsetTime(OffsetTime.parse("10:30:00.000Z")))
      }

      "decode offsetdatetime within array" in {
        val input = s"""[{ "$OffsetDateTimeKey": "2015-01-31T10:30:00.000Z" }]"""

        val results = DataCodec.parse(input).toOption
        val expected = Data.Arr(List(Data.OffsetDateTime(OffsetDateTime.parse("2015-01-31T10:30:00Z"))))

        results must beSome(expected)
      }

      "decode offsetdatetime AND localdatetime within array" in {
        val input = s"""[
          { "$OffsetDateTimeKey": "2015-01-31T10:30:00.000Z" },
          { "$LocalDateTimeKey": "2015-01-31T10:30" }]"""

        val results = DataCodec.parse(input).toOption

        val expected = Data.Arr(
          List(
            Data.OffsetDateTime(OffsetDateTime.parse("2015-01-31T10:30:00Z")),
            Data.LocalDateTime(LocalDateTime.parse("2015-01-31T10:30:00"))))

        results must beSome(expected)
      }


      // Some invalid inputs:

      "fail with unescaped leading '$'" in {
        DataCodec.parse("""{ "$a": 1 }""") must
          be_-\/(UnescapedKeyError(jSingleObject("$a", jNumber(1))))
      }

      "fail with invalid offset date time value" in {
        DataCodec.parse(s"""{ "$OffsetDateTimeKey": 123456 }""") must be_-\/
      }

      "fail with invalid offset date time string" in {
        DataCodec.parse(s"""{ "$OffsetDateTimeKey": "10 o'clock this morning" }""") must be_-\/
      }
    }
  }

  "Readable" should {
    implicit val codec = DataCodec.Readable

    "render" should {
      // NB: these tests verify that all the formatting matches our documentation

      "encode null" in { DataCodec.render(Data.Null) must beSome("null") }
      "encode true" in { DataCodec.render(Data.True) must beSome("true") }
      "encode false" in { DataCodec.render(Data.False) must beSome("false") }
      "encode int" in { DataCodec.render(Data.Int(0)) must beSome("0") }
      "encode dec" in { DataCodec.render(Data.Dec(1.1)) must beSome("1.1") }
      "encode dec with no fractional part" in { DataCodec.render(Data.Dec(2.0)) must beSome("2.0") }

      "encode localdatetime" in {
        DataCodec.render(Data.LocalDateTime(LocalDateTime.parse("2015-01-31T10:30:00"))) must
          beSome("\"2015-01-31T10:30\"")
      }

      "encode localdate" in {
        DataCodec.render(Data.LocalDate(LocalDate.parse("2015-01-31"))) must
          beSome("\"2015-01-31\"")
      }

      "encode localtime" in {
        DataCodec.render(Data.LocalTime(LocalTime.parse("10:30:00.000"))) must
          beSome("\"10:30\"")
      }

      "encode offsetdatetime" in {
        DataCodec.render(Data.OffsetDateTime(OffsetDateTime.parse("2015-01-31T10:30:00Z"))) must
          beSome("\"2015-01-31T10:30Z\"")
      }

      "encode offsetdate" in {
        DataCodec.render(Data.OffsetDate(OffsetDate.parse("2015-01-31Z"))) must
          beSome("\"2015-01-31Z\"")
      }

      "encode offsettime" in {
        DataCodec.render(Data.OffsetTime(OffsetTime.parse("10:30:00.000Z"))) must
          beSome("\"10:30Z\"")
      }

      "encode interval" in {
        (for {
          interval <- DateTimeInterval.parse("PT12H34M")
          rendered <- DataCodec.render(Data.Interval(interval))
        } yield rendered) must beSome("\"PT12H34M\"")
      }

      "encode obj" in {
        // NB: more than 4, to verify order is preserved
        DataCodec.render(Data.Obj(ListMap(
          "a" -> Data.Int(1),
          "b" -> Data.Int(2),
          "c" -> Data.Int(3),
          "d" -> Data.Int(4),
          "e" -> Data.Int(5)))) must
          beSome("""{ "a": 1, "b": 2, "c": 3, "d": 4, "e": 5 }""")
      }

      "encode obj with leading '$'s" in {
        DataCodec.render(Data.Obj(ListMap(
          "$a" -> Data.Int(1),
          LocalDateTimeKey -> Data.LocalDateTime(LocalDateTime.parse("2015-01-31T10:30"))))) must
          beSome(s"""{ "$$a": 1, "$LocalDateTimeKey": "2015-01-31T10:30" }""")
      }

      "encode array" in {
        DataCodec.render(Data.Arr(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beSome("[ 0, 1, 2 ]")
      }

      "encode NA" in {
        DataCodec.render(Data.NA) must beNone
      }
    }

    "round-trip" >> prop { data: RepresentableData =>
      roundTrip(data.data) must beSome(data.data.right[DataEncodingError])
    }

    "parse" should {
      // These types get inferred whenever a string matches the expected format:

      "re-parse Str as Timestamp" in {
        val ts = Data.LocalDateTime(LocalDateTime.now)
        val str = Data.Str(ts.value.toString)
        roundTrip(str) must beSome(ts.right[DataEncodingError])
      }

      "re-parse Str as Date" in {
        val date = Data.LocalDate(LocalDate.now)
        val str = Data.Str(date.value.toString)
        roundTrip(str) must beSome(date.right[DataEncodingError])
      }

      "re-parse Str as Time" in {
        val time = Data.LocalTime(LocalTime.now)
        val str = Data.Str(time.value.toString)
        roundTrip(str) must beSome(time.right[DataEncodingError])
      }

      "re-parse Str as Interval" in {
        val interval = Data.Interval(DateTimeInterval.make(0, 0, 0, 1, 0))
        val str = Data.Str(interval.value.toString)
        roundTrip(str) must beSome(interval.right[DataEncodingError])
      }


      // These types get lost on the way through rendering and re-parsing:

      "re-parse very large Int value as Dec" in {
        roundTrip(Data.Int(LargeInt)) must beSome(Data.Dec(new java.math.BigDecimal(LargeInt.underlying)).right[DataEncodingError])
      }
    }
  }

  "Error messages" should {
    "UnrepresentableDataError" >> prop { any: Data =>
      UnrepresentableDataError(any).message must_= ("not representable: " + any)
    }
    "UnescapedKeyError" in {
      val sample:Json = jString("foo")
      UnescapedKeyError(sample).message must_= s"un-escaped key: $sample"
    }
  }
}
