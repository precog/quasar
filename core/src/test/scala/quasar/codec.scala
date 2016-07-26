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

package quasar

import argonaut._, Argonaut._
import quasar.DataEncodingError.{UnrepresentableDataError, UnescapedKeyError}
import quasar.Predef._

import org.specs2.scalaz._
import org.specs2.ScalaCheck

import org.threeten.bp._
import scalaz._, Scalaz._

class DataCodecSpecs extends quasar.QuasarSpecification with ScalaCheck with DisjunctionMatchers {
  import DataArbitrary._

  implicit val DataShow = new Show[Data] { override def show(v: Data) = v.toString }
  implicit val ShowStr = new Show[String] { override def show(v: String) = v }

  "Precise" should {
    implicit val codec = DataCodec.Precise

    // Identify the sub-set of values can be represented in Precise JSON in
    // such a way that the parser recovers the original type, which is
    // effectively everything except integer values that don't fit into
    // Long, which Argonaut does not allow us to distinguish from decimals.
    def representable(data: Data) = data match {
      case Data.Int(x)    => x.isValidLong
      case Data.Set(_)    => false
      case _              => true
    }

    "render" should {
      // NB: these tests verify that all the formatting matches our documentation

      "encode null"      in { DataCodec.render(Data.Null)     must beRightDisjunction("null") }
      "encode true"      in { DataCodec.render(Data.True)     must beRightDisjunction("true") }
      "encode false"     in { DataCodec.render(Data.False)    must beRightDisjunction("false") }
      "encode int"       in { DataCodec.render(Data.Int(0))   must beRightDisjunction("0") }
      "encode dec"       in { DataCodec.render(Data.Dec(1.1)) must beRightDisjunction("1.1") }
      "encode dec with no fractional part" in { DataCodec.render(Data.Dec(2.0)) must beRightDisjunction("2.0") }
      "encode timestamp" in { DataCodec.render(Data.Timestamp(Instant.parse("2015-01-31T10:30:00Z"))) must beRightDisjunction("""{ "$timestamp": "2015-01-31T10:30:00Z" }""") }
      "encode date"      in { DataCodec.render(Data.Date(LocalDate.parse("2015-01-31")))              must beRightDisjunction("""{ "$date": "2015-01-31" }""") }
      "encode time"      in { DataCodec.render(Data.Time(LocalTime.parse("10:30:00.000")))            must beRightDisjunction("""{ "$time": "10:30" }""") }
      "encode interval"  in { DataCodec.render(Data.Interval(Duration.parse("PT12H34M")))             must beRightDisjunction("""{ "$interval": "PT12H34M" }""") }
      "encode obj" in {
        // NB: more than 4, to verify order is preserved
        DataCodec.render(Data.Obj(ListMap("a" -> Data.Int(1), "b" -> Data.Int(2), "c" -> Data.Int(3), "d" -> Data.Int(4), "e" -> Data.Int(5)))) must
          beRightDisjunction("""{ "a": 1, "b": 2, "c": 3, "d": 4, "e": 5 }""")
      }
      "encode obj with leading '$'s" in {
        DataCodec.render(Data.Obj(ListMap("$a" -> Data.Int(1), "$date" -> Data.Timestamp(Instant.parse("2015-01-31T10:30:00Z"))))) must
          beRightDisjunction("""{ "$obj": { "$a": 1, "$date": { "$timestamp": "2015-01-31T10:30:00Z" } } }""")
      }
      "encode obj with $obj" in {
        DataCodec.render(Data.Obj(ListMap("$obj" -> Data.Obj(ListMap("$obj" -> Data.Int(1)))))) must
          beRightDisjunction("""{ "$obj": { "$obj": { "$obj": { "$obj": 1 } } } }""")
      }
      "encode array"     in { DataCodec.render(Data.Arr(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beRightDisjunction("[ 0, 1, 2 ]") }
      "encode set"       in { DataCodec.render(Data.Set(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beLeftDisjunction }
      "encode binary"    in { DataCodec.render(Data.Binary(Array[Byte](76, 77, 78, 79))) must beRightDisjunction("""{ "$binary": "TE1OTw==" }""") }
      "encode objectId"  in { DataCodec.render(Data.Id("abc")) must beRightDisjunction("""{ "$oid": "abc" }""") }
      "encode NA"        in { DataCodec.render(Data.NA) must beRightDisjunction("""{ "$na": null }""") }
    }

    "round-trip" ! prop { (data: Data) =>
      representable(data) ==> {
        DataCodec.render(data).flatMap(DataCodec.parse) must beRightDisjunction(data)
      }
    }

    "parse" should {
      // These types get lost on the way through rendering and re-parsing:

      "re-parse very large Int value as Dec" in {
        DataCodec.render(LargeInt).flatMap(DataCodec.parse) must beRightDisjunction(Data.Dec(new java.math.BigDecimal(LargeInt.value.underlying)))
      }


      // Some invalid inputs:

      "fail with unescaped leading '$'" in {
        DataCodec.parse("""{ "$a": 1 }""") must beLeftDisjunction(UnescapedKeyError(jSingleObject("$a", jNumber(1))))
      }

      "fail with bad timestamp value" in {
        DataCodec.parse("""{ "$timestamp": 123456 }""") must beLeftDisjunction
      }

      "fail with bad timestamp string" in {
        DataCodec.parse("""{ "$timestamp": "10 o'clock this morning" }""") must beLeftDisjunction
      }
    }
  }

  "Readable" should {
    implicit val codec = DataCodec.Readable

    // Identify the sub-set of values can be represented in Readable JSON in
    // such a way that the parser recovers the original type.
    // NB: this does not account for Str values that will be confused with
    // other types (e.g. `Data.Str("12:34")`, which becomes `Data.Time`).
    def representable(data: Data): Boolean = data match {
      case Data.Int(x)     => x.isValidLong
      case Data.Set(_)     => false
      case Data.Binary(_)  => false
      case Data.Id(_)      => false
      case Data.NA         => false
      case Data.Arr(value) => value.forall(representable)
      case Data.Obj(value) => value.values.forall(representable)
      case _               => true
    }

    "render" should {
      // NB: these tests verify that all the formatting matches our documentation

      "encode null"      in { DataCodec.render(Data.Null)     must beRightDisjunction("null") }
      "encode true"      in { DataCodec.render(Data.True)     must beRightDisjunction("true") }
      "encode false"     in { DataCodec.render(Data.False)    must beRightDisjunction("false") }
      "encode int"       in { DataCodec.render(Data.Int(0))   must beRightDisjunction("0") }
      "encode dec"       in { DataCodec.render(Data.Dec(1.1)) must beRightDisjunction("1.1") }
      "encode dec with no fractional part" in { DataCodec.render(Data.Dec(2.0)) must beRightDisjunction("2.0") }
      "encode timestamp" in { DataCodec.render(Data.Timestamp(Instant.parse("2015-01-31T10:30:00Z"))) must beRightDisjunction("\"2015-01-31T10:30:00Z\"") }
      "encode date"      in { DataCodec.render(Data.Date(LocalDate.parse("2015-01-31")))              must beRightDisjunction("\"2015-01-31\"") }
      "encode time"      in { DataCodec.render(Data.Time(LocalTime.parse("10:30:00.000")))            must beRightDisjunction("\"10:30\"") }
      "encode interval"  in { DataCodec.render(Data.Interval(Duration.parse("PT12H34M")))             must beRightDisjunction("\"PT12H34M\"") }
      "encode obj" in {
        // NB: more than 4, to verify order is preserved
        DataCodec.render(Data.Obj(ListMap("a" -> Data.Int(1), "b" -> Data.Int(2), "c" -> Data.Int(3), "d" -> Data.Int(4), "e" -> Data.Int(5)))) must
          beRightDisjunction("""{ "a": 1, "b": 2, "c": 3, "d": 4, "e": 5 }""")
      }
      "encode obj with leading '$'s" in {
        DataCodec.render(Data.Obj(ListMap("$a" -> Data.Int(1), "$date" -> Data.Timestamp(Instant.parse("2015-01-31T10:30:00Z"))))) must
          beRightDisjunction("""{ "$a": 1, "$date": "2015-01-31T10:30:00Z" }""")
        }
      "encode array"     in { DataCodec.render(Data.Arr(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beRightDisjunction("[ 0, 1, 2 ]") }
      "encode set"       in { DataCodec.render(Data.Set(List(Data.Int(0), Data.Int(1), Data.Int(2)))) must beLeftDisjunction }
      "encode binary"    in { DataCodec.render(Data.Binary(Array[Byte](76, 77, 78, 79))) must beRightDisjunction("\"TE1OTw==\"") }
      "encode empty binary" in { DataCodec.render(Data.Binary(Array[Byte]())) must beRightDisjunction("\"\"") }
      "encode objectId"  in { DataCodec.render(Data.Id("abc")) must beRightDisjunction("\"abc\"") }
      "encode NA"        in { DataCodec.render(Data.NA) must beRightDisjunction("null") }
    }

    "round-trip" ! prop { (data: Data) =>
      representable(data) ==> {
        DataCodec.render(data).flatMap(DataCodec.parse) must beRightDisjunction(data)
      }
    }

    "parse" should {
      // These types get inferred whenever a string matches the expected format:

      "re-parse Str as Timestamp" in {
        val ts = Data.Timestamp(Instant.now)
        val str = Data.Str(ts.value.toString)
        DataCodec.render(str).flatMap(DataCodec.parse) must beRightDisjunction(ts)
      }

      "re-parse Str as Date" in {
        val date = Data.Date(LocalDate.now)
        val str = Data.Str(date.value.toString)
        DataCodec.render(str).flatMap(DataCodec.parse) must beRightDisjunction(date)
      }

      "re-parse Str as Time" in {
        val time = Data.Time(LocalTime.now)
        val str = Data.Str(time.value.toString)
        DataCodec.render(str).flatMap(DataCodec.parse) must beRightDisjunction(time)
      }

      "re-parse Str as Interval" in {
        val interval = Data.Interval(Duration.ofSeconds(1))
        val str = Data.Str(interval.value.toString)
        DataCodec.render(str).flatMap(DataCodec.parse) must beRightDisjunction(interval)
      }


      // These types get lost on the way through rendering and re-parsing:

      "re-parse very large Int value as Dec" in {
        DataCodec.render(LargeInt).flatMap(DataCodec.parse) must beRightDisjunction(Data.Dec(new java.math.BigDecimal(LargeInt.value.underlying)))
      }

      "re-parse Binary as Str" in {
        val binary = Data.Binary(Array[Byte](0, 1, 2, 3))
        DataCodec.render(binary).flatMap(DataCodec.parse) must beRightDisjunction(Data.Str("AAECAw=="))
      }

      "re-parse Id as Str" in {
        val id = Data.Id("abc")
        DataCodec.render(id).flatMap(DataCodec.parse) must beRightDisjunction(Data.Str("abc"))
      }
    }
  }

  "Error messages" should {
    "UnrepresentableDataError" ! prop { any: Data =>
      UnrepresentableDataError(any).message must_= ("not representable: " + any)
    }
    "UnescapedKeyError" in {
      val sample:Json = jString("foo")
      UnescapedKeyError(sample).message must_= s"un-escaped key: $sample"
    }
  }
}
