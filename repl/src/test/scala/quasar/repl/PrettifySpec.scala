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

package quasar.repl

import slamdata.Predef._

import quasar.common.data._, RepresentableDataGenerators._
import qdata.time.{DateTimeInterval, OffsetDate}

import java.time.{
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
  ZoneOffset
}

import eu.timepit.refined.auto._
import org.scalacheck.Arbitrary
import scalaz._, Scalaz._

final class PrettifySpec extends quasar.Qspec {
  import Prettify._

  "flatten" should {
    "find single field" in {
      val data = Data.Obj(ListMap("a" -> Data.Int(1)))
      flatten(data) must_== ListMap(
        Path(List(FieldSeg("a"))) -> Data.Int(1))
    }

    "find multiple fields" in {
      val data = Data.Obj(ListMap(
        "a" -> Data.Null, "b" -> Data.True, "c" -> Data.False, "d" -> Data.Dec(1.0), "e" -> Data.Str("foo")))
      flatten(data) must_== ListMap(
        Path(List(FieldSeg("a"))) -> Data.Null,
        Path(List(FieldSeg("b"))) -> Data.True,
        Path(List(FieldSeg("c"))) -> Data.False,
        Path(List(FieldSeg("d"))) -> Data.Dec(1.0),
        Path(List(FieldSeg("e"))) -> Data.Str("foo"))
    }

    "find nested fields" in {
      val data = Data.Obj(ListMap("value" -> Data.Obj(ListMap("a" -> Data.Int(1), "b" -> Data.Int(2)))))
      flatten(data) must_== ListMap(
        Path(List(FieldSeg("value"), FieldSeg("a"))) -> Data.Int(1),
        Path(List(FieldSeg("value"), FieldSeg("b"))) -> Data.Int(2))
    }

    "find array indices" in {
      val data = Data.Obj(ListMap("arr" -> Data.Arr(List(Data.Str("a"), Data.Str("b")))))
      flatten(data) must_== ListMap(
        Path(List(FieldSeg("arr"), IndexSeg(0))) -> Data.Str("a"),
        Path(List(FieldSeg("arr"), IndexSeg(1))) -> Data.Str("b"))
    }

    "find nested array indices" in {
      val data = Data.Obj(ListMap(
        "arr" -> Data.Arr(List(
          Data.Obj(ListMap("a" -> Data.Str("foo"))),
          Data.Obj(ListMap("b" -> Data.Str("bar")))))))
      flatten(data) must_== ListMap(
        Path(List(FieldSeg("arr"), IndexSeg(0), FieldSeg("a"))) -> Data.Str("foo"),
        Path(List(FieldSeg("arr"), IndexSeg(1), FieldSeg("b"))) -> Data.Str("bar"))
    }
  }

  "unflatten" should {
    "construct flat fields" in {
      val flat = ListMap(
        Path(List(FieldSeg("a"))) -> Data.Int(1),
        Path(List(FieldSeg("b"))) -> Data.Int(2))
      unflatten(flat) must_== Data.Obj(ListMap(
        "a" -> Data.Int(1),
        "b" -> Data.Int(2)))
    }

    "construct single nested field" in {
      val flat = ListMap(
        Path(List(FieldSeg("foo"), FieldSeg("bar"))) -> Data.Int(1))
      unflatten(flat) must_== Data.Obj(ListMap(
        "foo" -> Data.Obj(ListMap(
          "bar" -> Data.Int(1)))))
    }

    "construct array with missing elements" in {
      val flat = ListMap(
        Path(List(FieldSeg("a"), IndexSeg(0))) -> Data.Int(1),
        Path(List(FieldSeg("a"), IndexSeg(2))) -> Data.Int(3))
      unflatten(flat) must_==
        Data.Obj(ListMap(
          "a" -> Data.Arr(List(
            Data.Int(1), Data.Null, Data.Int(3)))))
    }

    "construct array with nested fields" in {
      val flat = ListMap(
        Path(List(FieldSeg("a"), IndexSeg(0), FieldSeg("foo"), FieldSeg("bar"))) -> Data.Int(1),
        Path(List(FieldSeg("a"), IndexSeg(2), FieldSeg("foo"), FieldSeg("bar"))) -> Data.Int(3),
        Path(List(FieldSeg("a"), IndexSeg(2), FieldSeg("foo"), FieldSeg("baz"))) -> Data.Int(4))
      unflatten(flat) must_==
        Data.Obj(ListMap(
          "a" -> Data.Arr(List(
            Data.Obj(ListMap(
              "foo" -> Data.Obj(ListMap(
                "bar" -> Data.Int(1))))),
            Data.Null,
            Data.Obj(ListMap(
              "foo" -> Data.Obj(ListMap(
                "bar" -> Data.Int(3),
                "baz" -> Data.Int(4)))))))))
    }

    "construct obj with populated contradictory paths" in {
      val flat = ListMap(
        Path(List(FieldSeg("foo"), IndexSeg(0))) -> Data.Int(1),
        Path(List(FieldSeg("foo"), FieldSeg("bar"))) -> Data.Int(2))
      unflatten(flat) must_==
        Data.Obj(ListMap(
          "foo" -> Data.Arr(List(
            Data.Int(1)))))
    }
  }

  "Path.label" should {
    "nested fields" in {
      Path(List(FieldSeg("foo"), FieldSeg("bar"))).label must_== "foo.bar"
    }

    "index at root" in {
      Path(List(IndexSeg(0))).label must_== "[0]"
    }

    "index in the middle" in {
      Path(List(FieldSeg("foo"), IndexSeg(0), FieldSeg("bar"))).label must_== "foo[0].bar"
    }

    "nested indices" in {
      Path(List(FieldSeg("matrix"), IndexSeg(0), IndexSeg(1))).label must_== "matrix[0][1]"
    }

    "escape special chars" in {
      Path(List(FieldSeg("foo1.2"), FieldSeg("1"), FieldSeg("[alt]"))).label must_== ???
    }.pendingUntilFixed("it's not clear what we need here, if anything")
  }

  "Path.parse" should {
    "nested fields" in {
      Path.parse("foo.bar") must_== \/-(Path(List(FieldSeg("foo"), FieldSeg("bar"))))
    }

    "index at root" in {
      Path.parse("[0]") must_== \/-(Path(List(IndexSeg(0))))
    }

    "index in the middle" in {
      Path.parse("foo[0].bar") must_== \/-(Path(List(FieldSeg("foo"), IndexSeg(0), FieldSeg("bar"))))
    }

    "field-style index" in {
      Path.parse("foo.0.bar") must_== \/-(Path(List(FieldSeg("foo"), IndexSeg(0), FieldSeg("bar"))))
    }

    "nested indices" in {
      Path.parse("matrix[0][1]") must_== \/-(Path(List(FieldSeg("matrix"), IndexSeg(0), IndexSeg(1))))
    }

    "fail with unmatched brackets" in {
      Path.parse("foo[0") must be_-\/
      Path.parse("foo]") must be_-\/
    }

    "fail with bad index" in {
      Path.parse("foo[]") must be_-\/
      Path.parse("foo[abc]") must be_-\/
      Path.parse("foo[-1]") must be_-\/
    }

    "fail with bad field" in {
      Path.parse("foo..bar") must be_-\/
    }
  }

  "render" should {
    "render Str without quotes" in {
      render(Data.Str("abc")) must_== Aligned.Left("abc")
    }

    "render round Dec with trailing zero" in {
      render(Data.Dec(1.0)) must_== Aligned.Right("1.0")
    }

    val localDate = JLocalDate.of(2016, 2, 29)
    val localTime = JLocalTime.of(6, 7, 8, 9)
    val offset = ZoneOffset.UTC

    "render OffsetDateTime" in {
      val dt = JOffsetDateTime.of(localDate, localTime, offset)
      render(Data.OffsetDateTime(dt)) must_== Aligned.Right("2016-02-29T06:07:08.000000009Z")
    }

    "render OffsetDate" in {
      val dt = OffsetDate(localDate, offset)
      render(Data.OffsetDate(dt)) must_== Aligned.Right("2016-02-29Z")
    }

    "render OffsetTime" in {
      val dt = JOffsetTime.of(localTime, offset)
      render(Data.OffsetTime(dt)) must_== Aligned.Right("06:07:08.000000009Z")
    }

    "render LocalDateTime" in {
      val dt = JLocalDateTime.of(localDate, localTime)
      render(Data.LocalDateTime(dt)) must_== Aligned.Right("2016-02-29T06:07:08.000000009")
    }

    "render LocalDate" in {
      render(Data.LocalDate(localDate)) must_== Aligned.Right("2016-02-29")
    }

    "render LocalTime" in {
      render(Data.LocalTime(localTime)) must_== Aligned.Right("06:07:08.000000009")
    }

    "render DateTimeInterval" in {
      val i = DateTimeInterval.make(2, 3, 4, 5, 678)
      render(Data.Interval(i)) must_== Aligned.Right("P2Y3M4DT5.000000678S")
    }
  }

  "parse" should {
    "parse \"\"" in {
      parse("") must beNone
    }

    "parse int" in {
      parse("1") must beSome(Data.Int(1))
    }

    import DataGenerators._

    // TODO: Add explanation for why these particular values are not representable here
    def representable(data: Data): Boolean = data match {
      case Data.Str("") => false
      case Data.Obj(_)  => false
      case Data.Arr(_)  => false
      case Data.NA      => false
      case _            => true
    }

    "round-trip all representable values" >> prop { (data: RepresentableData) =>
      representable(data.data) ==> {
        val r = render(data.data).value
        parse(r) must beSome(data.data)
      }
    }

    def isFlat(data: Data) = data match {
      case Data.Obj(_) => false
      case Data.Arr(_) => false
      case _ => true
    }

    "handle a decimal string with a leading zero" in {
      val data = Data.Str("00.12345")
      val r = render(data).value
      parse(r).map(render(_).value) must beSome("0.12345")
    }

    def trimNumericString(str: String): String =
      str.parseBigDecimal.toOption.map(_.toString).getOrElse(str)

    def removeLeadingZero(data: Data): Data = data match {
      case Data.Str(str) => Data.Str(trimNumericString(str))
      case data => data
    }

    "round-trip all flat rendered values that aren't \"\"" >> prop { (data0: Data) =>
      val data = removeLeadingZero(data0)
      val r = render(data).value
      // Unfortunately currently there is a bug where intervals do not serialize/deserialize properly
      // and although it would appear to work for a human observer,
      // the runtime instances are not found to be "equal" which is breaking tests
      (isFlat(data) && r ≠ "" && !Data._interval.nonEmpty(data)) ==> {
        parse(r).map(render(_).value) must beSome(r)
      }
      // Test will sometimes fail due to to many generator failures without this
    }.setArbitrary(Arbitrary(DataGenerators.simpleNonNested))
  }

  "renderTable" should {
    "format empty result" in {
      renderTable(Nil) must_== List()
    }

    "format one value" in {
      renderTable(List(
        Data.Int(0))) must_==
        List(
          " value |",
          "-------|",
          "     0 |")
    }

    "format one obj" in {
      renderTable(List(
        Data.Obj(ListMap(
          "a" -> Data.Int(0))))) must_==
        List(
          " a  |",
          "----|",
          "  0 |")
    }

    "format one row" in {
      renderTable(List(
        Data.Obj(ListMap(
          "a" -> Data.Int(0), "b" -> Data.Str("foo"))))) must_==
        List(
          " a  | b    |",
          "----|------|",
          "  0 | foo  |")
    }

    "format one array" in {
      renderTable(List(
        Data.Arr(List(
          Data.Int(0), Data.Str("foo"))))) must_==
        List(
          " [0] | [1]  |",
          "-----|------|",
          "   0 | foo  |")
    }

    "format empty values" in {
      renderTable(List(
        Data.Obj(),
        Data.Obj())) must_==
        List(
          " <empty> |",
          "---------|",
          "         |",
          "         |")
    }
  }

  "renderStream" should {
    import scalaz.concurrent.Task
    import scalaz.stream.Process

    "empty stream" in {
      val values: Process[Task, Data] = Process.halt
      val rows = renderStream(values, 100)
      rows.runLog.unsafePerformSync must_== Vector(
        List("<empty>"))
    }

    "empty values" in {
      val values: Process[Task, Data] = Process.emitAll(List(
        Data.Obj(),
        Data.Obj()))
      val rows = renderStream(values, 100)
      rows.runLog.unsafePerformSync must_== Vector(
        List("<empty>"),
        List(""),
        List(""))
    }

    "one trivial value" in {
      val values: Process[Task, Data] = Process.emitAll(List(
        Data.Obj(ListMap("a" -> Data.Int(1)))))
      val rows = renderStream(values, 100)
      rows.runLog.unsafePerformSync must_== Vector(
        List("a"),
        List("1"))
    }

    "more than n" in {
      val values: Process[Task, Data] = Process.emitAll(List(
        Data.Obj(ListMap(
          "a" -> Data.Int(1))),
        Data.Obj(ListMap(
          "b" -> Data.Int(2))),
        Data.Obj(ListMap(
          "a" -> Data.Int(3),
          "b" -> Data.Int(4)))))
      val rows = renderStream(values, 2)
      rows.runLog.unsafePerformSync must_== Vector(
        List("a", "b"),
        List("1", ""),
        List("", "2"),
        List("3", "4"))
    }

    "more than n with new fields after" in {
      val values: Process[Task, Data] = Process.emitAll(List(
        Data.Obj(ListMap(
          "a" -> Data.Int(1))),
        Data.Obj(ListMap(
          "b" -> Data.Int(2))),
        Data.Obj(ListMap(
          "a" -> Data.Int(3),
          "b" -> Data.Int(4),
          "c" -> Data.Int(5)))))
      val rows = renderStream(values, 2)
      rows.runLog.unsafePerformSync must_== Vector(
        List("a", "b"),
        List("1", ""),
        List("", "2"),
        List("3", "4"))
    }

    "properly sequence effects" in {
      // A source of values that keeps track of evaluation:
      val history = new scala.collection.mutable.ListBuffer[Int]
      def t(n: Int) = Task.delay { history += n; Data.Obj(ListMap("n" -> Data.Int(n))) }

      val values = Process.eval(t(0)) ++ Process.eval(t(1))

      val rows = renderStream(values, 1)

      // Run the process once:
      val rez = rows.runLog.unsafePerformSync

      rez must_== Vector(
        List("n"),
        List("0"),
        List("1"))
      history must_== List(0, 1)
    }
  }
}
