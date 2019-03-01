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

import quasar.api.table.ColumnType
import quasar.common.{CPath, CPathField}

import org.specs2.execute.PendingUntilFixed._
import org.specs2.matcher.Matcher
import org.specs2.matcher.MatchersImplicits._
import org.specs2.specification.core.SpecStructure

import scala.collection.immutable.{Map, Set}

import java.lang.String

abstract class ScalarStageSpec
    extends ScalarStageSpec.IdsSpec
    with ScalarStageSpec.WrapSpec
    with ScalarStageSpec.ProjectSpec
    with ScalarStageSpec.MaskSpec
    with ScalarStageSpec.PivotSpec
    with ScalarStageSpec.CartesianSpec

/*
 * Test names must begin with the prefix specified in their
 * `override def is` implementation followed by `-#*` in
 * order for `pendingFragments` to come into effect.
 */
object ScalarStageSpec {

  def parseNumber(prefix: String, name: String): Option[Int] = {
    val regexPrefix = s"${prefix}-[1-9][0-9]*".r
    val regexIdx = s"[1-9][0-9]*".r

    regexPrefix.findPrefixOf(name)
      .flatMap(regexIdx.findFirstIn)
      .map(_.toInt)
  }

  def pendingFragments(sis: SpecStructure, pendingExamples: Set[Int], prefix: String)
      : SpecStructure =
    sis.copy(lazyFragments = () => sis.fragments.map { f =>
      parseNumber(prefix, f.description.show) match {
        case Some(i) =>
          if (pendingExamples.contains(i))
            f.updateExecution(_.mapResult(_.pendingUntilFixed))
          else
            f
        case None => f
      }
    })

  /*
   * Please note that this is currently *over*-specified.
   * We don't technically need monotonic ids or even numerical
   * ones, we just need *unique* identities. That assertion is
   * quite hard to encode though. If we find we need such an
   * implementation in the future, these assertions should be
   * changed.
   */
  trait IdsSpec extends JsonSpec {
    import IdStatus.{ExcludeId, IdOnly, IncludeId}

    val idsPendingExamples: Set[Int]

    "ExcludeId" should {
      "ids-1 emit scalar rows unmodified" in {
        val input = ldjson("""
          1
          "hi"
          true
          """)

        input must interpretIdsAs(ExcludeId, input, 3)
      }

      "ids-2 emit vector rows unmodified" in {
        val input = ldjson("""
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          [{ "d": {} }]
          """)

        input must interpretIdsAs(ExcludeId, input, 3)
      }
    }

    "IdOnly" should {
      "ids-3 return monotonic integers for each scalar row" in {
        val input = ldjson("""
          1
          "hi"
          true
          """)

        val expected = ldjson("""
          0
          1
          2
          """)

        input must interpretIdsAs(IdOnly, expected, 3)
      }

      "ids-4 return monotonic integers for each vector row" in {
        val input = ldjson("""
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          [{ "d": {} }]
          """)

        val expected = ldjson("""
          0
          1
          2
          """)

        input must interpretIdsAs(IdOnly, expected, 3)
      }
    }

    "IncludeId" should {
      "ids-5 wrap each scalar row in monotonic integers" in {
        val input = ldjson("""
          1
          "hi"
          true
          """)

        val expected = ldjson("""
          [0, 1]
          [1, "hi"]
          [2, true]
          """)

        input must interpretIdsAs(IncludeId, expected, 3)
      }

      "ids-6 wrap each vector row in monotonic integers" in {
        val input = ldjson("""
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          [{ "d": {} }]
          """)

        val expected = ldjson("""
          [0, [1, 2, 3]]
          [1, { "a": "hi", "b": { "c": null } }]
          [2, [{ "d": {} }]]
          """)

        input must interpretIdsAs(IncludeId, expected, 3)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, idsPendingExamples, "ids")

    def evalIds(idStatus: IdStatus, stream: JsonStream): JsonStream

    def interpretIdsAs(idStatus: IdStatus, expected: JsonStream, expectedLength: Int)
      : Matcher[JsonStream] = { stream: JsonStream =>

      val result: JsonStream = evalIds(idStatus, stream)

      (result.length mustEqual expectedLength) and
        (result must bestSemanticEqual(expected))
    }
  }

  trait WrapSpec extends JsonSpec {
    protected final type Wrap = ScalarStage.Wrap
    protected final val Wrap = ScalarStage.Wrap

    val wrapPendingExamples: Set[Int]

    "wrap" should {
      "wrap-1 nest scalars" in {
        val input = ldjson("""
          1
          "hi"
          true
          """)

        val expected = ldjson("""
          { "foo": 1 }
          { "foo": "hi" }
          { "foo": true }
          """)

        input must wrapInto("foo")(expected, 3)
      }

      "wrap-2 nest vectors" in {
        val input = ldjson("""
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          [{ "d": {} }]
          """)

        val expected = ldjson("""
          { "bar": [1, 2, 3] }
          { "bar": { "a": "hi", "b": { "c": null } } }
          { "bar": [{ "d": {} }] }
          """)

        input must wrapInto("bar")(expected, 3)
      }

      "wrap-3 nest empty objects" in {
        val input = ldjson("""
          "a"
          {}
          []
          1
          """)

        val expected = ldjson("""
          { "bar": "a" }
          { "bar": {} }
          { "bar": [] }
          { "bar": 1 }
          """)

        input must wrapInto("bar")(expected, 4)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, wrapPendingExamples, "wrap")

    def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream

    def wrapInto(name: String)(expected: JsonStream, expectedLength: Int)
      : Matcher[JsonStream] = { stream: JsonStream =>

      val result: JsonStream = evalWrap(Wrap(name), stream)

      (result.length mustEqual expectedLength) and
        (result must bestSemanticEqual(expected))
    }
  }

  trait ProjectSpec extends JsonSpec {
    protected final type Project = ScalarStage.Project
    protected final val Project = ScalarStage.Project

    val projectPendingExamples: Set[Int]

    "project" should {
      "prj-1 passthrough at identity" in {
        val input = ldjson("""
          1
          "two"
          false
          [1, 2, 3]
          { "a": 1, "b": "two" }
          []
          {}
          """)

        input must projectInto(".")(input, 7)
      }

      "prj-2 extract .a" in {
        val input = ldjson("""
          { "a": 1, "b": "two" }
          { "a": "foo", "b": "two" }
          { "a": true, "b": "two" }
          { "a": [], "b": "two" }
          { "a": {}, "b": "two" }
          { "a": [1, 2], "b": "two" }
          { "a": { "c": 3 }, "b": "two" }
          """)
        val expected = ldjson("""
          1
          "foo"
          true
          []
          {}
          [1, 2]
          { "c": 3 }
          """)

        input must projectInto(".a")(expected, 7)
      }

      "prj-3 extract .a.b" in {
        val input = ldjson("""
          { "a": { "b": 1 }, "b": "two" }
          { "a": { "b": "foo" }, "b": "two" }
          { "a": { "b": true }, "b": "two" }
          { "a": { "b": [] }, "b": "two" }
          { "a": { "b": {} }, "b": "two" }
          { "a": { "b": [1, 2] }, "b": "two" }
          { "a": { "b": { "c": 3 } }, "b": "two" }
          """)
        val expected = ldjson("""
          1
          "foo"
          true
          []
          {}
          [1, 2]
          { "c": 3 }
          """)

        input must projectInto(".a.b")(expected, 7)
      }

      "prj-4 extract .a[1]" in {
        val input = ldjson("""
          { "a": [3, 1], "b": "two" }
          { "a": [3, "foo"], "b": "two" }
          { "a": [3, true], "b": "two" }
          { "a": [3, []], "b": "two" }
          { "a": [3, {}], "b": "two" }
          { "a": [3, [1, 2]], "b": "two" }
          { "a": [3, { "c": 3 }], "b": "two" }
          """)
        val expected = ldjson("""
          1
          "foo"
          true
          []
          {}
          [1, 2]
          { "c": 3 }
          """)

        input must projectInto(".a[1]")(expected, 7)
      }

      "prj-5 extract [1]" in {
        val input = ldjson("""
          [0, 1]
          [0, "foo"]
          [0, true]
          [0, []]
          [0, {}]
          [0, [1, 2]]
          [0, { "c": 3 }]
          """)

        val expected = ldjson("""
          1
          "foo"
          true
          []
          {}
          [1, 2]
          { "c": 3 }
          """)

        input must projectInto("[1]")(expected, 7)
      }

      "prj-6 extract [1][0]" in {
        val input = ldjson("""
          [0, [1]]
          [0, ["foo"]]
          [0, [true]]
          [0, [[]]]
          [0, [{}]]
          [0, [[1, 2]]]
          [0, [{ "c": 3 }]]
          """)

        val expected = ldjson("""
          1
          "foo"
          true
          []
          {}
          [1, 2]
          { "c": 3 }
          """)

        input must projectInto("[1][0]")(expected, 7)
      }

      "prj-7 extract [1].a" in {
        val input = ldjson("""
          [0, { "a": 1 }]
          [false, { "a": "foo" }]
          [1, { "a": true }]
          [[], { "a": [] }]
          ["foo", { "a": {} }]
          [{}, { "a": [1, 2] }]
          [0, { "a": { "c": 3 } }]
          """)

        val expected = ldjson("""
          1
          "foo"
          true
          []
          {}
          [1, 2]
          { "c": 3 }
          """)

        input must projectInto("[1].a")(expected, 7)
      }

      "prj-8 produce undefined for rows not containing object path" in {
        val input = ldjson("""
          { "x": 1 }
          { "x": 2, "y": 3 }
          { "y": 4, "z": 5 }
          ["a", "b"]
          4
          "seven"
          { "z": 4, "x": 8 }
          false
          { "y": "nope", "x": {} }
          { "one": 1, "two": 2 }
          {}
          []
          """)

        val expected = ldjson("""
          1
          2
          8
          {}
          """)

        input must projectInto(".x")(expected, 12)
      }

      "prj-9 extract paths starting from root, else returning undefined" in {
        val input = ldjson("""
          { "z": "b", "x": { "y": 4 } }
          { "x": 2, "y": { "x": 1 } }
          { "a": { "x": { "z": false, "y": true } }, "b": "five" }
          { "x": { "y": 1, "z": 2 } }
          """)

        val expected = ldjson("""
          4
          1
          """)

        input must projectInto(".x.y")(expected, 4)
      }

      "prj-10 produce undefined for rows not containing array path" in {
        val input = ldjson("""
          [0, 1, 2, -1, -2]
          [3]
          [4, 5]
          { "y": 6, "z": 7 }
          ["a", "b", "c"]
          ["a", [8]]
          ["a", { "x": 9 }]
          4.8
          "seven"
          false
          null
          {}
          []
          """)

        val expected = ldjson("""
          1
          5
          "b"
          [8]
          { "x": 9 }
          """)

        input must projectInto("[1]")(expected, 13)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, projectPendingExamples, "prj")

    def evalProject(project: Project, stream: JsonStream): JsonStream

    def projectInto(path: String)(expected: JsonStream, expectedLength: Int)
      : Matcher[JsonStream] = { stream: JsonStream =>

      val result: JsonStream =
        evalProject(Project(CPath.parse(path)), stream)

      (result.length mustEqual expectedLength) and
        (result must bestSemanticEqual(expected))
    }
  }

  trait MaskSpec extends JsonSpec {
    import ColumnType._

    protected final type Mask = ScalarStage.Mask
    protected final val Mask = ScalarStage.Mask

    val maskPendingExamples: Set[Int]

    "masks" should {
      "mask-1 return undefined for everything when empty" in {
        val input = ldjson("""
          1
          "hi"
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          true
          [{ "d": {} }]
          """)

        val expected = ldjson("")

        input must maskInto()(expected, 6)
      }

      "mask-2 retain two scalar types at identity" in {
        val input = ldjson("""
          1
          "hi"
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          true
          []
          [{ "d": {} }]
          """)

        val expected = ldjson("""
          1
          true
          """)

        input must maskInto("." -> Set(Number, Boolean))(expected, 7)
      }

      "mask-3 retain different sorts of numbers at identity" in {
        val input = ldjson("""
          42
          3.14
          null
          27182e-4
          "derp"
          """)

        val expected = ldjson("""
          42
          3.14
          27182e-4
          """)

        input must maskInto("." -> Set(Number))(expected, 5)
      }

      "mask-4 retain different sorts of objects at identity" in {
        val input = ldjson("""
          1
          "hi"
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          true
          {}
          [{ "d": {} }]
          { "a": true }
          """)

        val expected = ldjson("""
          { "a": "hi", "b": { "c": null } }
          {}
          { "a": true }
          """)

        input must maskInto("." -> Set(Object))(expected, 8)
      }

      "mask-5 retain different sorts of arrays at identity" in {
        val input = ldjson("""
          1
          "hi"
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          true
          []
          [{ "d": {} }]
          { "a": true }
          """)

        val expected = ldjson("""
          [1, 2, 3]
          []
          [{ "d": {} }]
          """)

        input must maskInto("." -> Set(Array))(expected, 8)
      }

      "mask-6 retain two scalar types at .a.b" in {
        val input = ldjson("""
          { "a": { "b": 1 } }
          null
          { "a": { "b": "hi" } }
          { "foo": true }
          { "a": { "b": [1, 2, 3] } }
          [1, 2, 3]
          { "a": { "b": { "a": "hi", "b": { "c": null } } } }
          { "a": { "c": 42 } }
          { "a": { "b": true } }
          { "a": { "b": [] } }
          { "a": { "b": [{ "d": {} }] } }
          """)

        val expected = ldjson("""
          { "a": { "b": 1 } }
          { "a": { "b": true } }
          """)

        input must maskInto(".a.b" -> Set(Number, Boolean))(expected, 11)
      }

      "mask-7 retain different sorts of numbers at .a.b" in {
        val input = ldjson("""
          { "a": { "b": 42 } }
          null
          { "foo": true }
          { "a": { "b": 3.14 } }
          [1, 2, 3]
          { "a": { "b": null } }
          { "a": { "b": 27182e-4 } }
          { "a": { "b": "derp" } }
          """)

        val expected = ldjson("""
          { "a": { "b": 42 } }
          { "a": { "b": 3.14 } }
          { "a": { "b": 27182e-4 } }
          """)

        input must maskInto(".a.b" -> Set(Number))(expected, 8)
      }

      "mask-8 retain different sorts of objects at .a.b" in {
        val input = ldjson("""
          { "a": { "b": 1 } }
          { "a": { "b": "hi" } }
          { "a": { "b": [1, 2, 3] } }
          { "a": { "b": { "a": "hi", "b": { "c": null } } } }
          { "a": { "b": true } }
          { "a": { "b": {} } }
          { "a": { "b": [{ "d": {} }] } }
          { "a": { "b": { "a": true } } }
          """)

        val expected = ldjson("""
          { "a": { "b": { "a": "hi", "b": { "c": null } } } }
          { "a": { "b": {} } }
          { "a": { "b": { "a": true } } }
          """)

        input must maskInto(".a.b" -> Set(Object))(expected, 8)
      }

      "mask-9 retain different sorts of arrays at .a.b" in {
        val input = ldjson("""
          { "a": { "b": 1 } }
          { "a": { "b": "hi" } }
          { "a": { "b": [1, 2, 3] } }
          { "a": { "b": { "a": "hi", "b": { "c": null } } } }
          { "a": { "b": true } }
          { "a": { "b": [] } }
          { "a": { "b": [{ "d": {} }] } }
          { "a": { "b": { "a": true } } }
          """)

        val expected = ldjson("""
          { "a": { "b": [1, 2, 3] } }
          { "a": { "b": [] } }
          { "a": { "b": [{ "d": {} }] } }
          """)

        input must maskInto(".a.b" -> Set(Array))(expected, 8)
      }

      "mask-10 discard unmasked structure" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "c": true } }
          """)

        input must maskInto(".a.c" -> Set(Boolean))(expected, 1)
      }

      "mask-11 compose disjunctively across paths" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "c": true }, "c": [] }
          """)

        input must maskInto(".a.c" -> Set(Boolean), ".c" -> Set(Array))(expected, 1)
      }

      "mask-12 compose disjunctively across suffix-overlapped paths" in {
        val input = ldjson("""
          { "a": { "x": 42, "b": { "c": true } }, "b": { "c": [] }, "c": [1, 2] }
          """)

        val expected = ldjson("""
          { "a": { "b": { "c": true } }, "b": { "c": [] } }
          """)

        input must maskInto(".a.b.c" -> Set(Boolean), ".b.c" -> Set(Array))(expected, 1)
      }

      "mask-13 compose disjunctively across paths where one side is false" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true } }
          """)

        val expected = ldjson("""
          { "a": { "c": true } }
          """)

        input must maskInto(".a.c" -> Set(Boolean), ".a" -> Set(Array))(expected, 1)
      }

      "mask-14 subsume inner by outer" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "b": 42, "c": true } }
          """)

        input must maskInto(".a.b" -> Set(Boolean), ".a" -> Set(Object))(expected, 1)
      }

      "mask-15 disallow the wrong sort of vector" in {
        val input = ldjson("""
          { "a": true }
          [1, 2, 3]
          """)

        val expected1 = ldjson("""
          { "a": true }
          """)

        val expected2 = ldjson("""
          [1, 2, 3]
          """)

        input must maskInto("." -> Set(Object))(expected1, 2)
        input must maskInto("." -> Set(Array))(expected2, 2)
      }

      "mask-16 compact surrounding array" in {
        ldjson("[1, 2, 3]") must maskInto("[1]" -> Set(Number))(ldjson("[2]"), 1)
      }

      "mask-17 compact surrounding array with multiple values retained" in {
        val input = ldjson("""
          [1, 2, 3, 4, 5]
          """)

        val expected = ldjson("""
          [1, 3, 4]
          """)

        input must maskInto(
          "[0]" -> Set(Number),
          "[2]" -> Set(Number),
          "[3]" -> Set(Number))(expected, 1)
      }

      "mask-18 compact surrounding nested array with multiple values retained" in {
        val input = ldjson("""
          { "a": { "b": [1, 2, 3, 4, 5], "c" : null } }
          """)

        val expected = ldjson("""
          { "a": { "b": [1, 3, 4] } }
          """)

        input must maskInto(
          ".a.b[0]" -> Set(Number),
          ".a.b[2]" -> Set(Number),
          ".a.b[3]" -> Set(Number))(expected, 1)
      }

      "mask-19 compact array containing nested arrays with single nested value retained" in {
        val input = ldjson("""
          { "a": [[[1, 3, 5], "k"], "foo", { "b": [5, 6, 7], "c": [] }], "d": "x" }
          """)

        val expected = ldjson("""
          { "a": [{"b": [5, 6, 7] }] }
          """)

        input must maskInto(".a[2].b" -> Set(Array))(expected, 1)
      }

      "mask-20 remove object entirely when no values are retained" in {
        ldjson("""{ "a": 42 }""") must maskInto(".a" -> Set(Boolean))(ldjson(""), 1)
      }

      "mask-21 remove array entirely when no values are retained" in {
        ldjson("[42]") must maskInto("[0]" -> Set(Boolean))(ldjson(""), 1)
      }

      "mask-22 retain vector at depth and all recursive contents" in {
        val input = ldjson("""{ "a": { "b": { "c": { "e": true }, "d": 42 } } }""")
        input must maskInto(".a.b" -> Set(Object))(input, 1)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, maskPendingExamples, "mask")

    def evalMask(mask: Mask, stream: JsonStream): JsonStream

    def maskInto(masks: (String, Set[ColumnType])*)(expected: JsonStream, expectedLength: Int)
      : Matcher[JsonStream] = { stream: JsonStream =>

      val result: JsonStream =
        evalMask(Mask(Map(masks.map({ case (k, v) => CPath.parse(k) -> v }): _*)), stream)

      (result.length mustEqual expectedLength) and
        (result must bestSemanticEqual(expected))
    }
  }

  trait PivotSpec extends JsonSpec {

    protected final type Pivot = ScalarStage.Pivot
    protected final val Pivot = ScalarStage.Pivot

    val pivotPendingExamples: Set[Int]

    "pivot" should {
      "shift an array" >> {
        val input = ldjson("""
          [1, 2, 3]
          [4, 5, 6]
          [7, 8, 9, 10]
          [11]
          []
          [12, 13]
          """)

        "pivot-1 ExcludeId" >> {
          val expected = ldjson("""
            1
            2
            3
            4
            5
            6
            7
            8
            9
            10
            11
            12
            13
            """)

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Array)(expected, 14)
        }

        "pivot-2 IdOnly" >> {
          val expected = ldjson("""
            0
            1
            2
            0
            1
            2
            0
            1
            2
            3
            0
            0
            1
            """)

          input must pivotInto(IdStatus.IdOnly, ColumnType.Array)(expected, 14)
        }

        "pivot-3 IncludeId" >> {
          val expected = ldjson("""
            [0, 1]
            [1, 2]
            [2, 3]
            [0, 4]
            [1, 5]
            [2, 6]
            [0, 7]
            [1, 8]
            [2, 9]
            [3, 10]
            [0, 11]
            [0, 12]
            [1, 13]
            """)

          input must pivotInto(IdStatus.IncludeId, ColumnType.Array)(expected, 14)
        }
      }

      "shift an object" >> {
        val input = ldjson("""
          { "a": 1, "b": 2, "c": 3 }
          { "d": 4, "e": 5, "f": 6 }
          { "g": 7, "h": 8, "i": 9, "j": 10 }
          { "k": 11 }
          {}
          { "l": 12, "m": 13 }
          """)

        "pivot-4 ExcludeId" >> {
          val expected = ldjson("""
            1
            2
            3
            4
            5
            6
            7
            8
            9
            10
            11
            12
            13
            """)

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Object)(expected, 14)
        }

        "pivot-5 IdOnly" >> {
          val expected = ldjson("""
            "a"
            "b"
            "c"
            "d"
            "e"
            "f"
            "g"
            "h"
            "i"
            "j"
            "k"
            "l"
            "m"
            """)

          input must pivotInto(IdStatus.IdOnly, ColumnType.Object)(expected, 14)
        }

        "pivot-6 IncludeId" >> {
          val expected = ldjson("""
            ["a", 1]
            ["b", 2]
            ["c", 3]
            ["d", 4]
            ["e", 5]
            ["f", 6]
            ["g", 7]
            ["h", 8]
            ["i", 9]
            ["j", 10]
            ["k", 11]
            ["l", 12]
            ["m", 13]
            """)

          input must pivotInto(IdStatus.IncludeId, ColumnType.Object)(expected, 14)
        }
      }

      "pivot-7 preserve empty arrays as values of an array pivot" in {
        val input = ldjson("""
          [ 1, "two", [] ]
          [ [] ]
          [ [], 3, "four" ]
          """)

        val expected = ldjson("""
          1
          "two"
          []
          []
          []
          3
          "four"
        """)

        input must pivotInto(IdStatus.ExcludeId, ColumnType.Array)(expected, 7)
      }

      "pivot-8 preserve empty objects as values of an object pivot" in {
        val input = ldjson("""
          { "1": 1, "2": "two", "3": {} }
          { "4": {} }
          { "5": {}, "6": 3, "7": "four" }
          """)

        val expected = ldjson("""
          1
          "two"
          {}
          {}
          {}
          3
          "four"
        """)

        input must pivotInto(IdStatus.ExcludeId, ColumnType.Object)(expected, 7)
      }

      "pivot-9 return undefined when object pivoting a value of a different kind" in {
        val input = ldjson("""
          1
          "three"
          false
          null
          ["x", true, {}, []]
          { "a": 1, "b": "two", "c": {}, "d": [] }
          []
          {}
          """)

        val expected = ldjson("""
          1
          "two"
          {}
          []
        """)

        input must pivotInto(IdStatus.ExcludeId, ColumnType.Object)(expected, 11)
      }

      "pivot-10 return undefined when array pivoting a value of a different kind" in {
        val input = ldjson("""
          1
          "two"
          false
          null
          ["x", true, {}, []]
          { "a": 1, "b": "two", "c": {}, "d": [] }
          []
          {}
          """)

        val expected = ldjson("""
          "x"
          true
          {}
          []
        """)

        input must pivotInto(IdStatus.ExcludeId, ColumnType.Array)(expected, 11)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, pivotPendingExamples, "pivot")

    def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream

    def pivotInto(
        idStatus: IdStatus,
        structure: ColumnType.Vector)(
        expected: JsonStream, expectedLength: Int)
      : Matcher[JsonStream] = { stream: JsonStream =>

      val result: JsonStream =
        evalPivot(Pivot(idStatus, structure), stream)

      (result.length mustEqual expectedLength) and
        (result must bestSemanticEqual(expected))
    }
  }

  trait CartesianSpec extends JsonSpec {
    protected final type Cartesian = ScalarStage.Cartesian
    protected final val Cartesian = ScalarStage.Cartesian

    val cartesianPendingExamples: Set[Int]

    "cartesian" should {
      // a0 as a1, b0 as b1, c0 as c1, d0 as d1
      "cart-1 cross fields with no parse instructions" in {
        val input = ldjson("""
          { "a0": "hi", "b0": null, "c0": { "x": 42 }, "d0": [1, 2, 3] }
          """)

        val expected = ldjson("""
          { "a1": "hi", "b1": null, "c1": { "x": 42 }, "d1": [1, 2, 3] }
          """)

        val targets = Map(
          (CPathField("a1"), (CPathField("a0"), Nil)),
          (CPathField("b1"), (CPathField("b0"), Nil)),
          (CPathField("c1"), (CPathField("c0"), Nil)),
          (CPathField("d1"), (CPathField("d0"), Nil)))

        input must cartesianInto(targets)(expected, 1)
      }

      // a0 as a1, b0 as b1
      "cart-2 cross fields with no parse instructions ignoring extra fields" in {
        val input = ldjson("""
          { "a0": "hi", "b0": null, "c0": 42 }
          """)

        val expected = ldjson("""
          { "a1": "hi", "b1": null }
          """)

        val targets = Map(
          (CPathField("a1"), (CPathField("a0"), Nil)),
          (CPathField("b1"), (CPathField("b0"), Nil)))

        input must cartesianInto(targets)(expected, 1)
      }

      // a0 as a1, b0 as b1, d0 as d1
      "cart-3 cross fields with no parse instructions ignoring absent fields" in {
        val input = ldjson("""
          { "a0": "hi", "b0": null }
          """)

        val expected = ldjson("""
          { "a1": "hi", "b1": null }
          """)

        val targets = Map(
          (CPathField("a1"), (CPathField("a0"), Nil)),
          (CPathField("b1"), (CPathField("b0"), Nil)),
          (CPathField("d1"), (CPathField("d0"), Nil)))

        input must cartesianInto(targets)(expected, 1)
      }

      // a0[_] as a1, b0 as b1, c0{_} as c1
      "cart-4 cross fields with single pivot" in {
        import ScalarStage.Pivot

        val input = ldjson("""
          { "a0": [1, 2, 3], "b0": null, "c0": { "x": 4, "y": 5 } }
          """)

        val expected = ldjson("""
          { "a1": 1, "b1": null, "c1": 4 }
          { "a1": 1, "b1": null, "c1": 5 }
          { "a1": 2, "b1": null, "c1": 4 }
          { "a1": 2, "b1": null, "c1": 5 }
          { "a1": 3, "b1": null, "c1": 4 }
          { "a1": 3, "b1": null, "c1": 5 }
          """)

        val targets = Map(
          (CPathField("a1"),
            (CPathField("a0"), List(Pivot(IdStatus.ExcludeId, ColumnType.Array)))),
          (CPathField("b1"),
            (CPathField("b0"), Nil)),
          (CPathField("c1"),
            (CPathField("c0"), List(Pivot(IdStatus.ExcludeId, ColumnType.Object)))))

        input must cartesianInto(targets)(expected, 6)
      }

      // a[_].x0.y0{_} as y, a[_].x1[_] as z, b{_:} as b, c as c
      "cart-5 cross fields with multiple nested pivots" in {
        import ScalarStage.{Pivot, Project}

        val input = ldjson("""
          {
            "a": [ { "x0": { "y0": { "f": "eff", "g": "gee" }, "y1": { "h": 42 } }, "x1": [ "0", 0, null ] } ],
            "b": { "k1": null, "k2": null },
            "c": true
          }
          """)

        val expected = ldjson("""
          { "y": "eff", "z": "0" , "b": "k1", "c": true }
          { "y": "gee", "z": "0" , "b": "k1", "c": true }
          { "y": "eff", "z": 0   , "b": "k1", "c": true }
          { "y": "gee", "z": 0   , "b": "k1", "c": true }
          { "y": "eff", "z": null, "b": "k1", "c": true }
          { "y": "gee", "z": null, "b": "k1", "c": true }
          { "y": "eff", "z": "0" , "b": "k2", "c": true }
          { "y": "gee", "z": "0" , "b": "k2", "c": true }
          { "y": "eff", "z": 0   , "b": "k2", "c": true }
          { "y": "gee", "z": 0   , "b": "k2", "c": true }
          { "y": "eff", "z": null, "b": "k2", "c": true }
          { "y": "gee", "z": null, "b": "k2", "c": true }
          """)

        val targets = Map(
          (CPathField("y"),
            (CPathField("a"), List(
              Pivot(IdStatus.ExcludeId, ColumnType.Array),
              Project(CPath.parse("x0")),
              Project(CPath.parse("y0")),
              Pivot(IdStatus.ExcludeId, ColumnType.Object)))),
          (CPathField("z"),
            (CPathField("a"), List(
              Pivot(IdStatus.ExcludeId, ColumnType.Array),
              Project(CPath.parse("x1")),
              Pivot(IdStatus.ExcludeId, ColumnType.Array)))),
          (CPathField("b"),
            (CPathField("b"), List(
              Pivot(IdStatus.IdOnly, ColumnType.Object)))),
          (CPathField("c"),
            (CPathField("c"), Nil)))

        input must cartesianInto(targets)(expected, 12)
      }

      // a as a, b[_] as ba, b{_} as bm
      "cart-6 emit defined fields when some are undefined" in {
        import ScalarStage.{Mask, Pivot}

        val input = ldjson("""
          { "a": 1, "b": [ "two", "three" ] }
          { "a": 2, "b": { "x": "four", "y": "five" } }
          { "a": 3, "b": 42 }
          """)

        val expected = ldjson("""
          { "a": 1, "ba": "two" }
          { "a": 1, "ba": "three" }
          { "a": 2, "bm": "four" }
          { "a": 2, "bm": "five" }
          { "a": 3 }
          """)

        val targets = Map(
          (CPathField("a"), (CPathField("a"), Nil)),

          (CPathField("ba"), (CPathField("b"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
            Pivot(IdStatus.ExcludeId, ColumnType.Array)))),

          (CPathField("bm"), (CPathField("b"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
            Pivot(IdStatus.ExcludeId, ColumnType.Object)))))

        input must cartesianInto(targets)(expected, 5)
      }

      // a as a, b[_][_] as ba
      "cart-7 nested array pivoting retains undefined values" in {
        import ScalarStage.{Mask, Pivot}

        val input = ldjson("""
          { "a": 1, "b": [[ "two", "three" ], 12, ["four"]] }
          { "a": 2, "b": [{ "x": "four", "y": "five" }] }
          { "a": 3, "b": 42 }
          """)

        val expected = ldjson("""
          { "a": 1, "ba": "two" }
          { "a": 1, "ba": "three" }
          { "a": 1 }
          { "a": 1, "ba": "four" }
          { "a": 2 }
          { "a": 3 }
          """)

        val targets = Map(
          (CPathField("a"), (CPathField("a"), Nil)),

          (CPathField("ba"), (CPathField("b"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
            Pivot(IdStatus.ExcludeId, ColumnType.Array),
            Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
            Pivot(IdStatus.ExcludeId, ColumnType.Array)))))

        input must cartesianInto(targets)(expected, 6)
      }

      // a as a, b{_}{_} as ba
      "cart-8 nested object pivoting retains undefined values" in {
        import ScalarStage.{Mask, Pivot}

        val input = ldjson("""
          { "a": 1, "b": { "x": { "q": "two", "r": "three"}, "y": 12, "z": { "s": "four" } } }
          { "a": 2, "b": { "a": ["four", "five"] } }
          { "a": 3, "b": 42 }
          """)

        val expected = ldjson("""
          { "a": 1, "ba": "two" }
          { "a": 1, "ba": "three" }
          { "a": 1 }
          { "a": 1, "ba": "four" }
          { "a": 2 }
          { "a": 3 }
          """)

        val targets = Map(
          (CPathField("a"), (CPathField("a"), Nil)),

          (CPathField("ba"), (CPathField("b"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
            Pivot(IdStatus.ExcludeId, ColumnType.Object),
            Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
            Pivot(IdStatus.ExcludeId, ColumnType.Object)))))

        input must cartesianInto(targets)(expected, 6)
      }

      // a[_] as a, b[_] as b
      "cart-9 pivoting retains row alignment through undefineds" in {
        import ScalarStage.{Mask, Pivot}

        val input = ldjson("""
          { "a": [1], "b": [4, 5] }
          { "a": [2] }
          { "a": [3], "b": [6] }
          """)

        val expected = ldjson("""
          { "a": 1, "b": 4 }
          { "a": 1, "b": 5 }
          { "a": 2 }
          { "a": 3, "b": 6 }
          """)

        val targets = Map(
          (CPathField("a"), (CPathField("a"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
            Pivot(IdStatus.ExcludeId, ColumnType.Array)))),

          (CPathField("b"), (CPathField("b"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
            Pivot(IdStatus.ExcludeId, ColumnType.Array)))))

        input must cartesianInto(targets)(expected, 4)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, cartesianPendingExamples, "cart")

    def evalCartesian(cartesian: Cartesian, stream: JsonStream): JsonStream

    def cartesianInto(
        cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])])(
        expected: JsonStream, expectedLength: Int)
      : Matcher[JsonStream] = { stream: JsonStream =>

      val result: JsonStream =
        evalCartesian(Cartesian(cartouches), stream)

      (result.length mustEqual expectedLength) and
        (result must bestSemanticEqual(expected))
    }
  }
}
