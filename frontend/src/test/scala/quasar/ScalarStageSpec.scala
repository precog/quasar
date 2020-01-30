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

package quasar

import slamdata.Predef._

import quasar.api.table.ColumnType
import quasar.common.{CPath, CPathField}

import org.specs2.execute.PendingUntilFixed._
import org.specs2.matcher.Matcher
import org.specs2.specification.core.SpecStructure

import scala.collection.immutable.{Map, Set}

import java.lang.String

abstract class ScalarStageSpec
    extends ScalarStageSpec.IdsSpec
    with ScalarStageSpec.WrapSpec
    with ScalarStageSpec.ProjectSpec
    with ScalarStageSpec.MaskSpec
    with ScalarStageSpec.PivotSpec
    with ScalarStageSpec.FocusedSpec
    with ScalarStageSpec.CartesianSpec
    with ScalarStageSpec.FullSpec

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

        input must interpretIdsAs(ExcludeId, input)
      }

      "ids-2 emit vector rows unmodified" in {
        val input = ldjson("""
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          [{ "d": {} }]
          """)

        input must interpretIdsAs(ExcludeId, input)
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

        input must interpretIdsAs(IdOnly, expected)
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

        input must interpretIdsAs(IdOnly, expected)
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

        input must interpretIdsAs(IncludeId, expected)
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

        input must interpretIdsAs(IncludeId, expected)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, idsPendingExamples, "ids")

    def evalIds(idStatus: IdStatus, stream: JsonStream): JsonStream

    def interpretIdsAs(idStatus: IdStatus, expected: JsonStream) : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream => evalIds(idStatus, str) }
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

        input must wrapInto("foo")(expected)
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

        input must wrapInto("bar")(expected)
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

        input must wrapInto("bar")(expected)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, wrapPendingExamples, "wrap")

    def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream

    def wrapInto(name: String)(expected: JsonStream): Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream => evalWrap(Wrap(name), str)}
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

        input must projectInto(".")(input)
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

        input must projectInto(".a")(expected)
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

        input must projectInto(".a.b")(expected)
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

        input must projectInto(".a[1]")(expected)
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

        input must projectInto("[1]")(expected)
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

        input must projectInto("[1][0]")(expected)
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

        input must projectInto("[1].a")(expected)
      }

      "prj-8 elide rows not containing object path" in {
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

        input must projectInto(".x")(expected)
      }

      "prj-9 only extract paths starting from root" in {
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

        input must projectInto(".x.y")(expected)
      }

      "prj-10 elide rows not containing array path" in {
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

        input must projectInto("[1]")(expected)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, projectPendingExamples, "prj")

    def evalProject(project: Project, stream: JsonStream): JsonStream

    def projectInto(path: String)(expected: JsonStream): Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream =>
        evalProject(Project(CPath.parse(path)), str)
      }
  }

  trait MaskSpec extends JsonSpec {
    import ColumnType._

    protected final type Mask = ScalarStage.Mask
    protected final val Mask = ScalarStage.Mask

    val maskPendingExamples: Set[Int]

    "masks" should {
      "mask-1 drop everything when empty" in {
        val input = ldjson("""
          1
          "hi"
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          true
          [{ "d": {} }]
          """)

        val expected = ldjson("")

        input must maskInto()(expected)
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

        input must maskInto("." -> Set(Number, Boolean))(expected)
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

        input must maskInto("." -> Set(Number))(expected)
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

        input must maskInto("." -> Set(Object))(expected)
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

        input must maskInto("." -> Set(Array))(expected)
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

        input must maskInto(".a.b" -> Set(Number, Boolean))(expected)
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

        input must maskInto(".a.b" -> Set(Number))(expected)
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

        input must maskInto(".a.b" -> Set(Object))(expected)
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

        input must maskInto(".a.b" -> Set(Array))(expected)
      }

      "mask-10 discard unmasked structure" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "c": true } }
          """)

        input must maskInto(".a.c" -> Set(Boolean))(expected)
      }

      "mask-11 compose disjunctively across paths" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "c": true }, "c": [] }
          """)

        input must maskInto(".a.c" -> Set(Boolean), ".c" -> Set(Array))(expected)
      }

      "mask-12 compose disjunctively across suffix-overlapped paths" in {
        val input = ldjson("""
          { "a": { "x": 42, "b": { "c": true } }, "b": { "c": [] }, "c": [1, 2] }
          """)

        val expected = ldjson("""
          { "a": { "b": { "c": true } }, "b": { "c": [] } }
          """)

        input must maskInto(".a.b.c" -> Set(Boolean), ".b.c" -> Set(Array))(expected)
      }

      "mask-13 compose disjunctively across paths where one side is false" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true } }
          """)

        val expected = ldjson("""
          { "a": { "c": true } }
          """)

        input must maskInto(".a.c" -> Set(Boolean), ".a" -> Set(Array))(expected)
      }

      "mask-14 subsume inner by outer" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "b": 42, "c": true } }
          """)

        input must maskInto(".a.b" -> Set(Boolean), ".a" -> Set(Object))(expected)
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

        input must maskInto("." -> Set(Object))(expected1)
        input must maskInto("." -> Set(Array))(expected2)
      }

      "mask-16 compact surrounding array" in {
        ldjson("[1, 2, 3]") must maskInto("[1]" -> Set(Number))(ldjson("[2]"))
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
          "[3]" -> Set(Number))(expected)
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
          ".a.b[3]" -> Set(Number))(expected)
      }

      "mask-19 compact array containing nested arrays with single nested value retained" in {
        val input = ldjson("""
          { "a": [[[1, 3, 5], "k"], "foo", { "b": [5, 6, 7], "c": [] }], "d": "x" }
          """)

        val expected = ldjson("""
          { "a": [{"b": [5, 6, 7] }] }
          """)

        input must maskInto(".a[2].b" -> Set(Array))(expected)
      }

      "mask-20 remove object entirely when no values are retained" in {
        ldjson("""{ "a": 42 }""") must maskInto(".a" -> Set(Boolean))(ldjson(""))
      }

      "mask-21 remove array entirely when no values are retained" in {
        ldjson("[42]") must maskInto("[0]" -> Set(Boolean))(ldjson(""))
      }

      "mask-22 retain vector at depth and all recursive contents" in {
        val input = ldjson("""{ "a": { "b": { "c": { "e": true }, "d": 42 } } }""")
        input must maskInto(".a.b" -> Set(Object))(input)
      }

      // minimization of `multilevelFlatten.test`
      "mask-23 disjunctively retain values in an array" in {
        val input = ldjson("""
          ["a", 13]
          ["b", []]
          ["c", {}]
          ["d", [12]]
          ["e", { "z": 14}]
          """)

        val expected = ldjson("""
          ["a"]
          ["b"]
          ["c", {}]
          ["d"]
          ["e", { "z": 14}]
          """)

        input must maskInto("[0]" -> ColumnType.Top, "[1]" -> Set(ColumnType.Object))(expected)
      }

      "mask-24 disjunctively retain values in an array with compaction" in {
        val input = ldjson("""
          [13, "a"]
          [[], "b"]
          [{}, "c"]
          [[12], "d"]
          [{ "z": 14}, "e"]
          """)

        val expected = ldjson("""
          ["a"]
          ["b"]
          [{}, "c"]
          ["d"]
          [{ "z": 14}, "e"]
          """)

        input must maskInto("[0]" -> Set(ColumnType.Object), "[1]" -> ColumnType.Top)(expected)
      }

      "mask-25 disjunctively retain values in an object" in {
        val input = ldjson("""
          { "v": "a", "w": 13 }
          { "v": "b", "w": [] }
          { "v": "c", "w": {} }
          { "v": "d", "w": [12] }
          { "v": "e", "w": { "z": 14} }
          """)

        val expected = ldjson("""
          { "v": "a" }
          { "v": "b" }
          { "v": "c", "w": {} }
          { "v": "d" }
          { "v": "e", "w": { "z": 14} }
          """)

        input must maskInto(".v" -> ColumnType.Top, ".w" -> Set(ColumnType.Object))(expected)
      }

      "mask all values at Top to themselves" >> {
        // minimization of `arrayLengthHeterogeneous.test`
        "mask-26 at identity path" in {
          val input = ldjson("""
            [[1], {"z":2}, [], {}, "foo", null, 42, 42.2, true]
            {"a":[1], "b":{"z":2}, "c":[], "d":{}, "e":"foo", "f":null, "g":42, "h":42.2, "i":true}
            []
            {}
            "foo"
            null
            42
            42.2
            true
            """)

          input must maskInto("." -> ColumnType.Top)(input)
        }

        "mask-27 at object projected path" in {
          val input = ldjson("""
            {"y": [[1], {"z":2}, [], {}, "foo", null, 42, 42.2, true]}
            {"y": {"a":[1], "b":{"z":2}, "c":[], "d":{}, "e":"foo", "f":null, "g":42, "h":42.2, "i":true}}
            {"y": []}
            {"y": {}}
            {"y": "foo"}
            {"y": null}
            {"y": 42}
            {"y": 42.2}
            {"y": true}
            """)

          input must maskInto(".y" -> ColumnType.Top)(input)
        }

        "mask-28 at array projected path" in {
          val input = ldjson("""
            [[[1], {"z":2}, [], {}, "foo", null, 42, 42.2, true]]
            [{"a":[1], "b":{"z":2}, "c":[], "d":{}, "e":"foo", "f":null, "g":42, "h":42.2, "i":true}]
            [[]]
            [{}]
            ["foo"]
            [null]
            [42]
            [42.2]
            [true]
            """)

          input must maskInto("[0]" -> ColumnType.Top)(input)
        }
      }

      "retain each non-temporal scalar type at identity" >> {
        val input = ldjson("""
          1
          2.2
          27182e-4
          "hi"
          true
          false
          null
          []
          {}
          [1, 2, 3]
          { "a": "hi", "b": null }
          """)

        "mask-29 Null" in {
          val expected = ldjson("""null""")
          input must maskInto("." -> Set(Null))(expected)
        }

        "mask-30 Boolean" in {
          val expected = ldjson("""
            true
            false
            """)
          input must maskInto("." -> Set(Boolean))(expected)
        }

        "mask-31 Number" in {
          val expected = ldjson("""
            1
            2.2
            27182e-4
            """)
          input must maskInto("." -> Set(Number))(expected)
        }

        "mask-32 String" in {
          val expected = ldjson(""""hi"""")
          input must maskInto("." -> Set(ColumnType.String))(expected)
        }
      }

      "mask-33 mask multiple columns at Top" in {
        val input = ldjson("""
          { "x": "hi", "y": null }
          [4, 5]
          """)

        input must maskInto(
          ".x" -> ColumnType.Top,
          ".y" -> ColumnType.Top,
          "[0]" -> ColumnType.Top,
          "[1]" -> ColumnType.Top)(input)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, maskPendingExamples, "mask")

    def evalMask(mask: Mask, stream: JsonStream): JsonStream

    def maskInto(
        masks: (String, Set[ColumnType])*)(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream =>
        evalMask(Mask(Map(masks.map({ case (k, v) => CPath.parse(k) -> v }): _*)), str)
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

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Array)(expected)
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

          input must pivotInto(IdStatus.IdOnly, ColumnType.Array)(expected)
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

          input must pivotInto(IdStatus.IncludeId, ColumnType.Array)(expected)
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

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Object)(expected)
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

          input must pivotInto(IdStatus.IdOnly, ColumnType.Object)(expected)
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

          input must pivotInto(IdStatus.IncludeId, ColumnType.Object)(expected)
        }
      }

      "omit undefined row in object pivot" >> {
        val input = ldjson("""
          { "a": 1 }
          12
          { "b": 2 }
          """)

        "pivot-12 ExcludeId" in {
          val expected = ldjson("""
            1
            2
            """)

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Object)(expected)
        }

        "pivot-13 IdOnly" in {
          val expected = ldjson("""
            "a"
            "b"
            """)

          input must pivotInto(IdStatus.IdOnly, ColumnType.Object)(expected)
        }

        "pivot-14 IncludeId" in {
          val expected = ldjson("""
            ["a", 1]
            ["b", 2]
            """)

          input must pivotInto(IdStatus.IncludeId, ColumnType.Object)(expected)
        }
      }

      "omit undefined row in array pivot" >> {
        val input = ldjson("""
          [11]
          12
          [13]
          """)

        "pivot-15 ExcludeId" in {
          val expected = ldjson("""
            11
            13
            """)

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Array)(expected)
        }

        "pivot-16 IdOnly" in {
          val expected = ldjson("""
            0
            0
            """)

          input must pivotInto(IdStatus.IdOnly, ColumnType.Array)(expected)
        }

        "pivot-17 IncludeId" in {
          val expected = ldjson("""
            [0, 11]
            [0, 13]
            """)

          input must pivotInto(IdStatus.IncludeId, ColumnType.Array)(expected)
        }
      }

      "preserve empty arrays as values of an array pivot" >> {
        val input = ldjson("""
          [ 1, "two", [] ]
          [ [] ]
          [ [], 3, "four" ]
          """)

        "pivot-18 ExludeId" in {
          val expected = ldjson("""
            1
            "two"
            []
            []
            []
            3
            "four"
          """)

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Array)(expected)
        }

        "pivot-19 IdOnly" in {
          val expected = ldjson("""
            0
            1
            2
            0
            0
            1
            2
          """)

          input must pivotInto(IdStatus.IdOnly, ColumnType.Array)(expected)
        }

        "pivot-20 IncludeId" in {
          val expected = ldjson("""
            [0, 1]
            [1, "two"]
            [2, []]
            [0, []]
            [0, []]
            [1, 3]
            [2, "four"]
          """)

          input must pivotInto(IdStatus.IncludeId, ColumnType.Array)(expected)
        }
      }

      "preserve empty objects as values of an object pivot" >> {
        val input = ldjson("""
          { "1": 1, "2": "two", "3": {} }
          { "4": {} }
          { "5": {}, "6": 3, "7": "four" }
          """)

       "pivot-21 ExcludeId" in {
          val expected = ldjson("""
            1
            "two"
            {}
            {}
            {}
            3
            "four"
          """)

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Object)(expected)
        }

       "pivot-22 IdOnly" in {
          val expected = ldjson("""
            "1"
            "2"
            "3"
            "4"
            "5"
            "6"
            "7"
          """)

          input must pivotInto(IdStatus.IdOnly, ColumnType.Object)(expected)
        }

       "pivot-23 IncludeId" in {
          val expected = ldjson("""
            ["1", 1]
            ["2", "two"]
            ["3", {}]
            ["4", {}]
            ["5", {}]
            ["6", 3]
            ["7", "four"]
          """)

          input must pivotInto(IdStatus.IncludeId, ColumnType.Object)(expected)
        }
      }

      "omit results when object pivoting a value of a different kind" >> {
        val input = ldjson("""
          1
          "three"
          false
          null
          ["x", true, {}, []]
          { "a": 1, "b": "two", "c": {}, "d": [] }
          """)

        "pivot-24 ExcludeId" in {
          val expected = ldjson("""
            1
            "two"
            {}
            []
          """)

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Object)(expected)
        }

        "pivot-25 IdOnly" in {
          val expected = ldjson("""
            "a"
            "b"
            "c"
            "d"
          """)

          input must pivotInto(IdStatus.IdOnly, ColumnType.Object)(expected)
        }

        "pivot-26 IncludeId" in {
          val expected = ldjson("""
            ["a", 1]
            ["b", "two"]
            ["c", {}]
            ["d", []]
          """)

          input must pivotInto(IdStatus.IncludeId, ColumnType.Object)(expected)
        }
      }

      "pivot-10 omit results when array pivoting a value of a different kind" >> {
        val input = ldjson("""
          1
          "two"
          false
          null
          ["x", true, {}, []]
          { "a": 1, "b": "two", "c": {}, "d": [] }
          """)

        "pivot-27 ExcludeId" in {
          val expected = ldjson("""
            "x"
            true
            {}
            []
          """)

          input must pivotInto(IdStatus.ExcludeId, ColumnType.Array)(expected)
        }

        "pivot-28 IdOnly" in {
          val expected = ldjson("""
            0
            1
            2
            3
          """)

          input must pivotInto(IdStatus.IdOnly, ColumnType.Array)(expected)
        }

        "pivot-29 IncludeId" in {
          val expected = ldjson("""
            [0, "x"]
            [1, true]
            [2, {}]
            [3, []]
          """)

          input must pivotInto(IdStatus.IncludeId, ColumnType.Array)(expected)
        }
      }

      "omit empty vector from pivot results" >> {
        val input = ldjson("""
          {}
          []
        """)

        "pivot-30 ExcludeId" in {
          input must pivotInto(IdStatus.ExcludeId, ColumnType.Array)(ldjson(""))
          input must pivotInto(IdStatus.ExcludeId, ColumnType.Object)(ldjson(""))
        }

        "pivot-31 IdOnly" in {
          input must pivotInto(IdStatus.IdOnly, ColumnType.Array)(ldjson(""))
          input must pivotInto(IdStatus.IdOnly, ColumnType.Object)(ldjson(""))
        }

        "pivot-32 IncludeId" in {
          input must pivotInto(IdStatus.IncludeId, ColumnType.Array)(ldjson(""))
          input must pivotInto(IdStatus.IncludeId, ColumnType.Object)(ldjson(""))
        }
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, pivotPendingExamples, "pivot")

    def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream

    def pivotInto(
        idStatus: IdStatus,
        structure: ColumnType.Vector)(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream =>
        evalPivot(Pivot(idStatus, structure), str)
      }
  }

  trait FocusedSpec extends JsonSpec {
    import ColumnType._
    import IdStatus._

    import ScalarStage.{Mask, Pivot, Project, Wrap}

    val focusedPendingExamples: Set[Int]

    "sequential focused stages" should {

      "foc-5 Wrap . Pivot (no-op)" in {
        val stages = List(
          Wrap("foo"),
          Pivot(ExcludeId, Object))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          { "a": 7, "b": "two" }
          []
          {}
          """)

        input must interpretInto(stages)(input)
      }

      "foc-6 Wrap . Pivot (empty set)" in {
        val stages = List(
          Wrap("foo"),
          Pivot(ExcludeId, Array))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          { "a": 7, "b": "two" }
          []
          {}
          """)

        input must interpretInto(stages)(ldjson(""))
      }

      "foc-7 Wrap . Wrap" in {
        val stages = List(
          Wrap("foo"),
          Wrap("bar"))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          { "a": 7, "b": "two" }
          []
          {}
          """)

        val expected = ldjson("""
          { "bar": { "foo": 1 } }
          { "bar": { "foo": "two" } }
          { "bar": { "foo": false } }
          { "bar": { "foo": null } }
          { "bar": { "foo": [1, 2, 3] } }
          { "bar": { "foo": { "a": 7, "b": "two" } } }
          { "bar": { "foo": [] } }
          { "bar": { "foo": {} } }
          """)

        input must interpretInto(stages)(expected)
      }

      "foc-8 Wrap . Project (no-op)" in {
        val stages = List(
          Wrap("foo"),
          project("foo"))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          { "a": 7, "b": "two" }
          []
          {}
          """)

        input must interpretInto(stages)(input)
      }

      "foc-9 Wrap . Project (empty set)" in {
        val stages = List(
          Wrap("foo"),
          project("bar"))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          { "a": 7, "b": "two" }
          []
          {}
          """)

        input must interpretInto(stages)(ldjson(""))
      }

      "foc-10 Wrap . Mask (identity)" in {
        val stages = List(
          Wrap("foo"),
          mask("." -> Set(Object)))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          { "a": 7, "b": "two" }
          []
          {}
          """)

        val expected = ldjson("""
          { "foo": 1 }
          { "foo": "two" }
          { "foo": false }
          { "foo": null }
          { "foo": [1, 2, 3] }
          { "foo": { "a": 7, "b": "two" } }
          { "foo": [] }
          { "foo": {} }
          """)

        input must interpretInto(stages)(expected)
      }

      "foc-11 Wrap . Mask (empty set)" in {
        val stages = List(
          Wrap("foo"),
          mask("." -> Set(Array)))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          { "a": 7, "b": "two" }
          []
          {}
          """)

        input must interpretInto(stages)(ldjson(""))
      }

      "foc-12 Project . Pivot (object)" in {
        val stages = List(
          project("foo"),
          Pivot(ExcludeId, Object))

        val input = ldjson("""
          { "foo": 1 }
          { "foo": "two" }
          { "foo": false }
          { "foo": null }
          { "foo": [1, 2, 3] }
          { "foo": { "a": 7, "b": "two" } }
          { "foo": [] }
          { "foo": {} }
          { "bar": 2.2 }
          true
          """)

        val expected = ldjson("""
          7
          "two"
          """)

        input must interpretInto(stages)(expected)
      }

      "foc-13 Project . Pivot (array)" in {
        val stages = List(
          project("foo"),
          Pivot(ExcludeId, Array))

        val input = ldjson("""
          { "foo": 1 }
          { "foo": "two" }
          { "foo": false }
          { "foo": null }
          { "foo": [1, 2, 3] }
          { "foo": { "a": 7, "b": "two" } }
          { "foo": [] }
          { "foo": {} }
          { "bar": 2.2 }
          true
          """)

        val expected = ldjson("""
          1
          2
          3
          """)

        input must interpretInto(stages)(expected)
      }

      "foc-14 Project . Wrap" in {
        val stages = List(
          project("foo"),
          Wrap("foo"))

        val input = ldjson("""
          { "foo": 1 }
          { "foo": "two", "bar": "a" }
          { "foo": false, "baz": -1, "ack": -2 }
          { "foo": null }
          { "foo": [1, 2, 3] }
          { "foo": { "a": 7, "b": "two" } }
          { "foo": [] }
          { "foo": {} }
          { "bar": 2.2 }
          true
          """)

        val expected = ldjson("""
          { "foo": 1 }
          { "foo": "two" }
          { "foo": false }
          { "foo": null }
          { "foo": [1, 2, 3] }
          { "foo": { "a": 7, "b": "two" } }
          { "foo": [] }
          { "foo": {} }
          """)

        input must interpretInto(stages)(expected)
      }

      "foc-15 Project . Project" in {
        val stages = List(
          project("foo"),
          project("a"))

        val input = ldjson("""
          { "foo": 1 }
          { "foo": "two", "bar": "a" }
          { "foo": false, "baz": -1, "ack": -2 }
          { "foo": null }
          { "foo": [1, 2, 3] }
          { "foo": { "a": 7, "b": "two" } }
          { "foo": [] }
          { "foo": {} }
          { "a": 2.2 }
          true
          """)

        val expected = ldjson("""
          7
          """)

        input must interpretInto(stages)(expected)
      }

      "foc-16 Project . Mask" in {
        val stages = List(
          project("foo"),
          mask("." -> Set(Object)))

        val input = ldjson("""
          { "foo": 1 }
          { "foo": "two", "bar": "a" }
          { "foo": false, "baz": -1, "ack": -2 }
          { "foo": null }
          { "foo": [1, 2, 3] }
          { "foo": { "a": 7, "b": "two" } }
          { "foo": [] }
          { "foo": {} }
          { "a": 2.2 }
          true
          """)

        val expected = ldjson("""
          { "a": 7, "b": "two" }
          {}
          """)

        input must interpretInto(stages)(expected)
      }

      "foc-17 Mask . Pivot" in {
        val stages = List(
          mask("." -> Set(Object)),
          Pivot(ExcludeId, Object))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          { "a": 1, "b": "two" }
          []
          {}
          """)

        val expected = ldjson("""
          1
          "two"
        """)

        input must interpretInto(stages)(expected)
      }

      "foc-18 Mask . Wrap" in {
        val stages = List(
          mask("." -> Set(Object)),
          Wrap("foo"))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          [1, 2, 3]
          { "a": 1, "b": "two" }
          []
          {}
          """)

        val expected = ldjson("""
          { "foo": { "a": 1, "b": "two" } }
          { "foo": {} }
        """)

        input must interpretInto(stages)(expected)
      }

      "foc-19 Mask . Project" in {
        val stages = List(
          mask("." -> Set(Object)),
          project("b"))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          [1, 2, 3]
          { "a": 1, "b": "two" }
          []
          {}
          """)

        val expected = ldjson("""
          "two"
        """)

        input must interpretInto(stages)(expected)
      }

      "foc-20 Mask . Mask" in {
        val stages = List(
          mask("." -> Set(Object)),
          mask("." -> Set(Object)))

        val input = ldjson("""
          1
          "two"
          false
          null
          [1, 2, 3]
          [1, 2, 3]
          { "a": 1, "b": "two" }
          []
          {}
          """)

        val expected = ldjson("""
          { "a": 1, "b": "two" }
          {}
        """)

        input must interpretInto(stages)(expected)
      }

      "Pivot . Wrap" >> {
        val input = ldjson("""
          1
          "two"
          false
          null
          [2, "foo"]
          [3, [2.2]]
          [4, { "p": true }]
          { "a": 5, "b": "bar" }
          { "a": 6, "b": [1.1] }
          { "a": 7, "b": { "z": false } }
          []
          {}
          """)

        "Object" >> {
          "foc-29 IncludeId" in {
            val stages = List(Pivot(IncludeId, Object), Wrap("foo"))
            val expected = ldjson("""
              { "foo": ["a", 5] }
              { "foo": ["b", "bar"] }
              { "foo": ["a", 6] }
              { "foo": ["b", [1.1]] }
              { "foo": ["a", 7] }
              { "foo": ["b", { "z": false }] }
            """)
            input must interpretInto(stages)(expected)
          }
          "foc-30 ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Object), Wrap("foo"))
            val expected = ldjson("""
              { "foo": 5 }
              { "foo": "bar" }
              { "foo": 6 }
              { "foo": [1.1] }
              { "foo": 7 }
              { "foo": { "z": false } }
            """)
            input must interpretInto(stages)(expected)
          }
          "foc-31 IdOnly" in {
            val stages = List(Pivot(IdOnly, Object), Wrap("foo"))
            val expected = ldjson("""
              { "foo": "a" }
              { "foo": "b" }
              { "foo": "a" }
              { "foo": "b" }
              { "foo": "a" }
              { "foo": "b" }
            """)
            input must interpretInto(stages)(expected)
          }
        }
        "Array" >> {
          "foc-32 IncludeId" in {
            val stages = List(Pivot(IncludeId, Array), Wrap("foo"))
            val expected = ldjson("""
              { "foo": [0, 2] }
              { "foo": [1, "foo"] }
              { "foo": [0, 3] }
              { "foo": [1, [2.2]] }
              { "foo": [0, 4] }
              { "foo": [1, { "p": true }] }
            """)
            input must interpretInto(stages)(expected)
          }
          "foc-33 ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Array), Wrap("foo"))
            val expected = ldjson("""
              { "foo": 2 }
              { "foo": "foo" }
              { "foo": 3 }
              { "foo": [2.2] }
              { "foo": 4 }
              { "foo": { "p": true } }
            """)
            input must interpretInto(stages)(expected)
          }
          "foc-34 IdOnly" in {
            val stages = List(Pivot(IdOnly, Array), Wrap("foo"))
            val expected = ldjson("""
              { "foo": 0 }
              { "foo": 1 }
              { "foo": 0 }
              { "foo": 1 }
              { "foo": 0 }
              { "foo": 1 }
            """)
            input must interpretInto(stages)(expected)
          }
        }
      }

      "Pivot . Mask" >> {
        val input = ldjson("""
          1
          "two"
          false
          null
          [2, "foo"]
          [3, [2.2]]
          [4, { "p": true }]
          [5, {}]
          { "a": 6, "b": "bar" }
          { "a": 7, "b": [1.1] }
          { "a": 8, "b": { "z": false } }
          { "a": 9, "b": {} }
          []
          {}
          """)

        "Object" >> {
          "foc-35 IncludeId" in {
            val stages = List(
              Pivot(IncludeId, Object),
              Mask(Map(
                CPath.parse("[0]") -> ColumnType.Top,
                CPath.parse("[1]") -> Set(ColumnType.Object))))
            val expected = ldjson("""
              ["a"]
              ["b"]
              ["a"]
              ["b"]
              ["a"]
              ["b", { "z": false }]
              ["a"]
              ["b", {}]
            """)
            input must interpretInto(stages)(expected)
          }
          "foc-36 ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Object), mask("." -> Set(Object)))
            val expected = ldjson("""
              { "z": false }
              {}
            """)
            input must interpretInto(stages)(expected)
          }
          "foc-37 IdOnly" in {
            val stages = List(Pivot(IdOnly, Object), mask("." -> Set(ColumnType.String)))
            val expected = ldjson("""
              "a"
              "b"
              "a"
              "b"
              "a"
              "b"
              "a"
              "b"
            """)
            input must interpretInto(stages)(expected)
          }
        }
        "Array" >> {
          "foc-38 IncludeId" in {
            val stages = List(
              Pivot(IncludeId, Array),
              Mask(Map(
                CPath.parse("[0]") -> ColumnType.Top,
                CPath.parse("[1]") -> Set(ColumnType.Object))))
            val expected = ldjson("""
              [0]
              [1]
              [0]
              [1]
              [0]
              [1, { "p": true }]
              [0]
              [1, {}]
            """)
            input must interpretInto(stages)(expected)
          }
          "foc-39 ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Array), mask("." -> Set(Object)))
            val expected = ldjson("""
              { "p": true }
              {}
            """)
            input must interpretInto(stages)(expected)
          }
          "foc-40 IdOnly" in {
            val stages = List(Pivot(IdOnly, Array), mask("." -> Set(ColumnType.Number)))
            val expected = ldjson("""
              0
              1
              0
              1
              0
              1
              0
              1
            """)
            input must interpretInto(stages)(expected)
          }
        }
      }

      "Pivot . Project" >> {
        val input = ldjson("""
          1
          "two"
          false
          null
          [2, "foo"]
          [3, [2.2]]
          [4, { "p": 10 }]
          [5, {}]
          { "a": 6, "b": "bar" }
          { "a": 7, "b": [1.1] }
          { "a": 8, "b": { "z": 11 } }
          { "a": 9, "b": {} }
          []
          {}
          """)

        "Object" >> {
          "foc-41 IncludeId" in {
            val stages = List(Pivot(IncludeId, Object), project("z"))
            val expected = ldjson("")
            input must interpretInto(stages)(expected)
          }
          "foc-42 ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Object), project("z"))
            val expected = ldjson("11")
            input must interpretInto(stages)(expected)
          }
          "foc-43 IdOnly" in {
            val stages = List(Pivot(IdOnly, Object), project("z"))
            val expected = ldjson("")
            input must interpretInto(stages)(expected)
          }
        }
        "Array" >> {
          "foc-44 IncludeId" in {
            val stages = List(Pivot(IncludeId, Array), project("p"))
            val expected = ldjson("")
            input must interpretInto(stages)(expected)
          }
          "foc-45 ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Array), project("p"))
            val expected = ldjson("10")
            input must interpretInto(stages)(expected)
          }
          "foc-46 IdOnly" in {
            val stages = List(Pivot(IdOnly, Array), project("p"))
            val expected = ldjson("")
            input must interpretInto(stages)(expected)
          }
        }
      }

      "Pivot . Pivot" >> {
        val input = ldjson("""
          1
          "two"
          false
          null
          [2, "foo"]
          [3, [2.2]]
          [4, { "p": 10 }]
          [5, {}]
          { "a": 6, "b": "bar" }
          { "a": 7, "b": [1.1] }
          { "a": 8, "b": { "z": 11 } }
          { "a": 9, "b": {} }
          []
          {}
          """)

        "Object Object" >> {
          "foc-47 IncludeId IncludeId" in {
            val stages = List(Pivot(IncludeId, Object), Pivot(IncludeId, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-48 IncludeId ExcludeId" in {
            val stages = List(Pivot(IncludeId, Object), Pivot(ExcludeId, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-49 IncludeId IdOnly" in {
            val stages = List(Pivot(IncludeId, Object), Pivot(IdOnly, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-50 ExcludeId IncludeId" in {
            val stages = List(Pivot(ExcludeId, Object), Pivot(IncludeId, Object))
            input must interpretInto(stages)(ldjson("""["z", 11]"""))
          }
          "foc-51 ExcludeId ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Object), Pivot(ExcludeId, Object))
            input must interpretInto(stages)(ldjson("11"))
          }
          "foc-52 ExcludeId IdOnly" in {
            val stages = List(Pivot(ExcludeId, Object), Pivot(IdOnly, Object))
            input must interpretInto(stages)(ldjson(""""z""""))
          }
          "foc-53 IdOnly IncludeId" in {
            val stages = List(Pivot(IdOnly, Object), Pivot(IncludeId, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-54 IdOnly ExcludeId" in {
            val stages = List(Pivot(IdOnly, Object), Pivot(ExcludeId, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-55 IdOnly IdOnly" in {
            val stages = List(Pivot(IdOnly, Object), Pivot(IdOnly, Object))
            input must interpretInto(stages)(ldjson(""))
          }
        }

        "Object Array" >> {
          "foc-56 IncludeId IncludeId" in {
            val stages = List(Pivot(IncludeId, Object), Pivot(IncludeId, Array))
            val expected = ldjson("""
              [0, "a"]
              [1, 6]
              [0, "b"]
              [1, "bar"]
              [0, "a"]
              [1, 7]
              [0, "b"]
              [1, [1.1]]
              [0, "a"]
              [1, 8]
              [0, "b"]
              [1, { "z": 11 }]
              [0, "a"]
              [1, 9]
              [0, "b"]
              [1, {}]
              """)
            input must interpretInto(stages)(expected)
          }
          "foc-57 IncludeId ExcludeId" in {
            val stages = List(Pivot(IncludeId, Object), Pivot(ExcludeId, Array))
            val expected = ldjson("""
              "a"
              6
              "b"
              "bar"
              "a"
              7
              "b"
              [1.1]
              "a"
              8
              "b"
              { "z": 11 }
              "a"
              9
              "b"
              {}
              """)
            input must interpretInto(stages)(expected)
          }
          "foc-58 IncludeId IdOnly" in {
            val stages = List(Pivot(IncludeId, Object), Pivot(IdOnly, Array))
            val expected = ldjson("""
              0
              1
              0
              1
              0
              1
              0
              1
              0
              1
              0
              1
              0
              1
              0
              1
              """)
            input must interpretInto(stages)(expected)
          }
          "foc-59 ExcludeId IncludeId" in {
            val stages = List(Pivot(ExcludeId, Object), Pivot(IncludeId, Array))
            input must interpretInto(stages)(ldjson("[0, 1.1]"))
          }
          "foc-60 ExcludeId ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Object), Pivot(ExcludeId, Array))
            input must interpretInto(stages)(ldjson("1.1"))
          }
          "foc-61 ExcludeId IdOnly" in {
            val stages = List(Pivot(ExcludeId, Object), Pivot(IdOnly, Array))
            input must interpretInto(stages)(ldjson("0"))
          }
          "foc-62 IdOnly IncludeId" in {
            val stages = List(Pivot(IdOnly, Object), Pivot(IncludeId, Array))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-63 IdOnly ExcludeId" in {
            val stages = List(Pivot(IdOnly, Object), Pivot(ExcludeId, Array))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-64 IdOnly IdOnly" in {
            val stages = List(Pivot(IdOnly, Object), Pivot(IdOnly, Array))
            input must interpretInto(stages)(ldjson(""))
          }
        }

        "Array Object" >> {
          "foc-65 IncludeId IncludeId" in {
            val stages = List(Pivot(IncludeId, Array), Pivot(IncludeId, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-66 IncludeId ExcludeId" in {
            val stages = List(Pivot(IncludeId, Array), Pivot(ExcludeId, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-67 IncludeId IdOnly" in {
            val stages = List(Pivot(IncludeId, Array), Pivot(IdOnly, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-68 ExcludeId IncludeId" in {
            val stages = List(Pivot(ExcludeId, Array), Pivot(IncludeId, Object))
            input must interpretInto(stages)(ldjson("""["p", 10]"""))
          }
          "foc-69 ExcludeId ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Array), Pivot(ExcludeId, Object))
            input must interpretInto(stages)(ldjson("10"))
          }
          "foc-70 ExcludeId IdOnly" in {
            val stages = List(Pivot(ExcludeId, Array), Pivot(IdOnly, Object))
            input must interpretInto(stages)(ldjson(""""p""""))
          }
          "foc-71 IdOnly IncludeId" in {
            val stages = List(Pivot(IdOnly, Array), Pivot(IncludeId, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-72 IdOnly ExcludeId" in {
            val stages = List(Pivot(IdOnly, Array), Pivot(ExcludeId, Object))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-73 IdOnly IdOnly" in {
            val stages = List(Pivot(IdOnly, Array), Pivot(IdOnly, Object))
            input must interpretInto(stages)(ldjson(""))
          }
        }

        "Array Array" >> {
          "foc-74 IncludeId IncludeId" in {
            val stages = List(Pivot(IncludeId, Array), Pivot(IncludeId, Array))
            val expected = ldjson("""
              [0, 0]
              [1, 2]
              [0, 1]
              [1, "foo"]
              [0, 0]
              [1, 3]
              [0, 1]
              [1, [2.2]]
              [0, 0]
              [1, 4]
              [0, 1]
              [1, { "p": 10 }]
              [0, 0]
              [1, 5]
              [0, 1]
              [1, {}]
              """)
            input must interpretInto(stages)(expected)
          }
          "foc-75 IncludeId ExcludeId" in {
            val stages = List(Pivot(IncludeId, Array), Pivot(ExcludeId, Array))
            val expected = ldjson("""
              0
              2
              1
              "foo"
              0
              3
              1
              [2.2]
              0
              4
              1
              { "p": 10 }
              0
              5
              1
              {}
              """)
            input must interpretInto(stages)(expected)
          }
          "foc-76 IncludeId IdOnly" in {
            val stages = List(Pivot(IncludeId, Array), Pivot(IdOnly, Array))
            val expected = ldjson("""
              0
              1
              0
              1
              0
              1
              0
              1
              0
              1
              0
              1
              0
              1
              0
              1
              """)
            input must interpretInto(stages)(expected)
          }
          "foc-77 ExcludeId IncludeId" in {
            val stages = List(Pivot(ExcludeId, Array), Pivot(IncludeId, Array))
            input must interpretInto(stages)(ldjson("[0, 2.2]"))
          }
          "foc-78 ExcludeId ExcludeId" in {
            val stages = List(Pivot(ExcludeId, Array), Pivot(ExcludeId, Array))
            input must interpretInto(stages)(ldjson("2.2"))
          }
          "foc-79 ExcludeId IdOnly" in {
            val stages = List(Pivot(ExcludeId, Array), Pivot(IdOnly, Array))
            input must interpretInto(stages)(ldjson("0"))
          }
          "foc-80 IdOnly IncludeId" in {
            val stages = List(Pivot(IdOnly, Array), Pivot(IncludeId, Array))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-81 IdOnly ExcludeId" in {
            val stages = List(Pivot(IdOnly, Array), Pivot(ExcludeId, Array))
            input must interpretInto(stages)(ldjson(""))
          }
          "foc-82 IdOnly IdOnly" in {
            val stages = List(Pivot(IdOnly, Array), Pivot(IdOnly, Array))
            input must interpretInto(stages)(ldjson(""))
          }
        }
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, focusedPendingExamples, "foc")

    def evalFocused(stages: List[ScalarStage.Focused], stream: JsonStream): JsonStream

    def mask(masks: (String, Set[ColumnType])*): Mask =
      Mask(Map(masks.map({ case (k, v) => CPath.parse(k) -> v }): _*))

    def project(path: String): Project =
      Project(CPath.parse(path))

    def interpretInto(
        stages: List[ScalarStage.Focused])(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream =>
        evalFocused(stages, str)
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

        input must cartesianInto(targets)(expected)
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

        input must cartesianInto(targets)(expected)
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

        input must cartesianInto(targets)(expected)
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

        input must cartesianInto(targets)(expected)
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

        input must cartesianInto(targets)(expected)
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

        input must cartesianInto(targets)(expected)
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

        input must cartesianInto(targets)(expected)
      }

      // a[_] as a, b[_] as b
      "cart-10 pivoting retains row alignment through undefineds (pt II)" in {
        import ScalarStage.{Mask, Pivot}

        val input = ldjson("""
          { "a": [1], "b": [4, 5] }
          { "a": [2] }
          { "b": [6] }
          { "a": [3], "b": [7] }
          """)

        val expected = ldjson("""
          { "a": 1, "b": 4 }
          { "a": 1, "b": 5 }
          { "a": 2 }
          { "b": 6 }
          { "a": 3, "b": 7 }
          """)

        val targets = Map(
          (CPathField("a"), (CPathField("a"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
            Pivot(IdStatus.ExcludeId, ColumnType.Array)))),

          (CPathField("b"), (CPathField("b"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
            Pivot(IdStatus.ExcludeId, ColumnType.Array)))))

        input must cartesianInto(targets)(expected)
      }

      // a0 as a1, b0 as b1
      "cart-11 cross fields when some are undefined" in {
        val input = ldjson("""
            { "a0": 1 }
            { "a0": 2, "b0": "foo" }
            { "b0": "bar" }
            { "c": 12 }
            """)

        val expected = ldjson("""
            { "a1": 1 }
            { "a1": 2, "b1": "foo" }
            { "b1": "bar" }
            """)

        val targets = Map(
          (CPathField("a1"), (CPathField("a0"), Nil)),
          (CPathField("b1"), (CPathField("b0"), Nil)))

        input must cartesianInto(targets)(expected)
      }

      // minimization of `multilevelFlatten.test`
      // x[0] as x0, x[1] as x1
      "cart-12 cross fields when some are undefined after array projection" in {
        import ScalarStage.Project

        val input = ldjson("""
          { "x": ["foo"] }
          { "x": ["bar", 42] }
          """)

        val expected = ldjson("""
          { "x0": "foo" }
          { "x0": "bar", "x1": 42 }
          """)

        val targets = Map(
          (CPathField("x0"), (CPathField("x"), List(
            Project(CPath.parse("[0]"))))),
          (CPathField("x1"), (CPathField("x"), List(
            Project(CPath.parse("[1]"))))))

        input must cartesianInto(targets)(expected)
      }

      // x.a as xa, x.b as xb
      "cart-13 cross fields when some are undefined after object projection" in {
        import ScalarStage.Project

        val input = ldjson("""
          { "x": { "a": "foo" } }
          { "x": { "a": "bar", "b": 42 } }
          """)

        val expected = ldjson("""
          { "xa": "foo" }
          { "xa": "bar", "xb": 42 }
          """)

        val targets = Map(
          (CPathField("xa"), (CPathField("x"), List(
            Project(CPath.parse(".a"))))),
          (CPathField("xb"), (CPathField("x"), List(
            Project(CPath.parse(".b"))))))

        input must cartesianInto(targets)(expected)
      }

      // minimization of `flattenArrayValueAndIndexWithField.test`
      // a as x0, b[_] as x1
      "cart-14 cross fields when some are undefined after array pivot" in {
        import ScalarStage.Pivot

         val input = ldjson("""
          { "a": 1, "b": [true, true, true] }
          { "a": 2, "b": [false, false] }
          { "a": 3, "b": 42 }
          { "a": 4 }
          """)

         val expected = ldjson("""
          { "x0": 1, "x1": [0, true] }
          { "x0": 1, "x1": [1, true] }
          { "x0": 1, "x1": [2, true] }
          { "x0": 2, "x1": [0, false] }
          { "x0": 2, "x1": [1, false] }
          { "x0": 3 }
          { "x0": 4 }
          """)

          val targets = Map(
            (CPathField("x0"), (CPathField("a"), Nil)),
            (CPathField("x1"), (CPathField("b"), List(
              Pivot(IdStatus.IncludeId, ColumnType.Array)))))

        input must cartesianInto(targets)(expected)
      }

      // a as x0, b{_} as x1
      "cart-15 cross fields when some are undefined after object pivot" in {
        import ScalarStage.Pivot

         val input = ldjson("""
          { "a": 1, "b": {"x":true, "y":true, "z":true} }
          { "a": 2, "b": {"x":false, "y":false} }
          { "a": 3, "b": 42 }
          { "a": 4 }
          """)

         val expected = ldjson("""
          { "x0": 1, "x1": ["x", true] }
          { "x0": 1, "x1": ["y", true] }
          { "x0": 1, "x1": ["z", true] }
          { "x0": 2, "x1": ["x", false] }
          { "x0": 2, "x1": ["y", false] }
          { "x0": 3 }
          { "x0": 4 }
          """)

          val targets = Map(
            (CPathField("x0"), (CPathField("a"), Nil)),
            (CPathField("x1"), (CPathField("b"), List(
              Pivot(IdStatus.IncludeId, ColumnType.Object)))))

        input must cartesianInto(targets)(expected)
      }

      // Nested pivots in a cartesian can emit multiple rows where the
      // pivoted value is undefined. When these undefined rows are crossed
      // in the cartesian, they must be preserved.
      "nested pivoting in a cartouche preserves undefineds" >> {

        "Pivot . Pivot" >> {

          "Array Array " >> {
            import ScalarStage.Pivot

            val input = ldjson("""
              { "a": 1, "b": [["one", "two"], "three", ["four"]] }
              { "a": 2, "b": [{ "x": "five", "y": "six" }, { "z": "seven" }] }
              { "a": 3, "b": "eight" }
              { "a": 4, "c": "nine" }
              """)

            "cart-24 ExcludeId" in {
              val expected = ldjson("""
                { "a0": 1, "b0": "one" }
                { "a0": 1, "b0": "two" }
                { "a0": 1 }
                { "a0": 1, "b0": "four" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-25 IdOnly" in {
              val expected = ldjson("""
                { "a0": 1, "b0": 0 }
                { "a0": 1, "b0": 1 }
                { "a0": 1 }
                { "a0": 1, "b0": 0 }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Pivot(IdStatus.IdOnly, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-26 IncludeId" in {
              val expected = ldjson("""
                { "a0": 1, "b0": [0, "one"] }
                { "a0": 1, "b0": [1, "two"] }
                { "a0": 1 }
                { "a0": 1, "b0": [0, "four"] }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Pivot(IdStatus.IncludeId, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }
          }

          "Object Object" >> {
            import ScalarStage.Pivot

            val input = ldjson("""
              { "a": 1, "b": { "x": { "q": "one", "r": "two"}, "y": "three", "z": { "s": "four" } } }
              { "a": 2, "b": { "a": ["five", "six"], "b": "seven" } }
              { "a": 3, "b": "eight" }
              { "a": 4, "c": "nine" }
              """)

            "cart-27 ExcludeId" in {
              val expected = ldjson("""
                { "a0": 1, "b0": "one" }
                { "a0": 1, "b0": "two" }
                { "a0": 1 }
                { "a0": 1, "b0": "four" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-28 IdOnly" in {
              val expected = ldjson("""
                { "a0": 1, "b0": "q" }
                { "a0": 1, "b0": "r" }
                { "a0": 1 }
                { "a0": 1, "b0": "s" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Pivot(IdStatus.IdOnly, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-29 IncludeId" in {
              val expected = ldjson("""
                { "a0": 1, "b0": ["q", "one"] }
                { "a0": 1, "b0": ["r", "two"] }
                { "a0": 1 }
                { "a0": 1, "b0": ["s", "four"] }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Pivot(IdStatus.IncludeId, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }
          }
        }

        "Mask . Pivot . Mask . Pivot" >> {

          "Array Array" >> {
            import ScalarStage.{Mask, Pivot}

            val input = ldjson("""
              { "a": 1, "b": [["one", "two"], "three", ["four"]] }
              { "a": 2, "b": [{ "x": "five", "y": "six" }, { "z": "seven" }] }
              { "a": 3, "b": "eight" }
              { "a": 4, "c": "nine" }
              """)

            "cart-30 ExcludeId" in {
              val expected = ldjson("""
                { "a0": 1, "b0": "one" }
                { "a0": 1, "b0": "two" }
                { "a0": 1 }
                { "a0": 1, "b0": "four" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-31 IdOnly" in {
              val expected = ldjson("""
                { "a0": 1, "b0": 0 }
                { "a0": 1, "b0": 1 }
                { "a0": 1 }
                { "a0": 1, "b0": 0 }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
                  Pivot(IdStatus.IdOnly, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-32 IncludeId" in {
              val expected = ldjson("""
                { "a0": 1, "b0": [0, "one"] }
                { "a0": 1, "b0": [1, "two"] }
                { "a0": 1 }
                { "a0": 1, "b0": [0, "four"] }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
                  Pivot(IdStatus.IncludeId, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }
          }

          "Object Object" >> {
            import ScalarStage.{Mask, Pivot}

            val input = ldjson("""
              { "a": 1, "b": { "x": { "q": "one", "r": "two"}, "y": "three", "z": { "s": "four" } } }
              { "a": 2, "b": { "a": ["five", "six"], "b": "seven" } }
              { "a": 3, "b": "eight" }
              { "a": 4, "c": "nine" }
              """)

            "cart-33 ExcludeId" in {
              val expected = ldjson("""
                { "a0": 1, "b0": "one" }
                { "a0": 1, "b0": "two" }
                { "a0": 1 }
                { "a0": 1, "b0": "four" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-34 IdOnly" in {
              val expected = ldjson("""
                { "a0": 1, "b0": "q" }
                { "a0": 1, "b0": "r" }
                { "a0": 1 }
                { "a0": 1, "b0": "s" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
                  Pivot(IdStatus.IdOnly, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-35 IncludeId" in {
              val expected = ldjson("""
                { "a0": 1, "b0": ["q", "one"] }
                { "a0": 1, "b0": ["r", "two"] }
                { "a0": 1 }
                { "a0": 1, "b0": ["s", "four"] }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
                  Pivot(IdStatus.IncludeId, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }
          }
        }

        "Project . Pivot . Project . Pivot" >> {

          "Array Array" >> {
            import ScalarStage.{Pivot, Project}

            val input = ldjson("""
              { "a": 1, "b": { "x": ["one", "two"] } }
              { "a": 2, "b": { "x": [{ "q": ["three", "four"] }, { "p": "five" }, "six"] } }
              { "a": 3, "b": { "x": "seven" } }
              { "a": 4, "b": "eight" }
              { "a": 5, "c": "nine" }
              """)

            "cart-36 ExcludeId" in {
              val expected = ldjson("""
                { "a0": 1 }
                { "a0": 1 }
                { "a0": 2, "b0": "three" }
                { "a0": 2, "b0": "four" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                { "a0": 5 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Project(CPath.parse(".x")),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Project(CPath.parse(".q")),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-37 IdOnly" in {
              val expected = ldjson("""
                { "a0": 1 }
                { "a0": 1 }
                { "a0": 2, "b0": 0 }
                { "a0": 2, "b0": 1 }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                { "a0": 5 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Project(CPath.parse(".x")),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Project(CPath.parse(".q")),
                  Pivot(IdStatus.IdOnly, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-38 IncludeId" in {
              val expected = ldjson("""
                { "a0": 1 }
                { "a0": 1 }
                { "a0": 2, "b0": [0, "three"] }
                { "a0": 2, "b0": [1, "four"] }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                { "a0": 5 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Project(CPath.parse(".x")),
                  Pivot(IdStatus.ExcludeId, ColumnType.Array),
                  Project(CPath.parse(".q")),
                  Pivot(IdStatus.IncludeId, ColumnType.Array)))))

              input must cartesianInto(targets)(expected)
            }
          }

          "Object Object" >> {
            import ScalarStage.{Pivot, Project}

            val input = ldjson("""
              { "a": 1, "b": { "x": { "s": "one", "t": "two" } } }
              { "a": 2, "b": { "x": { "z": { "q": { "f": "three", "g": "four" } }, "y": { "p": "five" }, "r": "six" } } }
              { "a": 3, "b": { "x": "seven" } }
              { "a": 4, "b": "eight" }
              { "a": 5, "c": "nine" }
              """)

            "cart-39 ExcludeId" in {
              val expected = ldjson("""
                { "a0": 1 }
                { "a0": 1 }
                { "a0": 2, "b0": "three" }
                { "a0": 2, "b0": "four" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                { "a0": 5 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Project(CPath.parse(".x")),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Project(CPath.parse(".q")),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-40 IdOnly" in {
              val expected = ldjson("""
                { "a0": 1 }
                { "a0": 1 }
                { "a0": 2, "b0": "f" }
                { "a0": 2, "b0": "g" }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                { "a0": 5 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Project(CPath.parse(".x")),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Project(CPath.parse(".q")),
                  Pivot(IdStatus.IdOnly, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }

            "cart-41 IncludeId" in {
              val expected = ldjson("""
                { "a0": 1 }
                { "a0": 1 }
                { "a0": 2, "b0": ["f", "three"] }
                { "a0": 2, "b0": ["g", "four"] }
                { "a0": 2 }
                { "a0": 2 }
                { "a0": 3 }
                { "a0": 4 }
                { "a0": 5 }
                """)

              val targets = Map(
                (CPathField("a0"), (CPathField("a"), Nil)),

                (CPathField("b0"), (CPathField("b"), List(
                  Project(CPath.parse(".x")),
                  Pivot(IdStatus.ExcludeId, ColumnType.Object),
                  Project(CPath.parse(".q")),
                  Pivot(IdStatus.IncludeId, ColumnType.Object)))))

              input must cartesianInto(targets)(expected)
            }
          }
        }

        "Pivot . Wrap" >> {

          "cart-42 Array " in {
            import ScalarStage.{Pivot, Wrap}

            val input = ldjson("""
              { "a": 1, "b": [["one", "two"], "three", ["four"]] }
              { "a": 2, "b": [{ "x": "five", "y": "six" }, { "z": "seven" }] }
              { "a": 3, "b": "eight" }
              { "a": 4, "c": "nine" }
              """)

            val expected = ldjson("""
              { "a0": 1, "b0": { "q": ["one", "two"] } }
              { "a0": 1, "b0": { "q": "three" } }
              { "a0": 1, "b0": { "q": ["four"] } }
              { "a0": 2, "b0": { "q": { "x": "five", "y": "six" } } }
              { "a0": 2, "b0": { "q": { "z": "seven" } } }
              { "a0": 3 }
              { "a0": 4 }
              """)

            val targets = Map(
              (CPathField("a0"), (CPathField("a"), Nil)),

              (CPathField("b0"), (CPathField("b"), List(
                Pivot(IdStatus.ExcludeId, ColumnType.Array),
                Wrap("q")))))

            input must cartesianInto(targets)(expected)
          }

          "cart-43 Object " in {
            import ScalarStage.{Pivot, Wrap}

            val input = ldjson("""
              { "a": 1, "b": { "x": { "t": "one", "r": "two"}, "y": "three", "z": { "s": "four" } } }
              { "a": 2, "b": { "a": ["five", "six"], "b": "seven" } }
              { "a": 3, "b": "eight" }
              { "a": 4, "c": "nine" }
              """)

            val expected = ldjson("""
              { "a0": 1, "b0": { "q": { "t": "one", "r": "two"} } }
              { "a0": 1, "b0": { "q": "three" } }
              { "a0": 1, "b0": { "q": { "s": "four" } } }
              { "a0": 2, "b0": { "q": ["five", "six"] } }
              { "a0": 2, "b0": { "q": "seven" } }
              { "a0": 3 }
              { "a0": 4 }
              """)

            val targets = Map(
              (CPathField("a0"), (CPathField("a"), Nil)),

              (CPathField("b0"), (CPathField("b"), List(
                Pivot(IdStatus.ExcludeId, ColumnType.Object),
                Wrap("q")))))

            input must cartesianInto(targets)(expected)
          }
        }
      }

      // a0[_] as a, b0[_] as b
      "cart-44 cross with undefined values on both sides" >> {
        import ScalarStage.Pivot

        val input = ldjson("""
            { "a0": [1] }
            { "a0": [2], "b0": ["z"] }
            { "b0": ["y"] }
            { "a0": [3], "b0": "x" }
            { "a0": 4, "b0": ["w"] }
            { "a0": 5, "b0": "v" }
            """)

        val expected = ldjson("""
            { "a": 1 }
            { "a": 2, "b": "z" }
            { "b": "y" }
            { "a": 3 }
            { "b": "w" }
            """)

          val targets = Map(
            (CPathField("a"), (CPathField("a0"), List(
              Pivot(IdStatus.ExcludeId, ColumnType.Array)))),
            (CPathField("b"), (CPathField("b0"), List(
              Pivot(IdStatus.ExcludeId, ColumnType.Array)))))

        input must cartesianInto(targets)(expected)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, cartesianPendingExamples, "cart")

    def evalCartesian(cartesian: Cartesian, stream: JsonStream): JsonStream

    def cartesianInto(
        cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])])(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream =>
        evalCartesian(Cartesian(cartouches), str)
      }
  }

  trait FullSpec extends JsonSpec {

    import ScalarStage.{Cartesian, Pivot, Project}

    val fullPendingExamples: Set[Int]

    "scalar stages" should {

      "full-1 evaluate basic nested cartesians" in {

        val targets = List(
          Project(CPath.parse("a")),
          Pivot(IdStatus.ExcludeId, ColumnType.Object),
          Cartesian(Map(
            (CPathField("b0"), (CPathField("b"), List(Pivot(IdStatus.ExcludeId, ColumnType.Object)))),
            (CPathField("c0"), (CPathField("c"), Nil)))),
          Cartesian(Map(
            (CPathField("b1"), (CPathField("b0"), List(Pivot(IdStatus.ExcludeId, ColumnType.Array)))),
            (CPathField("c1"), (CPathField("c0"), Nil)))))

        val stages = ScalarStages(IdStatus.ExcludeId, targets)

        val input = ldjson("""
          {"a": {"x": {"b": {"k": [1, 2, 3], "j": 4}, "c": 5}, "y": {"b": {"k": [6, 7, 8], "j": 9}, "c": 10}}}
          """)

        val expected = ldjson("""
          {"b1": 1, "c1": 5}
          {"b1": 2, "c1": 5}
          {"b1": 3, "c1": 5}
          {"c1": 5}
          {"b1": 6, "c1": 10}
          {"b1": 7, "c1": 10}
          {"b1": 8, "c1": 10}
          {"c1": 10}
          """)

        input must interpretFullInto(stages)(expected)
      }
    }

    override def is: SpecStructure =
      pendingFragments(super.is, fullPendingExamples, "full")

    def evalFull(stages: ScalarStages, stream: JsonStream): JsonStream

    def interpretFullInto(
        stages: ScalarStages)(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream =>
        evalFull(stages, str)
      }
  }
}
