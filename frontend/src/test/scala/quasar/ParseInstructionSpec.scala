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

import quasar.common.CPath

import org.specs2.matcher.Matcher

import scala.collection.immutable.{Map, Set}

import java.lang.String

abstract class ParseInstructionSpec
    extends JsonSpec
    with ParseInstructionSpec.IdsSpec
    with ParseInstructionSpec.WrapSpec
    with ParseInstructionSpec.MaskSpec
    with ParseInstructionSpec.PivotSpec

object ParseInstructionSpec {

  /*
   * Please note that this is currently *over*-specified.
   * We don't technically need monotonic ids or even numerical
   * ones, we just need *unique* identities. That assertion is
   * quite hard to encode though. If we find we need such an
   * implementation in the future, these assertions should be
   * changed.
   */
  trait IdsSpec extends JsonSpec {
    protected final val Ids = ParseInstruction.Ids

    "ids" should {
      "wrap each scalar row in monotonic integers" in {
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

        input must assignIdsTo(expected)
      }

      "wrap each vector row in monotonic integers" in {
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

        input must assignIdsTo(expected)
      }
    }

    def evalIds(stream: JsonStream): JsonStream

    def assignIdsTo(expected: JsonStream) : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream => evalIds(str) }
  }

  trait WrapSpec extends JsonSpec {
    protected final type Wrap = ParseInstruction.Wrap
    protected final val Wrap = ParseInstruction.Wrap

    "wrap" should {
      "nest scalars at identity" in {
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

        input must wrapInto(".", "foo")(expected)
      }

      "nest vectors at identity" in {
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

        input must wrapInto(".", "bar")(expected)
      }

      "nest scalars at .a.b" in {
        val input = ldjson("""
          { "a": { "b": 1 } }
          { "a": { "b": "hi" } }
          { "a": { "b": true } }
          """)

        val expected = ldjson("""
          { "a": { "b": { "foo": 1 } } }
          { "a": { "b": { "foo": "hi" } } }
          { "a": { "b": { "foo": true } } }
          """)

        input must wrapInto(".a.b", "foo")(expected)
      }

      "nest scalars at .a.b keeping rows excluding the target" in {
        val input = ldjson("""
          { "a": { "b": 1 } }
          { "a": { "c": 2 } }
          { "a": { "b": "hi" } }
          { "a": { "d": "bye" } }
          {}
          []
          42
          """)

        val expected = ldjson("""
          { "a": { "b": { "foo": 1 } } }
          { "a": { "c": 2 } }
          { "a": { "b": { "foo": "hi" } } }
          { "a": { "d": "bye" } }
          {}
          []
          42
          """)

        input must wrapInto(".a.b", "foo")(expected)
      }

      "nest scalars at .a[1] with surrounding structure" in {
        val input = ldjson("""
          { "a": [null, 1, "nada"] }
          { "a": [[], "hi"] }
          { "a": [null, true, "nada"] }
          """)

        val expected = ldjson("""
          { "a": [null, { "bin": 1 }, "nada"] }
          { "a": [[], { "bin": "hi" }] }
          { "a": [null, { "bin": true }, "nada"] }
          """)

        input must wrapInto(".a[1]", "bin")(expected)
      }

      "nest scalars at .a[2] with surrounding structure keeping rows excluding the target" in {
        val input = ldjson("""
          { "a": [null, 1, "nada"] }
          { "a": [[], "hi"] }
          { "a": [null, true, "nada", "nix", "nil"] }
          { "a": [] }
          {}
          []
          42
          """)

        val expected = ldjson("""
          { "a": [null, 1, { "bin": "nada" }] }
          { "a": [[], "hi"] }
          { "a": [null, true, { "bin": "nada" }, "nix", "nil"] }
          { "a": [] }
          {}
          []
          42
          """)

        input must wrapInto(".a[2]", "bin")(expected)
      }

      "nest vectors at .a.b" in {
        val input = ldjson("""
          { "a": { "b": [1, 2, 3] } }
          { "a": { "b": { "a": "hi", "b": { "c": null } } } }
          { "a": { "b": [{ "d": {} }] } }
          """)

        val expected = ldjson("""
          { "a": { "b": { "bar": [1, 2, 3] } } }
          { "a": { "b": { "bar": { "a": "hi", "b": { "c": null } } } } }
          { "a": { "b": { "bar": [{ "d": {} }] } } }
          """)

        input must wrapInto(".a.b", "bar")(expected)
      }

      "retain surrounding structure with scalars at .a.b" in {
        val input = ldjson("""
          { "z": null, "a": { "b": 1 }, "d": false, "b": [] }
          { "z": [], "a": { "b": "hi" }, "d": "to be or not to be", "b": { "a": 321} }
          { "z": { "a": { "b": 3.14 } }, "a": { "b": true }, "d": {} }
          """)

        val expected = ldjson("""
          { "z": null, "a": { "b": { "qux": 1 } }, "d": false, "b": [] }
          { "z": [], "a": { "b": { "qux": "hi" } }, "d": "to be or not to be", "b": { "a": 321} }
          { "z": { "a": { "b": 3.14 } }, "a": { "b": { "qux": true } }, "d": {} }
          """)

        input must wrapInto(".a.b", "qux")(expected)
      }

      "retain surrounding structure with scalars at .a.b keeping rows excluding the target" in {
        val input = ldjson("""
          { "z": null, "a": { "b": 1 }, "d": false, "b": [] }
          { "z": [], "a": { "b": "hi" }, "d": "to be or not to be", "b": { "a": 321} }
          { "z": { "a": { "b": 3.14 } }, "a": { "c": null }, "d": {} }
          { "z": { "a": { "b": 3.14 } }, "a": { "b": true }, "d": {} }
          {}
          []
          42
          """)

        val expected = ldjson("""
          { "z": null, "a": { "b": { "qux": 1 } }, "d": false, "b": [] }
          { "z": [], "a": { "b": { "qux": "hi" } }, "d": "to be or not to be", "b": { "a": 321} }
          { "z": { "a": { "b": 3.14 } }, "a": { "c": null }, "d": {} }
          { "z": { "a": { "b": 3.14 } }, "a": { "b": { "qux": true } }, "d": {} }
          {}
          []
          42
          """)

        input must wrapInto(".a.b", "qux")(expected)
      }
    }

    def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream

    def wrapInto(
        path: String,
        name: String)(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream => evalWrap(Wrap(CPath.parse(path), name), str)}
  }

  trait MaskSpec extends JsonSpec {
    import ParseType._

    protected final type Mask = ParseInstruction.Mask
    protected final val Mask = ParseInstruction.Mask

    "masks" should {
      "drop everything when empty" in {
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

      "retain two scalar types at identity" in {
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

      "retain different sorts of numbers at identity" in {
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

      "retain different sorts of objects at identity" in {
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

      "retain different sorts of arrays at identity" in {
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

      "retain two scalar types at .a.b" in {
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

      "retain different sorts of numbers at .a.b" in {
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

      "retain different sorts of objects at .a.b" in {
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

      "retain different sorts of arrays at .a.b" in {
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

      "discard unmasked structure" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "c": true } }
          """)

        input must maskInto(".a.c" -> Set(Boolean))(expected)
      }

      "compose disjunctively across paths" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "c": true }, "c": [] }
          """)

        input must maskInto(".a.c" -> Set(Boolean), ".c" -> Set(Array))(expected)
      }

      "compose disjunctively across suffix-overlapped paths" in {
        val input = ldjson("""
          { "a": { "x": 42, "b": { "c": true } }, "b": { "c": [] }, "c": [1, 2] }
          """)

        val expected = ldjson("""
          { "a": { "b": { "c": true } }, "b": { "c": [] } }
          """)

        input must maskInto(".a.b.c" -> Set(Boolean), ".b.c" -> Set(Array))(expected)
      }

      "compose disjunctively across paths where one side is false" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true } }
          """)

        val expected = ldjson("""
          { "a": { "c": true } }
          """)

        input must maskInto(".a.c" -> Set(Boolean), ".a" -> Set(Array))(expected)
      }

      "subsume inner by outer" in {
        val input = ldjson("""
          { "a": { "b": 42, "c": true }, "c": [] }
          """)

        val expected = ldjson("""
          { "a": { "b": 42, "c": true } }
          """)

        input must maskInto(".a.b" -> Set(Boolean), ".a" -> Set(Object))(expected)
      }

      "disallow the wrong sort of vector" in {
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

      "compact surrounding array" in {
        ldjson("[1, 2, 3]") must maskInto("[1]" -> Set(Number))(ldjson("[2]"))
      }

      "compact surrounding array with multiple values retained" in {
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

      "compact surrounding nested array with multiple values retained" in {
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

      "compact array containing nested arrays with single nested value retained" in {
        val input = ldjson("""
          { "a": [[[1, 3, 5], "k"], "foo", { "b": [5, 6, 7], "c": [] }], "d": "x" }
          """)

        val expected = ldjson("""
          { "a": [{"b": [5, 6, 7] }] }
          """)

        input must maskInto(".a[2].b" -> Set(Array))(expected)
      }

      "remove object entirely when no values are retained" in {
        ldjson("""{ "a": 42 }""") must maskInto(".a" -> Set(Boolean))(ldjson(""))
      }

      "remove array entirely when no values are retained" in {
        ldjson("[42]") must maskInto("[0]" -> Set(Boolean))(ldjson(""))
      }
    }

    def evalMask(mask: Mask, stream: JsonStream): JsonStream

    def maskInto(
        masks: (String, Set[ParseType])*)(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream =>
        evalMask(Mask(Map(masks.map({ case (k, v) => CPath.parse(k) -> v }): _*)), str)
      }
  }

  trait PivotSpec extends JsonSpec {
    protected final type Pivot = ParseInstruction.Pivot
    protected final val Pivot = ParseInstruction.Pivot

    "pivot" should {
      "shift an array at identity" >> {
        val input = ldjson("""
          [1, 2, 3]
          [4, 5, 6]
          [7, 8, 9, 10]
          [11]
          []
          [12, 13]
          """)

        "ExcludeId" >> {
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

          input must pivotInto(".", IdStatus.ExcludeId, ParseType.Array)(expected)
        }

        "IdOnly" >> {
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

          input must pivotInto(".", IdStatus.IdOnly, ParseType.Array)(expected)
        }

        "IncludeId" >> {
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

          input must pivotInto(".", IdStatus.IncludeId, ParseType.Array)(expected)
        }
      }

      "shift an array at .a.b (with no surrounding structure)" >> {
        val input = ldjson("""
          { "a": { "b": [1, 2, 3] } }
          { "a": { "b": [4, 5, 6] } }
          { "a": { "b": [7, 8, 9, 10] } }
          { "a": { "b": [11] } }
          { "a": { "b": [] } }
          { "a": { "b": [12, 13] } }
          """)

        "ExcludeId" >> {
          val expected = ldjson("""
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 3 } }
            { "a": { "b": 4 } }
            { "a": { "b": 5 } }
            { "a": { "b": 6 } }
            { "a": { "b": 7 } }
            { "a": { "b": 8 } }
            { "a": { "b": 9 } }
            { "a": { "b": 10 } }
            { "a": { "b": 11 } }
            { "a": { "b": 12 } }
            { "a": { "b": 13 } }
            """)

          input must pivotInto(".a.b", IdStatus.ExcludeId, ParseType.Array)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson("""
            { "a": { "b": 0 } }
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 0 } }
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 0 } }
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 3 } }
            { "a": { "b": 0 } }
            { "a": { "b": 0 } }
            { "a": { "b": 1 } }
            """)

          input must pivotInto(".a.b", IdStatus.IdOnly, ParseType.Array)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson("""
            { "a": { "b": [0, 1] } }
            { "a": { "b": [1, 2] } }
            { "a": { "b": [2, 3] } }
            { "a": { "b": [0, 4] } }
            { "a": { "b": [1, 5] } }
            { "a": { "b": [2, 6] } }
            { "a": { "b": [0, 7] } }
            { "a": { "b": [1, 8] } }
            { "a": { "b": [2, 9] } }
            { "a": { "b": [3, 10] } }
            { "a": { "b": [0, 11] } }
            { "a": { "b": [0, 12] } }
            { "a": { "b": [1, 13] } }
            """)

          input must pivotInto(".a.b", IdStatus.IncludeId, ParseType.Array)(expected)
        }
      }

     "shift an array at .a[0] (with no surrounding structure)" >> {
        val input = ldjson("""
          { "a": [ [1, 2, 3] ] }
          { "a": [ [4, 5, 6] ] }
          { "a": [ [7, 8, 9, 10] ] }
          { "a": [ [11] ] }
          { "a": [ [] ] }
          { "a": [ [12, 13] ] }
          """)

        "ExcludeId" >> {
          val expected = ldjson("""
            { "a": [ 1 ] }
            { "a": [ 2 ] }
            { "a": [ 3 ] }
            { "a": [ 4 ] }
            { "a": [ 5 ] }
            { "a": [ 6 ] }
            { "a": [ 7 ] }
            { "a": [ 8 ] }
            { "a": [ 9 ] }
            { "a": [ 10 ] }
            { "a": [ 11 ] }
            { "a": [ 12 ] }
            { "a": [ 13 ] }
            """)

          input must pivotInto(".a[0]", IdStatus.ExcludeId, ParseType.Array)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson("""
            { "a": [ 0 ] }
            { "a": [ 1 ] }
            { "a": [ 2 ] }
            { "a": [ 0 ] }
            { "a": [ 1 ] }
            { "a": [ 2 ] }
            { "a": [ 0 ] }
            { "a": [ 1 ] }
            { "a": [ 2 ] }
            { "a": [ 3 ] }
            { "a": [ 0 ] }
            { "a": [ 0 ] }
            { "a": [ 1 ] }
            """)

          input must pivotInto(".a[0]", IdStatus.IdOnly, ParseType.Array)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson("""
            { "a": [ [0, 1] ] }
            { "a": [ [1, 2] ] }
            { "a": [ [2, 3] ] }
            { "a": [ [0, 4] ] }
            { "a": [ [1, 5] ] }
            { "a": [ [2, 6] ] }
            { "a": [ [0, 7] ] }
            { "a": [ [1, 8] ] }
            { "a": [ [2, 9] ] }
            { "a": [ [3, 10] ] }
            { "a": [ [0, 11] ] }
            { "a": [ [0, 12] ] }
            { "a": [ [1, 13] ] }
            """)

          input must pivotInto(".a[0]", IdStatus.IncludeId, ParseType.Array)(expected)
        }
      }


      "shift an object at identity" >> {
        val input = ldjson("""
          { "a": 1, "b": 2, "c": 3 }
          { "d": 4, "e": 5, "f": 6 }
          { "g": 7, "h": 8, "i": 9, "j": 10 }
          { "k": 11 }
          {}
          { "l": 12, "m": 13 }
          """)

        "ExcludeId" >> {
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

          input must pivotInto(".", IdStatus.ExcludeId, ParseType.Object)(expected)
        }

        "IdOnly" >> {
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

          input must pivotInto(".", IdStatus.IdOnly, ParseType.Object)(expected)
        }

        "IncludeId" >> {
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

          input must pivotInto(".", IdStatus.IncludeId, ParseType.Object)(expected)
        }
      }

      "shift an object at .a.b (no surrounding structure)" >> {
        val input = ldjson("""
          { "a": { "b": { "a": 1, "b": 2, "c": 3 } } }
          { "a": { "b": { "d": 4, "e": 5, "f": 6 } } }
          { "a": { "b": { "g": 7, "h": 8, "i": 9, "j": 10 } } }
          { "a": { "b": { "k": 11 } } }
          { "a": { "b": {} } }
          { "a": { "b": { "l": 12, "m": 13 } } }
          """)

        "ExcludeId" >> {
          val expected = ldjson("""
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 3 } }
            { "a": { "b": 4 } }
            { "a": { "b": 5 } }
            { "a": { "b": 6 } }
            { "a": { "b": 7 } }
            { "a": { "b": 8 } }
            { "a": { "b": 9 } }
            { "a": { "b": 10 } }
            { "a": { "b": 11 } }
            { "a": { "b": 12 } }
            { "a": { "b": 13 } }
            """)

          input must pivotInto(".a.b", IdStatus.ExcludeId, ParseType.Object)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson("""
            { "a": { "b": "a" } }
            { "a": { "b": "b" } }
            { "a": { "b": "c" } }
            { "a": { "b": "d" } }
            { "a": { "b": "e" } }
            { "a": { "b": "f" } }
            { "a": { "b": "g" } }
            { "a": { "b": "h" } }
            { "a": { "b": "i" } }
            { "a": { "b": "j" } }
            { "a": { "b": "k" } }
            { "a": { "b": "l" } }
            { "a": { "b": "m" } }
            """)

          input must pivotInto(".a.b", IdStatus.IdOnly, ParseType.Object)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson("""
            { "a": { "b": ["a", 1] } }
            { "a": { "b": ["b", 2] } }
            { "a": { "b": ["c", 3] } }
            { "a": { "b": ["d", 4] } }
            { "a": { "b": ["e", 5] } }
            { "a": { "b": ["f", 6] } }
            { "a": { "b": ["g", 7] } }
            { "a": { "b": ["h", 8] } }
            { "a": { "b": ["i", 9] } }
            { "a": { "b": ["j", 10] } }
            { "a": { "b": ["k", 11] } }
            { "a": { "b": ["l", 12] } }
            { "a": { "b": ["m", 13] } }
            """)

          input must pivotInto(".a.b", IdStatus.IncludeId, ParseType.Object)(expected)
        }
      }

      "shift an object at .a[0] (no surrounding structure)" >> {
        val input = ldjson("""
          { "a": [ { "a": 1, "b": 2, "c": 3 } ] }
          { "a": [ { "d": 4, "e": 5, "f": 6 } ] }
          { "a": [ { "g": 7, "h": 8, "i": 9, "j": 10 } ] }
          { "a": [ { "k": 11 } ] }
          { "a": [ {} ] }
          { "a": [ { "l": 12, "m": 13 } ] }
          """)

        "ExcludeId" >> {
          val expected = ldjson("""
            { "a": [ 1 ] }
            { "a": [ 2 ] }
            { "a": [ 3 ] }
            { "a": [ 4 ] }
            { "a": [ 5 ] }
            { "a": [ 6 ] }
            { "a": [ 7 ] }
            { "a": [ 8 ] }
            { "a": [ 9 ] }
            { "a": [ 10 ] }
            { "a": [ 11 ] }
            { "a": [ 12 ] }
            { "a": [ 13 ] }
            """)

          input must pivotInto(".a[0]", IdStatus.ExcludeId, ParseType.Object)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson("""
            { "a": [ "a" ] }
            { "a": [ "b" ] }
            { "a": [ "c" ] }
            { "a": [ "d" ] }
            { "a": [ "e" ] }
            { "a": [ "f" ] }
            { "a": [ "g" ] }
            { "a": [ "h" ] }
            { "a": [ "i" ] }
            { "a": [ "j" ] }
            { "a": [ "k" ] }
            { "a": [ "l" ] }
            { "a": [ "m" ] }
            """)

          input must pivotInto(".a[0]", IdStatus.IdOnly, ParseType.Object)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson("""
            { "a": [ ["a", 1] ] }
            { "a": [ ["b", 2] ] }
            { "a": [ ["c", 3] ] }
            { "a": [ ["d", 4] ] }
            { "a": [ ["e", 5] ] }
            { "a": [ ["f", 6] ] }
            { "a": [ ["g", 7] ] }
            { "a": [ ["h", 8] ] }
            { "a": [ ["i", 9] ] }
            { "a": [ ["j", 10] ] }
            { "a": [ ["k", 11] ] }
            { "a": [ ["l", 12] ] }
            { "a": [ ["m", 13] ] }
            """)

          input must pivotInto(".a[0]", IdStatus.IncludeId, ParseType.Object)(expected)
        }
      }

      "shift an object under .shifted with IncludeId and only one field" in {
        val input = ldjson("""
          { "shifted": { "shifted1": { "foo": "bar" } } }
          """)

        val expected = ldjson("""
          { "shifted": ["shifted1", { "foo": "bar" }] }
          """)

        input must pivotInto(".shifted", IdStatus.IncludeId, ParseType.Object)(expected)
      }
    }

    def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream

    def pivotInto(
        path: String,
        idStatus: IdStatus,
        structure: CompositeParseType)(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream =>
        evalPivot(Pivot(Map((CPath.parse(path), (idStatus, structure)))), str)
      }
  }
}
