/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.yggdrasil
package table

import quasar.yggdrasil.bytecode._
import quasar.blueeyes._, json._
import scalaz.syntax.comonad._
import quasar.precog.TestSupport._

trait SchemasSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with SpecificationLike with ScalaCheck {
  def testSingleSchema = {
    val expected = Set(JObjectFixedT(Map("a" -> JNumberT, "b" -> JTextT, "c" -> JNullT)))
    val trivialData = Stream.fill(100)(JParser.parseUnsafe("""{ "a": 1, "b": "x", "c": null }"""))
    val sample = SampleData(trivialData)
    val table = fromSample(sample, Some(10))
    table.schemas.copoint must_== expected
  }

  def testHomogeneousArraySchema = {
    val expected = Set(JArrayHomogeneousT(JNumberT))
    val data = Stream.fill(10)(JParser.parseUnsafe("""[1, 2, 3]"""))
    val table0 = fromSample(SampleData(data), Some(10))
    val table = table0.toArray[Long]
    table.schemas.copoint must_== expected
  }

  def testCrossSliceSchema = {
    val expected = Set(
      JObjectFixedT(Map("a" -> JNumberT, "b" -> JTextT)),
      JObjectFixedT(Map("a" -> JTextT, "b" -> JNumberT))
    )
    val data = Stream.fill(10)(JParser.parseUnsafe("""{ "a": 1, "b": "2" }""")) ++
      Stream.fill(10)(JParser.parseUnsafe("""{ "a": "x", "b": 2 }"""))
    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  def testIntervleavedSchema = {
    val expected = Set(
      JObjectFixedT(Map("a" -> JArrayFixedT(Map.empty), "b" -> JTextT)),
      JObjectFixedT(Map("a" -> JNullT, "b" -> JTextT)),
      JObjectFixedT(Map("a" -> JArrayFixedT(Map(0 -> JNumberT, 1 -> JNumberT)), "b" -> JArrayFixedT(Map(0 -> JTextT, 1 -> JObjectFixedT(Map.empty)))))
    )
    val data = Stream.tabulate(30) {
      case i if i % 3 == 0 => JParser.parseUnsafe("""{ "a": [], "b": "2" }""")
      case i if i % 3 == 1 => JParser.parseUnsafe("""{ "a": null, "b": "2" }""")
      case _ => JParser.parseUnsafe("""{ "a": [ 1, 2 ], "b": [ "2", {} ] }""")
    }
    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  def testUndefinedsInSchema = {
    val expected = Set(
      JObjectFixedT(Map("a" -> JNumberT, "b" -> JNumberT)),
      JObjectFixedT(Map("a" -> JNumberT)),
      JObjectFixedT(Map("b" -> JNumberT)),
      JObjectFixedT(Map.empty)
    )

    val data = Stream.tabulate(100) {
      case i if i % 4 == 0 => JObject(List(JField("a", JNum(1)), JField("b", JNum(i))))
      case i if i % 4 == 1 => JObject(List(JField("a", JNum(1)), JField("b", JUndefined)))
      case i if i % 4 == 2 => JObject(List(JField("a", JUndefined), JField("b", JNum(i))))
      case _               => JObject()
    }

    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  def testAllTypesInSchema = {
    val expected = Set(
      JNumberT,
      JTextT,
      JBooleanT,
      JNullT,
      JArrayFixedT(Map(0 -> JNumberT, 1 -> JNumberT)),
      JObjectFixedT(Map("a" -> JNumberT)),
      JObjectFixedT(Map("a" -> JBooleanT)),
      JObjectFixedT(Map("a" -> JTextT)),
      JObjectFixedT(Map("a" -> JNullT)),
      JObjectFixedT(Map("a" -> JArrayFixedT(Map.empty))),
      JObjectFixedT(Map("a" -> JObjectFixedT(Map.empty))),
      JObjectFixedT(Map("a" -> JArrayFixedT(Map(0 -> JNumberT, 1 -> JTextT, 2 -> JBooleanT)))),
      JObjectFixedT(Map("a" -> JObjectFixedT(Map("b" -> JObjectFixedT(Map("c" -> JNumberT))))))
    )
    val data = Stream(
      "1", "true", "null",
      """ "abc" """,
      """[ 1, 2 ]""",
      """{ "a": 1 }""",
      """{ "a": true }""",
      """{ "a": null }""",
      """{ "a": "a" }""",
      """{ "a": 1.2 }""",
      """{ "a": 112311912931223e-1000 }""",
      """{ "a": [] }""",
      """{ "a": {} }""",
      """{ "a": [ 1, "a", true ] }""",
      """{ "a": { "b": { "c": 3 } } }"""
    ) map (JParser.parseUnsafe(_))

    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }
}
