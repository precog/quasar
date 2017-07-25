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

import quasar.precog.common._
import quasar.blueeyes.json._

import org.specs2._

import _root_.scalaz._, Scalaz._

trait LeftShiftSpec[M[+_]] extends TableModuleTestSupport[M] with SpecificationLike {

  def testTrivialArrayLeftShift = {
    val rec = toRecord(Array(0), JArray(JNum(12) :: JNum(13) :: Nil))
    val table = fromSample(SampleData(Stream(rec)))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JNum(0), JNum(12))),
        toRecord(Array(0), JArray(JNum(1), JNum(13))))

    toJson(table.leftShift(CPath.Identity \ 1)).copoint mustEqual expected
  }

  def testTrivialObjectLeftShift = {
    val rec = toRecord(Array(0), JObject(JField("foo", JNum(12)), JField("bar", JNum(13))))
    val table = fromSample(SampleData(Stream(rec)))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JString("bar"), JNum(13))),
        toRecord(Array(0), JArray(JString("foo"), JNum(12))))

    toJson(table.leftShift(CPath.Identity \ 1)).copoint mustEqual expected
  }

  def testTrivialObjectArrayLeftShift = {
    val table =
      fromSample(
        SampleData(
          Stream(
            toRecord(Array(0), JObject(JField("foo", JNum(12)), JField("bar", JNum(13)))),
            toRecord(Array(1), JArray(JNum(42), JNum(43))))))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JString("bar"), JNum(13))),
        toRecord(Array(0), JArray(JString("foo"), JNum(12))),
        toRecord(Array(1), JArray(JNum(0), JNum(42))),
        toRecord(Array(1), JArray(JNum(1), JNum(43))))

    toJson(table.leftShift(CPath.Identity \ 1)).copoint.toVector mustEqual expected
  }

  def testSetArrayLeftShift = {
    def rec(i: Int) =
      toRecord(Array(i), JArray(JNum(i * 12) :: JNum(i * 13) :: Nil))

    val table = fromSample(SampleData(Stream.from(0).map(rec).take(100)))

    def expected(i: Int) =
      Vector(
        toRecord(Array(i), JArray(JNum(0), JNum(i * 12))),
        toRecord(Array(i), JArray(JNum(1), JNum(i * 13))))

    val expectedAll = (0 until 100).toVector.flatMap(expected)

    toJson(table.leftShift(CPath.Identity \ 1)).copoint mustEqual expectedAll
  }

  def testHeteroArrayLeftShift = {
    val table =
      fromSample(
        SampleData(
          Stream(
            toRecord(Array(0), JArray(JNum(12) :: JNum(13) :: Nil)),
            toRecord(Array(1), JArray(JNum(22) :: JNum(23) :: JNum(24) :: JNum(25) :: Nil)),
            toRecord(Array(2), JArray(Nil)),
            toRecord(Array(3), JString("psych!")))))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JNum(0), JNum(12))),
        toRecord(Array(0), JArray(JNum(1), JNum(13))),
        toRecord(Array(1), JArray(JNum(0), JNum(22))),
        toRecord(Array(1), JArray(JNum(1), JNum(23))),
        toRecord(Array(1), JArray(JNum(2), JNum(24))),
        toRecord(Array(1), JArray(JNum(3), JNum(25))))

    toJson(table.leftShift(CPath.Identity \ 1)).copoint mustEqual expected
  }

  def testTrivialArrayLeftShiftWithInnerObject = {
    val rec = toRecord(Array(0), JArray(JNum(12) :: JNum(13) :: JObject(JField("a", JNum(42))) :: Nil))
    val table = fromSample(SampleData(Stream(rec)))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JNum(0), JNum(12))),
        toRecord(Array(0), JArray(JNum(1), JNum(13))),
        toRecord(Array(0), JArray(JNum(2), JObject(JField("a", JNum(42))))))

    toJson(table.leftShift(CPath.Identity \ 1)).copoint mustEqual expected
  }

  // replaces SampleData.toRecord to avoid ordering issues
  def toRecord(indices: Array[Int], jv: JValue): JValue =
    JArray(JArray(indices.map(JNum(_)).toList) :: jv :: Nil)
}
