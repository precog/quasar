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

import quasar.blueeyes.json._
import scalaz.syntax.comonad._
import quasar.precog.TestSupport._

trait ToArraySpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with SpecificationLike {
  def testToArrayHomogeneous = {
    val data: Stream[JValue] =
      Stream(
        JObject(JField("value", JNum(23.4)) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12.4)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JNum(-12.4)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.toArray[Double])

    val expected = Stream(
      JArray(JNum(23.4) :: Nil),
      JArray(JNum(12.4) :: Nil),
      JArray(JNum(-12.4) :: Nil))

    results.copoint must_== expected
  }

  def testToArrayHeterogeneous = {
    val data: Stream[JValue] =
      Stream(
        JObject(JField("value", JObject(JField("foo", JNum(23.4)) :: JField("bar", JString("a")) :: Nil)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("foo", JNum(23.4)) :: JField("bar", JNum(18.8)) :: Nil)) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("bar", JNum(44.4)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.toArray[Double])

    val expected = Stream(JArray(JNum(18.8) :: JNum(23.4) :: Nil))

    results.copoint must_== expected
  }
}


