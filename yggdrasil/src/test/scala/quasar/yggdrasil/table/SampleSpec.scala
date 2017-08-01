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

import quasar.blueeyes._, json._
import quasar.precog.common._
import scalaz.syntax.comonad._
import quasar.precog.TestSupport._

trait SampleSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with SpecificationLike with ScalaCheck {
  import trans._

  val simpleData: Stream[JValue] = Stream.tabulate(100) { i =>
    JObject(JField("id", if (i % 2 == 0) JString(i.toString) else JNum(i)) :: Nil)
  }

  val simpleData2: Stream[JValue] = Stream.tabulate(100) { i =>
    JObject(
      JField("id", if (i % 2 == 0) JString(i.toString) else JNum(i)) ::
      JField("value", if (i % 2 == 0) JBool(true) else JNum(i)) ::
      Nil)
  }

  def testSample = {
    val data = SampleData(simpleData)
    val table = fromSample(data)
    table.sample(15, Seq(TransSpec1.Id, TransSpec1.Id)).copoint.toList must beLike {
      case s1 :: s2 :: Nil =>
        val result1 = toJson(s1).copoint
        val result2 = toJson(s2).copoint
        result1 must have size(15)
        result2 must have size(15)
        simpleData must containAllOf(result1)
        simpleData must containAllOf(result2)
    }
  }

  def testSampleEmpty = {
    val data = SampleData(simpleData)
    val table = fromSample(data)
    table.sample(15, Seq()).copoint.toList mustEqual Nil
  }

  def testSampleTransSpecs = {
    val data = SampleData(simpleData2)
    val table = fromSample(data)
    val specs = Seq(trans.DerefObjectStatic(TransSpec1.Id, CPathField("id")), trans.DerefObjectStatic(TransSpec1.Id, CPathField("value")))

    table.sample(15, specs).copoint.toList must beLike {
      case s1 :: s2 :: Nil =>
        val result1 = toJson(s1).copoint
        val result2 = toJson(s2).copoint
        result1 must have size(15)
        result2 must have size(15)

        val expected1 = toJson(table.transform(trans.DerefObjectStatic(TransSpec1.Id, CPathField("id")))).copoint
        val expected2 = toJson(table.transform(trans.DerefObjectStatic(TransSpec1.Id, CPathField("value")))).copoint
        expected1 must containAllOf(result1)
        expected2 must containAllOf(result2)
    }
  }

  def testLargeSampleSize = {
    val data = SampleData(simpleData)
    fromSample(data).sample(1000, Seq(TransSpec1.Id)).copoint.toList must beLike {
      case s :: Nil =>
        val result = toJson(s).copoint
        result must have size(100)
    }
  }

  def test0SampleSize = {
    val data = SampleData(simpleData)
    fromSample(data).sample(0, Seq(TransSpec1.Id)).copoint.toList must beLike {
      case s :: Nil =>
        val result = toJson(s).copoint
        result must have size(0)
    }
  }
}

