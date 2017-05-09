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
import scalaz.syntax.comonad._
import quasar.precog.TestSupport._

trait DistinctSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with SpecificationLike with ScalaCheck {
  import SampleData._
  import trans._

  def testDistinctIdentity = {
    implicit val gen = sort(distinct(sample(schema)))
    prop { (sample: SampleData) =>
      val table = fromSample(sample)

      val distinctTable = table.distinct(Leaf(Source))

      val result = toJson(distinctTable)

      result.copoint must_== sample.data
    }
  }

  def testDistinctAcrossSlices = {
    val array: JValue = JParser.parseUnsafe("""
      [{
        "value":{

        },
        "key":[1.0,1.0]
      },
      {
        "value":{

        },
        "key":[1.0,1.0]
      },
      {
        "value":{

        },
        "key":[2.0,1.0]
      },
      {
        "value":{

        },
        "key":[2.0,2.0]
      },
      {
        "value":{
          "fzz":false,
          "em":[{
            "l":210574764564691785.5,
            "fbk":-1.0
          },[[],""]],
          "z3y":[{
            "wd":null,
            "tv":false,
            "o":[]
          },{
            "sry":{

            },
            "in0":[]
          }]
        },
        "key":[1.0,2.0]
      },
      {
        "value":{
          "fzz":false,
          "em":[{
            "l":210574764564691785.5,
            "fbk":-1.0
          },[[],""]],
          "z3y":[{
            "wd":null,
            "tv":false,
            "o":[]
          },{
            "sry":{

            },
            "in0":[]
          }]
        },
        "key":[1.0,2.0]
      }]""")

    val data: Stream[JValue] = (array match {
      case JArray(li) => li
      case _ => sys.error("Expected a JArray")
    }).toStream

    val sample = SampleData(data)
    val table = fromSample(sample, Some(5))

    val result = toJson(table.distinct(Leaf(Source)))

    result.copoint must_== sample.data.toSeq.distinct
  }

  def testDistinctAcrossSlices2 = {
    val array: JValue = JParser.parseUnsafe("""
      [{
        "value":{
          "elxk7vv":-8.988465674311579E+307
        },
        "key":[1.0,1.0]
      },
      {
        "value":{
          "elxk7vv":-8.988465674311579E+307
        },
        "key":[1.0,1.0]
      },
      {
        "value":{
          "elxk7vv":-6.465000919622952E+307
        },
        "key":[2.0,4.0]
      },
      {
        "value":{
          "elxk7vv":-2.2425006462798597E+307
        },
        "key":[4.0,3.0]
      },
      {
        "value":{
          "elxk7vv":-1.0
        },
        "key":[5.0,8.0]
      },
      {
        "value":{
          "elxk7vv":-1.0
        },
        "key":[5.0,8.0]
      },
      {
        "value":[[]],
        "key":[3.0,1.0]
      },
      {
        "value":[[]],
        "key":[3.0,8.0]
      },
      {
        "value":[[]],
        "key":[6.0,7.0]
      },
      {
        "value":[[]],
        "key":[7.0,2.0]
      },
      {
        "value":[[]],
        "key":[8.0,1.0]
      },
      {
        "value":[[]],
        "key":[8.0,1.0]
      },
      {
        "value":[[]],
        "key":[8.0,4.0]
      }]""")

    val data: Stream[JValue] = (array match {
      case JArray(li) => li
      case _ => sys.error("Expected JArray")
    }).toStream

    val sample = SampleData(data)
    val table = fromSample(sample, Some(5))

    val result = toJson(table.distinct(Leaf(Source)))

    result.copoint must_== sample.data.toSeq.distinct
  }

  def removeUndefined(jv: JValue): JValue = jv match {
      case JObject(jfields) => JObject(jfields collect { case (s, v) if v != JUndefined => JField(s, removeUndefined(v)) })
      case JArray(jvs) => JArray(jvs map { jv => removeUndefined(jv) })
      case v => v
    }

  def testDistinct = {
    implicit val gen = sort(duplicateRows(sample(schema)))
    prop { (sample: SampleData) =>
      val table = fromSample(sample)

      val distinctTable = table.distinct(Leaf(Source))

      val result = toJson(distinctTable).copoint
      val expected = sample.data.toSeq.distinct

      result must_== expected
    }.set(minTestsOk = 2000)
  }
}
