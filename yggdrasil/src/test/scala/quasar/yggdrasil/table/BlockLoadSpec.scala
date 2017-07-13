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
import scalaz._
import scalaz.syntax.comonad._
import scalaz.syntax.std.boolean._
import SampleData._
import CValueGenerators._
import quasar.precog.TestSupport._, Gen._

trait BlockLoadSpec extends SpecificationLike with ScalaCheck {
  class BlockStoreLoadTestModule(sampleData: SampleData) extends BlockStoreTestModule[Need] {
    val M = Need.need
    val Some((idCount, schema)) = sampleData.schema
    val actualSchema = inferSchema(sampleData.data map { _ \ "value" })

    val projections = List(actualSchema).map { subschema =>

      val stream = sampleData.data flatMap { jv =>
        val back = subschema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
          case (obj, (jpath, ctype)) => {
            val vpath = JPath(JPathField("value") :: jpath.nodes)
            val valueAtPath = jv.get(vpath)

            if (compliesWithSchema(valueAtPath, ctype)) {
              obj.set(vpath, valueAtPath)
            } else {
              obj
            }
          }
        }

        if (back \ "value" == JUndefined)
          None
        else
          Some(back)
      }

      Path("/test") -> Projection(stream)
    } toMap

  }

  def testLoadDense(sample: SampleData) = {
    val module = new BlockStoreLoadTestModule(sample)

    val expected = sample.data flatMap { jv =>
      val back = module.schema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
        case (obj, (jpath, ctype)) => {
          val vpath = JPath(JPathField("value") :: jpath.nodes)
          val valueAtPath = jv.get(vpath)

          if (module.compliesWithSchema(valueAtPath, ctype)) {
            obj.set(vpath, valueAtPath)
          } else {
            obj
          }
        }
      }

      (back \ "value" != JUndefined).option(back)
    }

    val cschema = module.schema map { case (jpath, ctype) => ColumnRef(CPath(jpath), ctype) }

    val result = module.Table.constString(Set("/test")).load("dummyAPIKey", Schema.mkType(cschema).get).flatMap(t => EitherT.right(t.toJson)).run.copoint
    result.map(_.toList) must_== \/.right(expected.toList)
  }

  def checkLoadDense = {
    implicit val gen = sample(objectSchema(_, 3))
    prop { (sample: SampleData) => testLoadDense(sample) }
  }

  def testLoadSample1 = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":{
            "u":false,
            "md":"t",
            "l":[]
          },
          "key":[1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1 , List(JPath(".u") -> CBoolean, JPath(".md") -> CString, JPath(".l") -> CEmptyArray))
      )
    )

    testLoadDense(sampleData)
  }

  def testLoadSample2 = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":{
            "rzp":{ },
            "hW":1.0,
            "fa":null
          },
          "key":[2,1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (2, List(JPath(".fa") -> CNull, JPath(".hW") -> CLong, JPath(".rzp") -> CEmptyObject))
      )
    )

    testLoadDense(sampleData)
  }

  def testLoadSample3 = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
         {
           "value":{
             "f":{
               "bn":[null],
               "wei":1.0
             },
             "ljz":[null,["W"],true],
             "jmy":4.639428637939817E307
           },
           "key":[1,2,2]
         },
         {
           "value":{
             "f":{
               "bn":[null],
               "wei":5.615997508833152E307
             },
             "ljz":[null,[""],false],
             "jmy":-2.612503123965922E307
           },
           "key":[2,1,1]
         }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (3, List(JPath(".f.bn[0]") -> CNull,
                 JPath(".f.wei") -> CLong,
                 JPath(".f.wei") -> CDouble,
                 JPath(".ljz[0]") -> CNull,
                 JPath(".ljz[1][0]") -> CString,
                 JPath(".ljz[2]") -> CBoolean,
                 JPath(".jmy") -> CDouble))
      )
    )

    testLoadDense(sampleData)
  }

  def testLoadSample4 = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":{
            "dV":{
              "d":true,
              "l":false,
              "vq":{

              }
            },
            "oy":{
              "nm":false
            },
            "uR":-6.41847178802919E307
          },
          "key":[1,1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (2, List(JPath(".dV.d") -> CBoolean,
                 JPath(".dV.l") -> CBoolean,
                 JPath(".dV.vq") -> CEmptyObject,
                 JPath(".oy.nm") -> CBoolean,
                 JPath(".uR") -> CDouble))
      )
    )

    testLoadDense(sampleData)
  }

  def testLoadSample5 = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":{
            "cfnYTg92dg":"gu",
            "fg":[false,8.988465674311579E307,-1],
            "o8agyghfjxe":[]
          },
          "key":[1]
        },
        {
          "value":{
            "cfnYTg92dg":"yoqmrz",
            "fg":[false,0.0,0],
            "o8agyghfjxe":[]
          },
          "key":[1]
        },
        {
          "value":{
            "cfnYTg92dg":"bzjhpndgoY",
            "fg":[true,5.899727648511153E307,0],
            "o8agyghfjxe":[]
          },
          "key":[2]
        },
        {
          "value":{
            "cfnYTg92dg":"ztDcxy",
            "fg":[false,-1.0,-1],
            "o8agyghfjxe":[]
          },
          "key":[2]
        },
        {
          "value":{
            "cfnYTg92dg":"jeuHxunPdg",
            "fg":[true,3.3513345026993237E307,0],
            "o8agyghfjxe":[]
          },
          "key":[3]
        },
        {
          "value":{
            "cfnYTg92dg":"evxnIfv",
            "fg":[false,-5.295630177665229E307,1],
            "o8agyghfjxe":[]
          },
          "key":[3]
        },
        {
          "value":{
            "cfnYTg92dg":"v",
            "fg":[true,-6.98151882908554E307,3047586736114377501],
            "o8agyghfjxe":[]
          },
          "key":[6]
        },
        {
          "value":{
            "cfnYTg92dg":"ontecesf",
            "fg":[false,5.647795622045506E307,-1],
            "o8agyghfjxe":[]
          },
          "key":[6]
        },
        {
          "value":{
            "cfnYTg92dg":"",
            "fg":[true,1.0,-4341538468449353975],
            "o8agyghfjxe":[]
          },
          "key":[7]
        },
        {
          "value":{
            "cfnYTg92dg":"Hwpqxk",
            "fg":[true,-4.38879797446784E307,4611686018427387903],
            "o8agyghfjxe":[]
          },
          "key":[9]
        },
        {
          "value":{
            "cfnYTg92dg":"mkkhV",
            "fg":[true,-1.0,3724086638589828262],
            "o8agyghfjxe":[]
          },
          "key":[9]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some((1, List((JPath(".o8agyghfjxe") -> CEmptyArray),
                    (JPath(".fg[0]") -> CBoolean),
                    (JPath(".fg[1]") -> CNum),
                    (JPath(".fg[1]") -> CLong),
                    (JPath(".fg[2]") -> CNum),
                    (JPath(".fg[2]") -> CLong),
                    (JPath(".cfnYTg92dg") -> CString))))
    )

    testLoadDense(sampleData)
  }

}
