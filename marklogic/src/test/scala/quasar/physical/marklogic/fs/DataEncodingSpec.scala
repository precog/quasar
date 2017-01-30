/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.{Data, DataArbitrary}, DataArbitrary._
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.fs.data._
import quasar.physical.marklogic.qscript.EJsonTypeKey
import quasar.physical.marklogic.xml.SecureXML

import scala.xml.Elem

import argonaut._, Argonaut._
import scalaz._, Scalaz._

final class DataEncodingSpec extends quasar.Qspec {
  type Result[A] = ErrorMessages \/ A

  def parseXML(s: String): Result[Elem] =
    SecureXML.loadString(s).leftMap(_.toString.wrapNel)

  "Data <-> XML encoding" >> {
    "roundtrip" >> prop { xd: XmlSafeData =>
      (encodeXml[Result](xd.data) >>= decodeXmlStrict[Result] _) must_= xd.data.some.right
    }

    "roundtrip through serialization" >> prop { xd: XmlSafeData =>
      val rt = encodeXml[Result](xd.data) >>= (e => parseXML(e.toString)) >>= decodeXmlStrict[Result] _
      rt must_= xd.data.some.right
    }

    "None on attempt to decode non-data XML" >> {
      (parseXML("<foo>bar</foo>") >>= decodeXmlStrict[Result]) must_= None.right
    }

    "handle untyped nodes with recovery function" >> {
      val orig = s"""
        <ejson:ejson ejson:type="object" xmlns:ejson="http://quasar-analytics.org/ejson">
          <foo ejson:type="integer">34</foo>
          <bar ejson:type="id">123</bar>
          <baz>no types here</baz>
        </ejson:ejson>
      """

      val exp = Data._obj(ListMap(
        "foo" -> Data._int(34),
        "bar" -> Data._id("123"),
        "baz" -> Data._dec(42.0)
      ))

      (parseXML(orig) >>= decodeXml[Result](_ => Data._dec(42.0).right)) must_= exp.some.right
    }

    "error when object key is not a valid QName" >> {
      val k = "42 not qname"
      val d = Data.singletonObj(k, Data.Str("foo"))

      encodeXml[Result](d) must beLike {
        case -\/(NonEmptyList(msg, _)) => msg must contain(k)
      }
    }

    "error for Data.Set" >> {
      encodeXml[Result](Data.Set(List(Data.Str("a")))) must be_-\/
    }
  }

  "Data <-> JSON Encoding" >> {
    "roundtrip" >> prop { d: Data =>
      decodeJson[Result](encodeJson(d)) must_= d.right
    }

    "decodes typed null as Null" >> {
      decodeJson[Result](jSingleObject(EJsonTypeKey, jString("null"))) must_= Data.Null.right
    }
  }
}
