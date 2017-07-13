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

package quasar.physical.marklogic.xml

import slamdata.Predef._
import quasar.Data._

class ToDataSpec extends quasar.Qspec {
  "toEJsonData" should {
    "simple text node" in {
      val sample = <foo>hello</foo>
      toEJsonData(sample) must_= Obj(ListMap("foo" -> Str("hello")))
    }

    "simple nested structure" in {
      val sample =
        <foo>
          <bar>msg1</bar>
          <bar>msg2</bar>
        </foo>
      val expected =
        Obj(ListMap("foo" ->
          Obj(ListMap("bar" -> Arr(List(Str("msg1"), Str("msg2")))))))
      toEJsonData(sample) must_= expected
    }

    "support attributes" >> {
      "simplest case" in {
        val sample =
          <foo type="baz">
            <bar>msg</bar>
          </foo>
        val expected =
          Obj(ListMap(
            "foo" -> Obj(ListMap(
              "_xml.attributes" -> Obj(ListMap(
                "type" -> Str("baz")
              )),
              "bar" -> Str("msg")
            ))
          ))
        toEJsonData(sample) must_= expected
      }

      "element with attribute but only text" in {
        val sample = <foo type="baz">msg</foo>
        val expected =
          Obj(ListMap(
            "foo" -> Obj(ListMap(
              "_xml.attributes" -> Obj(ListMap(
                "type" -> Str("baz")
              )),
              "_xml.text" -> Str("msg")
          ))
        ))
        toEJsonData(sample) must_= expected
      }

      "element with just attributes" in {
        val sample = <foo type="baz" quux="bar" />
        val expected = Obj(
          "foo" -> Obj(
            "_xml.attributes" -> Obj(
              "type" -> Str("baz"),
              "quux" -> Str("bar")
            ),
            "_xml.text" -> Str("")))
        toEJsonData(sample) must_= expected
      }
    }

    "element with name prefix" in {
      val sample = <quasar:foo>hello</quasar:foo>
      val expected =
        Obj(ListMap(
          "quasar:foo" -> Str("hello")
        ))
      toEJsonData(sample) must_= expected
    }

    "empty element" in {
      val sample = <foo></foo>
      val expected =
        Obj(ListMap(
          "foo" -> Str("")
        ))
      toEJsonData(sample) must_= expected
    }

    "multiple empty elements" in {
      val sample =
        <foo>
          <bar></bar>
          <bar></bar>
          <bar></bar>
        </foo>
      val expected =
        Obj(ListMap(
          "foo" -> Obj(ListMap(
            "bar" -> Arr(List(Str(""), Str(""), Str("")))
          ))
        ))
      toEJsonData(sample) must_= expected
    }

    "example" in {
      val sample =
        <foo type="baz" id="1">
          <bar>
            <baz>37</baz>
            <bat>one</bat>
            <bat>two</bat>
          </bar>
          <quux>lorem ipsum</quux>
        </foo>
      val expected =
        Obj(ListMap(
          "foo" -> Obj(ListMap(
            "_xml.attributes" -> Obj(ListMap(
              "type" -> Str("baz"),
              "id"   -> Str("1")
            )),
            "bar"         -> Obj(ListMap(
              "baz" -> Str("37"),
              "bat" -> Arr(List(Str("one"), Str("two")))
            )),
            "quux"        -> Str("lorem ipsum")
          ))
        ))
      toEJsonData(sample) must_= expected
    }
  }
}
