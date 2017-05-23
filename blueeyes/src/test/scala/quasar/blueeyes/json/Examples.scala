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

package quasar.blueeyes
package json

import quasar.precog.TestSupport._
import Examples._

class ExamplesSpec extends Specification {
  import JParser._

  "Lotto example" in {
    val json          = parseUnsafe(lotto)
    val renderedLotto = json.renderCompact
    json mustEqual parseUnsafe(renderedLotto)
  }

  "Person example" in {
    val json           = parseUnsafe(person)
    val renderedPerson = json.renderPretty
    json mustEqual parseUnsafe(renderedPerson)
    //render(json) mustEqual render(personDSL)
    //compact(render(json \\ "name")) mustEqual """["Joe","Marilyn"]"""
    //compact(render(json \ "person" \ "name")) mustEqual "\"Joe\""
  }

  "Transformation example" in {
    val uppercased = parseUnsafe(person).transform(JField.liftCollect { case JField(n, v) => JField(n.toUpperCase, v) })
    val rendered   = uppercased.renderCompact
    rendered.contains(""""NAME":"Joe"""") mustEqual true
    rendered.contains(""""AGE":35.0""") mustEqual true
    //rendered mustEqual
    //  """{"PERSON":{"NAME":"Joe","AGE":35.0,"SPOUSE":{"PERSON":{"NAME":"Marilyn","AGE":33.0}}}}"""
  }

  "Remove example" in {
    val json = parseUnsafe(person) remove { _ == JString("Marilyn") }
    (json \\ "name").renderCompact mustEqual "\"Joe\""
  }

  "Quoted example" in {
    parseUnsafe(quoted) mustEqual JArray(List(JString("foo \" \n \t \r bar")))
  }

  "Null example" in {
    parseUnsafe(""" {"name": null} """).renderCompact mustEqual """{"name":null}"""
  }

  "Symbol example" in {
    symbols.renderCompact mustEqual """{"f1":"foo","f2":"bar"}"""
  }

  "Unicode example" in {
    parseUnsafe("[\" \\u00e4\\u00e4li\\u00f6t\"]") mustEqual JArray(List(JString(" \u00e4\u00e4li\u00f6t")))
  }

  "Exponent example" in {
    parseUnsafe("""{"num": 2e5 }""") mustEqual JObject(List(JField("num", JNum("2e5"))))
    parseUnsafe("""{"num": -2E5 }""") mustEqual JObject(List(JField("num", JNum("-2E5"))))
    parseUnsafe("""{"num": 2.5e5 }""") mustEqual JObject(List(JField("num", JNum("2.5e5"))))
    parseUnsafe("""{"num": 2.5e-5 }""") mustEqual JObject(List(JField("num", JNum("2.5e-5"))))
  }

  "JSON building example" in {
    val json = JObject(JField("name", JString("joe")) :: Nil) ++ JObject(JField("age", JNum(34)) :: Nil) ++
        JObject(JField("name", JString("mazy")) :: Nil) ++ JObject(JField("age", JNum(31)) :: Nil)

    json.renderCompact mustEqual """[{"name":"joe"},{"age":34},{"name":"mazy"},{"age":31}]"""
  }

  "Example which collects all integers and forms a new JSON" in {
    val json = parseUnsafe(person)
    val ints = json.foldDown(JUndefined: JValue) { (a, v) =>
      v match {
        case x: JNum => a ++ x
        case _       => a
      }
    }
    val out = ints.renderCompact
    out == "[33.0,35.0]" || out == "[35.0,33.0]" mustEqual true
  }

  "Example which folds up to form a flattened list" in {
    val json = parseUnsafe(person)

    def form(list: JPath*): List[(JPath, JValue)] = list.toList.map { path =>
      (path, json(path))
    }

    val folded = (json
      .foldUpWithPath[List[(JPath, JValue)]](Nil) { (list, path, json) =>
        (path, json) :: list
      })
      .sorted
      .collect { case (p, j) => (p, j) }

    val formed = form(
      NoJPath,
      JPath("person"),
      JPath("person.age"),
      JPath("person.name"),
      JPath("person.spouse"),
      JPath("person.spouse.person"),
      JPath("person.spouse.person.age"),
      JPath("person.spouse.person.name")
    )

    folded mustEqual formed
  }

  //"Renders JSON as Scala code" in {
  //  val json = parseUnsafe(lotto)
  //
  //  val output = Printer.compact(renderScala(json))
  //  output.contains("\"winning-numbers\",JArray(JNumStr(2)::JNumStr(45)::JNumStr(34)::JNumStr(23)::JNumStr(7)::JNumStr(5)::JNumStr(3)::Nil)") mustEqual true
  //  //output mustEqual """JObject("lotto",JObject("lotto-id",JNum(5)::"winning-numbers",JArray(JNum(2)::JNum(45)::JNum(34)::JNum(23)::JNum(7)::JNum(5)::JNum(3)::Nil)::"winners",JArray(JObject("winner-id",JNum(23)::"numbers",JArray(JNum(2)::JNum(45)::JNum(34)::JNum(23)::JNum(3)::JNum(5)::Nil)::Nil)::JObject("winner-id",JNum(54)::"numbers",JArray(JNum(52)::JNum(3)::JNum(12)::JNum(11)::JNum(18)::JNum(22)::Nil)::Nil)::Nil)::Nil)::Nil)"""
  //}

}

object Examples {
  def lotto = """
{
  "lotto":{
    "lotto-id":5,
    "winning-numbers":[2,45,34,23,7,5,3],
    "winners":[ {
      "winner-id":23,
      "numbers":[2,45,34,23,3, 5]
    },{
      "winner-id" : 54 ,
      "numbers":[ 52,3, 12,11,18,22 ]
    }]
  }
}
"""

  def person = """
{
  "person": {
    "name": "Joe",
    "age": 35.0,
    "spouse": {
      "person": {
        "name": "Marilyn",
        "age": 33.0
      }
    }
  }
}
"""

  def personDSL =
    JObject(
      JField(
        "person",
        JObject(
          JField("name", JString("Joe")) ::
            JField("age", JNum(35)) ::
              JField(
                "spouse",
                JObject(
                  JField(
                    "person",
                    JObject(
                      JField("name", JString("Marilyn")) ::
                        JField("age", JNum(33)) :: Nil
                    )) :: Nil
                )) :: Nil
        )) :: Nil
    )

  val objArray = """
{ "name": "joe",
  "address": {
    "street": "Bulevard",
    "city": "Helsinki"
  },
  "children": [
    {
      "name": "Mary",
      "age": 5.0
    },
    {
      "name": "Mazy",
      "age": 3.0
    }
  ]
}
"""

  val quoted  = """["foo \" \n \t \r bar"]"""
  val symbols = JObject(JField("f1", JString("foo")) :: JField("f2", JString("bar")) :: Nil)
}
