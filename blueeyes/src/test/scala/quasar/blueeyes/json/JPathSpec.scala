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

import org.scalacheck.Prop
import org.scalacheck.Arbitrary, Arbitrary.arbitrary
import quasar.precog.JsonTestSupport._

object JPathSpec extends Specification with ScalaCheck {
  "Parser" should {
    "parse all valid JPath strings" in {
      prop { (jpath: JPath) =>
        JPath(jpath.toString) == jpath
      }
    }

    "forgivingly parse initial field name without leading dot" in {
      JPath("foo.bar").nodes mustEqual (JPathField("foo") :: JPathField("bar") :: Nil)
    }
  }

  "Extractor" should {
    "extract all existing paths" in {

      implicit val arb: Arbitrary[(JValue, List[(JPath, JValue)])] = Arbitrary {
        for (jv <- arbitrary[JObject]) yield (jv, jv.flattenWithPath)
      }

      prop { (testData: (JValue, List[(JPath, JValue)])) =>
        testData match {
          case (obj, allPathValues) =>
            val allProps = allPathValues.map {
              case (path, pathValue) => path.extract(obj) == pathValue
            }
            allProps.foldLeft[Prop](true)(_ && _)
        }
      }
    }

    "extract a second level node" in {
      val j = JObject(JField("address", JObject(JField("city", JString("B")) :: JField("street", JString("2")) :: Nil)) :: Nil)

      JPath("address.city").extract(j) mustEqual (JString("B"))
    }
  }

  "Parent" should {
    "return parent" in {
      JPath(".foo.bar").parent must beSome(JPath(".foo"))
    }

    "return Identity for path 1 level deep" in {
      JPath(".foo").parent must beSome(NoJPath)
    }

    "return None when there is no parent" in {
      NoJPath.parent mustEqual None
    }
  }

  "Ancestors" should {
    "return two ancestors" in {
      JPath(".foo.bar.baz").ancestors mustEqual List(JPath(".foo.bar"), JPath(".foo"), NoJPath)
    }

    "return empty list for identity" in {
      NoJPath.ancestors mustEqual Nil
    }
  }

  "dropPrefix" should {
    "return just the remainder" in {
      JPath(".foo.bar[1].baz").dropPrefix(".foo.bar") must beSome(JPath("[1].baz"))
    }

    "return none on path mismatch" in {
      JPath(".foo.bar[1].baz").dropPrefix(".foo.bar[2]") must beNone
    }
  }

  "Ordering" should {
    "sort according to nodes names/indexes" in {
      val test = List(
        JPath("[1]"),
        JPath("[0]"),
        JPath("a"),
        JPath("a[9]"),
        JPath("a[10]"),
        JPath("b[10]"),
        JPath("a[10].a[1]"),
        JPath("b[10].a[1]"),
        JPath("b[10].a.x"),
        JPath("b[10].a[0]"),
        JPath("b[10].a[0].a")
      )

      val expected = List(1, 0, 2, 3, 4, 6, 5, 9, 10, 7, 8) map test

      test.sorted must_== expected
    }
  }
}
