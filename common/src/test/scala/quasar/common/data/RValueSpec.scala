/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.common.data

import slamdata.Predef._

import quasar._

object RValueSpec extends Qspec with RValueGenerators {

  "RValue undefined removal" should {

    "not remove scalar value" in {
      RValue.removeUndefined(CLong(1)) must beSome(CLong(1))
    }

    "no-op on value without undefined" >> prop { rvalue: RValue =>
      RValue.removeUndefined(rvalue) must beSome(rvalue)
    }

    "remove single undefined" in {
      RValue.removeUndefined(CUndefined) must beNone
    }

    "remove array that contains only undefined" in {
      val input = RArray(CUndefined, CUndefined, CUndefined)
      RValue.removeUndefined(input) must beNone
    }

    "remove object that contains only undefined" in {
      val input = RObject("a" -> CUndefined, "b" -> CUndefined, "c" -> CUndefined)
      RValue.removeUndefined(input) must beNone
    }

    "remove meta that contains undefined value" in {
      val input = RMeta(CUndefined, RObject(Map("a" -> CLong(1))))
      RValue.removeUndefined(input) must beNone
    }

    "remove meta that contains undefined meta" in {
      val input = RMeta(CLong(1), RObject(Map("a" -> CUndefined)))
      RValue.removeUndefined(input) must beNone
    }

    "remove undefined from array" in {
      val input = RArray(CUndefined, CLong(1), CString("2"), CUndefined, CUndefined, CBoolean(true))
      val expected = RArray(CLong(1), CString("2"), CBoolean(true))
      RValue.removeUndefined(input) must beSome(expected)
    }

    "remove undefined from object" in {
      val input = RObject("a" -> CUndefined, "b" -> CLong(1), "c" -> CString("2"), "d" -> CUndefined, "e" -> CUndefined, "f" -> CBoolean(true))
      val expected = RObject("b" -> CLong(1), "c" -> CString("2"), "f" -> CBoolean(true))
      RValue.removeUndefined(input) must beSome(expected)
    }

    "remove undefined from meta" in {
      val value = RArray(CUndefined, CLong(1), CString("2"), CUndefined, CUndefined, CBoolean(true))
      val meta = RObject("a" -> CUndefined, "b" -> CLong(1), "c" -> CString("2"), "d" -> CUndefined, "e" -> CUndefined, "f" -> CBoolean(true))
      val input = RMeta(value, meta.asInstanceOf[RObject])

      val expectedValue = RArray(CLong(1), CString("2"), CBoolean(true))
      val expectedMeta = RObject("b" -> CLong(1), "c" -> CString("2"), "f" -> CBoolean(true))
      val expected = RMeta(expectedValue, expectedMeta.asInstanceOf[RObject])

      RValue.removeUndefined(input) must beSome(expected)
    }

    "remove recursive undefined with outer object" in {
      val innerObj = RObject("a" -> CUndefined, "b" -> CLong(1), "c" -> CString("2"), "d" -> CUndefined, "e" -> CUndefined, "f" -> CBoolean(true))
      val innerArr = RArray(CUndefined, CLong(1), CString("2"), CUndefined, CUndefined, CBoolean(false))
      val input = RObject("z" -> innerObj, "y" -> CLong(3), "x" -> CString("4"), "w" -> innerArr, "v" -> CUndefined, "u" -> CBoolean(true))

      val expectedInnerObj = RObject("b" -> CLong(1), "c" -> CString("2"), "f" -> CBoolean(true))
      val expectedInnerArr = RArray(CLong(1), CString("2"), CBoolean(false))
      val expected = RObject("z" -> expectedInnerObj, "y" -> CLong(3), "x" -> CString("4"), "w" -> expectedInnerArr, "u" -> CBoolean(true))

      RValue.removeUndefined(input) must beSome(expected)
    }

    "remove recursive undefined with outer array" in {
      val innerObj = RObject("a" -> CUndefined, "b" -> CLong(1), "c" -> CString("2"), "d" -> CUndefined, "e" -> CUndefined, "f" -> CBoolean(true))
      val innerArr = RArray(CUndefined, CLong(1), CString("2"), CUndefined, CUndefined, CBoolean(false))
      val input = RArray(innerObj, CLong(3), CString("4"), innerArr, CUndefined, CBoolean(true))

      val expectedInnerObj = RObject("b" -> CLong(1), "c" -> CString("2"), "f" -> CBoolean(true))
      val expectedInnerArr = RArray(CLong(1), CString("2"), CBoolean(false))
      val expected = RArray(expectedInnerObj, CLong(3), CString("4"), expectedInnerArr, CBoolean(true))

      RValue.removeUndefined(input) must beSome(expected)
    }
  }
}
