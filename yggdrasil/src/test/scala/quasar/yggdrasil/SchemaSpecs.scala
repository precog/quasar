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

import quasar.yggdrasil.bytecode._
import quasar.blueeyes._
import quasar.precog.common._
import quasar.precog.TestSupport._

class SchemaSpec extends Specification {
  "cpath" should {
    "return the correct sequence of CPath" in {
      val jtype = JObjectFixedT(Map(
        "foo" -> JNumberT,
        "bar" -> JArrayFixedT(Map(
          0 -> JBooleanT,
          1 -> JObjectFixedT(Map(
            "baz" -> JArrayHomogeneousT(JNullT))),
          2 -> JTextT))))

      val result = Schema.cpath(jtype)
      val expected = Seq(
        CPath(CPathField("foo")),
        CPath(CPathField("bar"), CPathIndex(0)),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("baz"), CPathArray),
        CPath(CPathField("bar"), CPathIndex(2))) sorted

      result mustEqual expected
    }
  }
}
