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

package quasar.ejson

import slamdata.Predef._
import quasar.Qspec
import quasar.fp._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz.Inject

final class JsonParserSpec extends Qspec {
  type J = Fix[Json]

  val C = Inject[Common, Json]
  val O = Inject[Obj,    Json]

  "EJson JSON parser" should {
    "properly construct values" >> {
      val js = """{
        "array"  : [ 1, "two" ],
        "null"   : null,
        "false"  : false,
        "true"   : true,
        "num"    : 3598.345455,
        "int"    : 998765,
        "string" : "lorem ipsum"
      }"""

      jsonParser[Fix[Json], Json].parseUnsafe(js) must_= O(Obj(ListMap(
        "array"  -> C(Arr(List(C(Dec[J](BigDecimal(1))).embed, C(Str[J]("two")).embed))).embed,
        "null"   -> C(Null[J]()).embed,
        "false"  -> C(Bool[J](false)).embed,
        "true"   -> C(Bool[J](true)).embed,
        "num"    -> C(Dec[J](BigDecimal(3598.345455))).embed,
        "int"    -> C(Dec[J](BigDecimal(998765))).embed,
        "string" -> C(Str[J]("lorem ipsum")).embed
      ))).embed
    }
  }
}
