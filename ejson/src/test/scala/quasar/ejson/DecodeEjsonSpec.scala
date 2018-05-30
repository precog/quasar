/*
 * Copyright 2014–2018 SlamData Inc.
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

import slamdata.Predef.{Char => SChar, String}
import quasar.contrib.specs2.Spec

import matryoshka.data.Fix
import scalaz._, Scalaz._

class DecodeEJsonSpec extends Spec {
  type J = Fix[EJson]
  val J = Fixed[J]

  "char" >> {
    "Extension.Char" >> prop { c: SChar =>
      DecodeEJson[SChar].decode(J.char(c)).toDisjunction ≟ c.right
    }

    "single character Common.Str" >> prop { c: SChar =>
      DecodeEJson[SChar].decode(J.str(c.toString)).toDisjunction ≟ c.right
    }

    "fail for multicharacter Common.Str" >> prop {
      (s: String) => (s.length > 1) ==> {

      DecodeEJson[SChar].decode(J.str(s)).toDisjunction.isLeft
    }}
  }
}
