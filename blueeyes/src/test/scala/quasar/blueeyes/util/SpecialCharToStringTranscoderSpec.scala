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
package util

import quasar.precog.TestSupport._

class SpecialCharToStringTranscoderSpec extends Specification {
  val transcoder = SpecialCharToStringTranscoder({ case c: Char if (c == '.' | c == '@') => new String(Array('%', c, c)) }, {
    case c :: Nil if (c == '%')          => None
    case '%' :: List(c)                  => None
    case '%' :: y :: List(c) if (y == c) => Some(c)
  })

  "SpecialCharToStringTranscoder.encode" should {
    "encode specified chars" in {
      transcoder.encode("@foo.baz") mustEqual ("%@@foo%..baz")
    }
  }

  "SpecialCharToStringTranscoder.decode" should {
    "decode specified chars" in {
      transcoder.decode("%@@foo%..baz") mustEqual ("@foo.baz")
    }
    "decode incomple chars" in {
      transcoder.decode("%@foo%..baz%.") mustEqual ("%@foo.baz%.")
    }
  }
}
