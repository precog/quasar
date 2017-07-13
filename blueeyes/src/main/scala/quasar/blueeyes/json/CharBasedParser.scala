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

package quasar.blueeyes.json

import java.lang.Character.isHighSurrogate

import scala.annotation.switch

/**
  * Trait used when the data to be parsed is in UTF-16.
  */
private[json] trait CharBasedParser extends Parser {

  /**
    * See if the string has any escape sequences. If not, return the end of the
    * string. If so, bail out and return -1.
    *
    * This method expects the data to be in UTF-16 and accesses it as chars.
    * In a few cases we might bail out incorrectly (by reading the second-half
    * of a surrogate pair as \\) but for now the belief is that checking every
    * character would be more expensive. So... in those cases we'll fall back to
    * the slower (correct) UTF-16 parsing.
    */
  protected[this] final def parseStringSimple(i: Int, ctxt: Context): Int = {
    var j = i
    var c = at(j)
    while (c != '"') {
      if (c < ' ') die(j, "control char in string")
      if (c == '\\') return -1
      j += 1
      c = at(j)
    }
    j + 1
  }

  /**
    * Parse the string according to JSON rules, and add to the given context.
    *
    * This method expects the data to be in UTF-16, and access it as Char. It
    * performs the correct checks to make sure that we don't interpret a
    * multi-char code point incorrectly.
    */
  protected[this] final def parseString(i: Int, ctxt: Context): Int = {
    val k = parseStringSimple(i + 1, ctxt)
    if (k != -1) {
      ctxt.add(at(i + 1, k - 1))
      return k
    }

    var j = i + 1
    val sb = new CharBuilder

    var c = at(j)
    while (c != '"') {
      if (c < ' ') {
        die(j, "control char in string")
      } else if (c == '\\') {
        (at(j + 1): @switch) match {
          case 'b' => { sb.append('\b'); j += 2 }
          case 'f' => { sb.append('\f'); j += 2 }
          case 'n' => { sb.append('\n'); j += 2 }
          case 'r' => { sb.append('\r'); j += 2 }
          case 't' => { sb.append('\t'); j += 2 }

          // if there's a problem then descape will explode
          case 'u' => { sb.append(descape(at(j + 2, j + 6))); j += 6 }

          // permissive: let any escaped char through, not just ", / and \
          case c2 => { sb.append(c2); j += 2 }
        }
      } else if (isHighSurrogate(c)) {
        // this case dodges the situation where we might incorrectly parse the
        // second Char of a unicode code point.
        sb.append(c)
        sb.append(at(j + 1))
        j += 2
      } else {
        // this case is for "normal" code points that are just one Char.
        sb.append(c)
        j += 1
      }
      j = reset(j)
      c = at(j)
    }
    ctxt.add(sb.makeString)
    j + 1
  }
}
