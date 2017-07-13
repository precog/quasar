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

import scala.annotation.switch

/**
  * Trait used when the data to be parsed is in UTF-8.
  */
private[json] trait ByteBasedParser extends Parser {
  protected[this] def byte(i: Int): Byte

  /**
    * See if the string has any escape sequences. If not, return the end of the
    * string. If so, bail out and return -1.
    *
    * This method expects the data to be in UTF-8 and accesses it as bytes. Thus
    * we can just ignore any bytes with the highest bit set.
    */
  protected[this] final def parseStringSimple(i: Int, ctxt: Context): Int = {
    var j = i
    var c = byte(j)
    while (c != 34) {
      if (c == 92) return -1
      j += 1
      c = byte(j)
    }
    j + 1
  }

  /**
    * Parse the string according to JSON rules, and add to the given context.
    *
    * This method expects the data to be in UTF-8 and accesses it as bytes.
    */
  protected[this] final def parseString(i: Int, ctxt: Context): Int = {
    val k = parseStringSimple(i + 1, ctxt)
    if (k != -1) {
      ctxt.add(at(i + 1, k - 1))
      return k
    }

    var j = i + 1
    val sb = new CharBuilder

    var c = byte(j)
    while (c != 34) { // "
      if (c == 92) { // \
        (byte(j + 1): @switch) match {
          case 98  => { sb.append('\b'); j += 2 }
          case 102 => { sb.append('\f'); j += 2 }
          case 110 => { sb.append('\n'); j += 2 }
          case 114 => { sb.append('\r'); j += 2 }
          case 116 => { sb.append('\t'); j += 2 }

          // if there's a problem then descape will explode
          case 117 => { sb.append(descape(at(j + 2, j + 6))); j += 6 }

          // permissive: let any escaped char through, not just ", / and \
          case c2 => { sb.append(c2.toChar); j += 2 }
        }
      } else if (c < 128) {
        // 1-byte UTF-8 sequence
        sb.append(c.toChar)
        j += 1
      } else if ((c & 224) == 192) {
        // 2-byte UTF-8 sequence
        sb.extend(at(j, j + 2))
        j += 2
      } else if ((c & 240) == 224) {
        // 3-byte UTF-8 sequence
        sb.extend(at(j, j + 3))
        j += 3
      } else if ((c & 248) == 240) {
        // 4-byte UTF-8 sequence
        sb.extend(at(j, j + 4))
        j += 4
      } else {
        die(j, "invalid UTF-8 encoding")
      }
      c = byte(j)
    }
    ctxt.add(sb.makeString)
    j + 1
  }
}
