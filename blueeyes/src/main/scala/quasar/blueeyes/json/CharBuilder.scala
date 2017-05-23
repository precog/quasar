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

import quasar.blueeyes._

// efficient char-by-char string builder, taken from jawn under MIT license.
// (https://github.com/non/jawn)

private[json] final class CharBuilder {
  @inline final def INITIALSIZE = 16

  private var cs       = new Array[Char](INITIALSIZE)
  private var capacity = INITIALSIZE
  private var len      = 0

  def makeString: String = new String(cs, 0, len)

  def extend(s: String) {
    var i = 0
    val len = s.length
    while (i < len) { append(s.charAt(i)); i += 1 }
  }

  def append(c: Char) {
    if (len == capacity) {
      val n   = capacity * 2
      val ncs = new Array[Char](n)
      System.arraycopy(cs, 0, ncs, 0, capacity)
      cs = ncs
      capacity = n
    }
    cs(len) = c
    len += 1
  }
}
