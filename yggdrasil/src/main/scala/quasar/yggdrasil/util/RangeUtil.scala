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

package quasar.yggdrasil.util

import quasar.yggdrasil.table._

import scala.annotation.tailrec

object RangeUtil {

  /**
    * Loops through a Range much more efficiently than Range#foreach, running
    * the provided callback 'f' on each position. Assumes that step is 1.
    */
  def loop(r: Range)(f: Int => Unit) {
    var i = r.start
    val limit = r.end
    while (i < limit) {
      f(i)
      i += 1
    }
  }

  /**
    * Like loop but also includes a built-in check for whether the given Column
    * is defined for this particular row.
    */
  def loopDefined(r: Range, col: Column)(f: Int => Unit): Boolean = {
    @tailrec def unseen(i: Int, limit: Int): Boolean =
      if (i < limit) {
        if (col.isDefinedAt(i)) { f(i); seen(i + 1, limit) } else unseen(i + 1, limit)
      } else {
        false
      }

    @tailrec def seen(i: Int, limit: Int): Boolean =
      if (i < limit) {
        if (col.isDefinedAt(i)) f(i)
        seen(i + 1, limit)
      } else {
        true
      }

    unseen(r.start, r.end)
  }
}
