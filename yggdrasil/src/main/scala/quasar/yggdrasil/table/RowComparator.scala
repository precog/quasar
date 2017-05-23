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
package table

import quasar.blueeyes._
import scalaz.Ordering._

trait RowComparator { self =>
  def compare(i1: Int, i2: Int): ScalazOrdering

  def swap: RowComparator = new RowComparator {
    def compare(i1: Int, i2: Int) = self.compare(i2, i1).complement
  }

  @tailrec
  final def nextLeftIndex(lmin: Int, lmax: Int, ridx: Int): Int = {
    compare(lmax, ridx) match {
      case LT => lmax + 1
      case GT =>
        if (lmax - lmin <= 1) {
          compare(lmin, ridx) match {
            case LT      => lmax
            case GT | EQ => lmin
          }
        } else {
          val lmid = lmin + ((lmax - lmin) / 2)
          compare(lmid, ridx) match {
            case LT      => nextLeftIndex(lmid + 1, lmax, ridx)
            case GT | EQ => nextLeftIndex(lmin, lmid - 1, ridx)
          }
        }

      case EQ =>
        if (lmax - lmin <= 1) {
          compare(lmin, ridx) match {
            case LT      => lmax
            case GT | EQ => lmin
          }
        } else {
          val lmid = lmin + ((lmax - lmin) / 2)
          compare(lmid, ridx) match {
            case LT => nextLeftIndex(lmid + 1, lmax, ridx)
            case GT => sys.error("inputs on the left not sorted.")
            case EQ => nextLeftIndex(lmin, lmid - 1, ridx)
          }
        }
    }
  }
}
