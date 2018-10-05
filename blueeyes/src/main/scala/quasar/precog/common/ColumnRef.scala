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

package quasar.precog
package common

import scalaz.syntax.semigroup._
import scalaz.syntax.order._

import com.rklaehn.radixtree.Hash

case class ColumnRef(selector: CPath, ctype: CType)

object ColumnRef {
  def identity(ctype: CType) = ColumnRef(CPath.Identity, ctype)

  implicit object order extends scalaz.Order[ColumnRef] {
    def order(r1: ColumnRef, r2: ColumnRef): scalaz.Ordering = {
      (r1.selector ?|? r2.selector) |+| (r1.ctype ?|? r2.ctype)
    }
  }

  implicit val ordering: scala.math.Ordering[ColumnRef] = order.toScalaOrdering

  implicit val hashColumnRef: Hash[ColumnRef] = new Hash[ColumnRef] {
    def eqv(x: ColumnRef, y: ColumnRef): Boolean = scalaz.Equal[ColumnRef].equal(x, y)
    def hash(a: ColumnRef): Int = a.hashCode
  }
}
