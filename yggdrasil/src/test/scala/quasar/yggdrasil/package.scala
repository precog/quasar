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

package quasar

import quasar.blueeyes._
import scalaz._, Scalaz._, Ordering._

package object yggdrasil {
  type Identity   = Long
  type Identities = Array[Identity]
  object Identities {
    val Empty = Vector.empty[Identity]
  }

  type SEvent = (Identities, SValue)

  object SEvent {
    @inline
    def apply(id: Identities, sv: SValue): SEvent = (id, sv)
  }

  def prefixIdentityOrdering(ids1: Identities, ids2: Identities, prefixLength: Int): scalaz.Ordering = {
    var result: scalaz.Ordering = EQ
    var i                      = 0
    while (i < prefixLength && (result eq EQ)) {
      result = longInstance.order(ids1(i), ids2(i))
      i += 1
    }

    result
  }

  def fullIdentityOrdering(ids1: Identities, ids2: Identities) = prefixIdentityOrdering(ids1, ids2, ids1.length min ids2.length)

  object IdentitiesOrder extends scalaz.Order[Identities] {
    def order(ids1: Identities, ids2: Identities) = fullIdentityOrdering(ids1, ids2)
  }

  def prefixIdentityOrder(prefixLength: Int): scalaz.Order[Identities] = {
    new scalaz.Order[Identities] {
      def order(ids1: Identities, ids2: Identities) = prefixIdentityOrdering(ids1, ids2, prefixLength)
    }
  }

  def tupledIdentitiesOrder[A](idOrder: scalaz.Order[Identities]): scalaz.Order[(Identities, A)] =
    idOrder.contramap((_: (Identities, A))._1)

  def identityValueOrder[A](idOrder: scalaz.Order[Identities])(implicit ord: scalaz.Order[A]): scalaz.Order[(Identities, A)] =
    new scalaz.Order[(Identities, A)] {
      type IA = (Identities, A)
      def order(x: IA, y: IA): scalaz.Ordering = {
        val idComp = idOrder.order(x._1, y._1)
        if (idComp == EQ) {
          ord.order(x._2, y._2)
        } else idComp
      }
    }

  def valueOrder[A](implicit ord: scalaz.Order[A]): scalaz.Order[(Identities, A)] = new scalaz.Order[(Identities, A)] {
    type IA = (Identities, A)
    def order(x: IA, y: IA): scalaz.Ordering = {
      ord.order(x._2, y._2)
    }
  }
}
