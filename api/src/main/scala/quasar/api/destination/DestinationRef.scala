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

package quasar.api.destination

import monocle.PLens
import monocle.macros.Lenses
import scalaz.{Apply, Cord, Equal, Order, Show, Traverse1}
import scalaz.std.tuple._
import scalaz.syntax.show._

@Lenses
final case class DestinationRef[C](kind: DestinationType, name: DestinationName, config: C)

object DestinationRef extends DestinationRefInstances {
  def pConfig[C, D]: PLens[DestinationRef[C], DestinationRef[D], C, D] =
    PLens[DestinationRef[C], DestinationRef[D], C, D](
      _.config)(
      d => _.copy(config = d))
}

sealed abstract class DestinationRefInstances extends DestinationRefInstances0 {
  implicit def order[C: Order]: Order[DestinationRef[C]] =
    Order.orderBy(c => (c.kind, c.name, c.config))

  implicit def show[C: Show]: Show[DestinationRef[C]] =
    Show.show {
      case DestinationRef(t, n, c) =>
        Cord("DestinationRef(") ++ t.show ++ Cord(", ") ++ n.show ++ Cord(", ") ++ c.show ++ Cord(")")
    }

  implicit val traverse1: Traverse1[DestinationRef] =
    new Traverse1[DestinationRef] {
      def foldMapRight1[A, B](fa: DestinationRef[A])(z: A => B)(f: (A, => B) => B) =
        z(fa.config)

      def traverse1Impl[G[_]: Apply, A, B](fa: DestinationRef[A])(f: A => G[B]) =
        DestinationRef.pConfig[A, B].modifyF(f)(fa)
    }
}

sealed abstract class DestinationRefInstances0 {
  implicit def equal[C: Equal]: Equal[DestinationRef[C]] =
    Equal.equalBy(c => (c.kind, c.name, c.config))
}
