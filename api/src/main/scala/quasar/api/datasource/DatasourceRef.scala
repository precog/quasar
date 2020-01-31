/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.api.datasource

import monocle.PLens
import monocle.macros.Lenses
import scalaz.{Apply, Equal, Order, Show, Traverse1}
import scalaz.std.tuple._
import scalaz.syntax.show._
import scalaz.syntax.equal._

@Lenses
final case class DatasourceRef[C](kind: DatasourceType, name: DatasourceName, config: C)

object DatasourceRef extends DatasourceRefInstances {
  def atMostRenamed[C: Equal](a: DatasourceRef[C], b: DatasourceRef[C]) =
    a.kind === b.kind && a.config === b.config

  def pConfig[C, D]: PLens[DatasourceRef[C], DatasourceRef[D], C, D] =
    PLens[DatasourceRef[C], DatasourceRef[D], C, D](
      _.config)(
      d => _.copy(config = d))
}

sealed abstract class DatasourceRefInstances extends DatasourceRefInstances0 {
  implicit def order[C: Order]: Order[DatasourceRef[C]] =
    Order.orderBy(c => (c.kind, c.name, c.config))

  implicit def show[C: Show]: Show[DatasourceRef[C]] =
    Show.shows {
      case DatasourceRef(t, n, c) =>
        "DatasourceRef(" + t.shows + ", " + n.shows + ", " + c.shows + ")"
    }

  implicit val traverse1: Traverse1[DatasourceRef] =
    new Traverse1[DatasourceRef] {
      def foldMapRight1[A, B](fa: DatasourceRef[A])(z: A => B)(f: (A, => B) => B) =
        z(fa.config)

      def traverse1Impl[G[_]: Apply, A, B](fa: DatasourceRef[A])(f: A => G[B]) =
        DatasourceRef.pConfig[A, B].modifyF(f)(fa)
    }
}

sealed abstract class DatasourceRefInstances0 {
  implicit def equal[C: Equal]: Equal[DatasourceRef[C]] =
    Equal.equalBy(c => (c.kind, c.name, c.config))
}
