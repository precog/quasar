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

package quasar.impl.datasources

import quasar.api.DataSourceType

import monocle.PLens
import monocle.macros.Lenses
import scalaz.{Apply, Cord, Equal, Order, Show, Traverse1}
import scalaz.std.tuple._
import scalaz.syntax.show._

@Lenses
final case class DataSourceConfig[C](kind: DataSourceType, config: C)

object DataSourceConfig extends DataSourceConfigInstances {
  def pConfig[C, D]: PLens[DataSourceConfig[C], DataSourceConfig[D], C, D] =
    PLens[DataSourceConfig[C], DataSourceConfig[D], C, D](
      _.config)(
      d => _.copy(config = d))
}

sealed abstract class DataSourceConfigInstances extends DataSourceConfigInstances0 {
  def order[C: Order]: Order[DataSourceConfig[C]] =
    Order.orderBy(c => (c.kind, c.config))

  def show[C: Show]: Show[DataSourceConfig[C]] =
    Show.show {
      case DataSourceConfig(t, c) =>
        Cord("DataSourceConfig(") ++ t.show ++ Cord(", ") ++ c.show ++ Cord(")")
    }

  val traverse1: Traverse1[DataSourceConfig] =
    new Traverse1[DataSourceConfig] {
      def foldMapRight1[A, B](fa: DataSourceConfig[A])(z: A => B)(f: (A, => B) => B) =
        z(fa.config)

      def traverse1Impl[G[_]: Apply, A, B](fa: DataSourceConfig[A])(f: A => G[B]) =
        DataSourceConfig.pConfig[A, B].modifyF(f)(fa)
    }
}

sealed abstract class DataSourceConfigInstances0 {
  def equal[C: Equal]: Equal[DataSourceConfig[C]] =
    Equal.equalBy(c => (c.kind, c.config))
}
