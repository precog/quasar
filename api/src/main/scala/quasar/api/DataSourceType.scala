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

package quasar.api

import slamdata.Predef.String
import quasar.contrib.refined._
import quasar.fp.numeric.Positive

import eu.timepit.refined.api.Refined
import eu.timepit.refined.scalaz._
import eu.timepit.refined.string.MatchesRegex
import monocle.macros.Lenses
import scalaz.{Cord, Order, Show}
import scalaz.std.anyVal._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._
import shapeless.{Witness => W}

@Lenses
final case class DataSourceType(name: DataSourceType.Name, version: Positive)

object DataSourceType extends DataSourceTypeInstances {
  type Name = String Refined MatchesRegex[W.`"[a-zA-Z0-9-]+"`.T]
}

sealed abstract class DataSourceTypeInstances {
  implicit val order: Order[DataSourceType] =
    Order.orderBy(t => (t.name, t.version))

  implicit val show: Show[DataSourceType] =
    Show.show {
      case DataSourceType(n, v) =>
        Cord("DataSourceType(") ++ n.show ++ Cord(", ") ++ v.show ++ Cord(")")
    }
}
