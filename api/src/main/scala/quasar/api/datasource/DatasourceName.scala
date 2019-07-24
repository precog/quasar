/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef.String

import scalaz.{Order, Show}
import scalaz.std.string._

final case class DatasourceName(value: String)

object DatasourceName extends DatasourceNameInstances

sealed abstract class DatasourceNameInstances {
  implicit val order: Order[DatasourceName] =
    Order.orderBy(_.value)

  implicit val show: Show[DatasourceName] =
    Show.shows(_.value)
}
