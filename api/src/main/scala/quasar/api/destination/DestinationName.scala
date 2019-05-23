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

import slamdata.Predef.String

import scalaz.{Order, Show}
import scalaz.std.string._

final case class DestinationName(value: String)

object DestinationName extends DestinationNameInstances

sealed abstract class DestinationNameInstances {
  implicit val order: Order[DestinationName] =
    Order.orderBy(_.value)

  implicit val show: Show[DestinationName] =
    Show.shows(_.value)
}
