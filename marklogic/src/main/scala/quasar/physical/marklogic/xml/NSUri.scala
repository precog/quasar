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

package quasar.physical.marklogic.xml

import quasar.Predef.String

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.Uri
import scalaz.{Order, Show}
import scalaz.std.string._
import scalaz.syntax.show._

/** A URI used to denote a namespace. */
final case class NSUri(value: String Refined Uri) {
  override def toString = this.shows
}

object NSUri {
  implicit val order: Order[NSUri] =
    Order.orderBy(_.value.value)

  implicit val show: Show[NSUri] =
    Show.shows(_.value.value)
}
