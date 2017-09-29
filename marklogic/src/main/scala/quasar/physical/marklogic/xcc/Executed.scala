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

package quasar.physical.marklogic.xcc

import slamdata.Predef.Unit
import quasar.fp.ski.κ

import monocle.Iso
import scalaz.{Order, Show}
import scalaz.std.anyVal._

/** A content-free value indicating an operation was executed but does not produce
 * any results.
 */
sealed abstract class Executed

object Executed {
  val executed: Executed = new Executed {}

  val isoUnit: Iso[Executed, Unit] =
    Iso[Executed, Unit](κ(()))(κ(executed))

  implicit val order: Order[Executed] =
    Order.orderBy(isoUnit.get)

  implicit val show: Show[Executed] =
    Show.shows(κ("Executed"))
}
