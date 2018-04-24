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

package quasar.time

import slamdata.Predef._

import scalaz.{Order, Ordering, Show}
import scalaz.std.anyVal._

sealed abstract class TemporalPart extends Serializable

object TemporalPart {
  final case object Century     extends TemporalPart
  final case object Day         extends TemporalPart
  final case object Decade      extends TemporalPart
  final case object Hour        extends TemporalPart
  final case object Microsecond extends TemporalPart
  final case object Millennium  extends TemporalPart
  final case object Millisecond extends TemporalPart
  final case object Minute      extends TemporalPart
  final case object Month       extends TemporalPart
  final case object Quarter     extends TemporalPart
  final case object Second      extends TemporalPart
  final case object Week        extends TemporalPart
  final case object Year        extends TemporalPart

  private def toInt(t: TemporalPart) = t match {
    case Microsecond => 0
    case Millisecond => 1
    case Second => 2
    case Minute => 3
    case Hour => 4
    case Day => 5
    case Week => 6
    case Month => 7
    case Quarter => 8
    case Year => 9
    case Decade => 10
    case Century => 11
    case Millennium => 12
  }

  implicit val order: Order[TemporalPart] = new Order[TemporalPart] {
    def order(x: TemporalPart, y: TemporalPart): Ordering =
      Order[Int].order(toInt(x), toInt(y))
  }
  implicit val show: Show[TemporalPart] = Show.showFromToString
}
