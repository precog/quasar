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

package quasar.physical.mongodb.workflow

import scalaz._

sealed abstract class IdHandling
final case object ExcludeId extends IdHandling
final case object IncludeId extends IdHandling
final case object IgnoreId extends IdHandling

object IdHandling {
  implicit val monoid: Monoid[IdHandling] = new Monoid[IdHandling] {
    // this is the `coalesce` function
    def append(f1: IdHandling, f2: => IdHandling) = (f1, f2) match {
      case (_, IgnoreId) => f1
      case (_, _)        => f2
    }

    def zero = IgnoreId
  }

  implicit val show: Show[IdHandling] = Show.showFromToString

  implicit val equal: Equal[IdHandling] = Equal.equalA
}
