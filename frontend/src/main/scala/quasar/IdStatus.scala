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

package quasar

import slamdata.Predef.{Product, Serializable}
import scalaz.{Equal, Semigroup, Show}
import scalaz.syntax.std.boolean._
import scalaz.syntax.equal._

sealed abstract class IdStatus extends Product with Serializable

object IdStatus {
  case object IdOnly extends IdStatus
  case object IncludeId extends IdStatus
  case object ExcludeId extends IdStatus

  implicit def equal: Equal[IdStatus] = Equal.equalA

  implicit def renderTree: RenderTree[IdStatus] = RenderTree.fromShow("IdStatus")

  // NB: This forms a semilattice, if we ever have such a type class available.
  implicit def semigroup: Semigroup[IdStatus] = new Semigroup[IdStatus] {
    def append(a: IdStatus, b: => IdStatus) = (a === b).fold(a, IncludeId)
  }

  implicit def show: Show[IdStatus] = Show.showFromToString
}
