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

package quasar.fp

import slamdata.Predef._
import quasar.RenderTree
import quasar.fp.ski._

import monocle.Iso
import scalaz._

/** Used for a normally-recursive parameter that has been “externalized”. E.g.,
  * `Tree[LogicalPlan[ExternallyManaged]]` vs `Fix[LogicalPlan]`. This indicates
  * that the recursive structure is intact, but is handled by some containing
  * structure (`Tree`, in that example).
  */
sealed abstract class ExternallyManaged

final case object Extern extends ExternallyManaged

object ExternallyManaged {
  def unit = Iso[ExternallyManaged, Unit](κ(()))(κ(Extern))

  implicit val equal: Equal[ExternallyManaged] = Equal.equalA
  implicit val show: Show[ExternallyManaged] = Show.showFromToString
  implicit val renderTree: RenderTree[ExternallyManaged] =
    RenderTree.fromShow("ExternallyManaged")
}
