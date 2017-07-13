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

package quasar

import slamdata.Predef.Unit
import quasar.fp.ski.κ

import monocle.Prism
import scalaz._
import scalaz.std.anyVal._

sealed abstract class BackendCapability

object BackendCapability {
  case object Query extends BackendCapability
  case object Write extends BackendCapability

  lazy val All = ISet.fromFoldable(IList[BackendCapability](Query, Write))

  val query = Prism.partial[BackendCapability, Unit] {
    case Query => ()
  } (κ(Query))

  val write = Prism.partial[BackendCapability, Unit] {
    case Write => ()
  } (κ(Write))

  implicit val order: Order[BackendCapability] =
    Order.orderBy {
      case Query => 0
      case Write => 1
    }

  implicit val show: Show[BackendCapability] =
    Show.showFromToString
}
