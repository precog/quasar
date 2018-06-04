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

package quasar.common

import slamdata.Predef._
import quasar.{RenderTree, Terminal}

import scalaz._, Scalaz._

sealed abstract class JoinType extends Product with Serializable

object JoinType {
  final case object Inner extends JoinType
  final case object FullOuter extends JoinType
  final case object LeftOuter extends JoinType
  final case object RightOuter extends JoinType

  implicit val equal: Equal[JoinType] = Equal.equalA
  implicit val show: Show[JoinType] = Show.showFromToString
  implicit val renderTree: RenderTree[JoinType] =
    RenderTree.make(t => Terminal(List(t.shows, "JoinType"), None))
}
