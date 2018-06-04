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

package quasar.qscript

import quasar.RenderTree

import scalaz._

sealed abstract class JoinSide {
  def fold[A](left: => A, right: => A): A =
    this match {
      case LeftSide => left
      case RightSide => right
    }
}
final case object LeftSide extends JoinSide
final case object RightSide extends JoinSide

object JoinSide {
  implicit val equal: Equal[JoinSide] = Equal.equalA
  implicit val show: Show[JoinSide] = Show.showFromToString
  implicit val renderTree: RenderTree[JoinSide] = RenderTree.fromShowAsType("JoinSide")
}

sealed abstract class JoinSide3 {
  def fold[A](left: => A, center: => A, right: => A): A =
    this match {
      case LeftSide3 => left
      case Center => center
      case RightSide3 => right
    }
}
case object LeftSide3 extends JoinSide3
case object Center extends JoinSide3
case object RightSide3 extends JoinSide3

object JoinSide3 {
  implicit val equal: Equal[JoinSide3] = Equal.equalA
  implicit val show: Show[JoinSide3] = Show.showFromToString
  implicit val renderTree: RenderTree[JoinSide3] = RenderTree.fromShowAsType("JoinSide3")
}
