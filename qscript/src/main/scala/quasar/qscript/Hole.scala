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

import slamdata.Predef._
import quasar.fp.ski._
import quasar.{RenderTree, Terminal}

import monocle.Iso
import scalaz._

sealed abstract class Hole

final case object SrcHole extends Hole

object Hole {
  def apply(): Hole = SrcHole

  def unit = Iso[Hole, Unit](κ(()))(κ(SrcHole))

  implicit val equal: Equal[Hole] = Equal.equalA
  implicit val show: Show[Hole] = Show.showFromToString
  implicit val renderTree: RenderTree[Hole] = RenderTree.make(κ(Terminal(List("○"), None)))
}
