/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.fp._
import quasar.RenderTree

import matryoshka._
import monocle.macros.Lenses
import pathy.Path._
import scalaz._, Scalaz._

// TODO: Abstract Read over the backend’s preferred path representation.
/** A backend-resolved `Root`, which is now a path. */
@Lenses final case class Read(path: AbsFile[Sandboxed])

object Read {
  implicit def equal: Equal[Read] = Equal.equalBy(_.path)
  implicit def show: Show[Read] =
    Show.show(r => Cord("Read(") ++ posixCodec.printPath(r.path) ++ Cord(")"))

  implicit def renderTree: RenderTree[Read] = RenderTree.fromShow("Read")
}
