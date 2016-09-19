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

sealed abstract class IdStatus
case object IncludeId extends IdStatus
case object ExcludeId extends IdStatus

object IdStatus {
  implicit def equal: Equal[IdStatus] = Equal.equalRef
  implicit def show: Show[IdStatus] = Show.showFromToString
  implicit def renderTree: RenderTree[IdStatus] = RenderTree.fromShow("IdStatus")
}

/** Similar to [[Read]], but returns a dataset with an entry for each record. If
  * `idStatus` is [[IncludeId]], then it returns a two-element array for each
  * record, with the id at element 0 and the record itself at element 1. If it’s
  * [[ExcludeId]], then it simply returns the record.
  */
@Lenses final case class ShiftedRead
  (path: AbsFile[Sandboxed], idStatus: IdStatus)

object ShiftedRead {
  implicit def equal: Equal[ShiftedRead] = Equal.equalBy(_.path)
  implicit def show: Show[ShiftedRead] =
    Show.show(r => Cord("ShiftedRead(") ++
      posixCodec.printPath(r.path) ++ Cord(", ") ++
      r.idStatus.show ++ Cord(")"))
  implicit def renderTree: RenderTree[ShiftedRead] = RenderTree.fromShow("ShiftedRead")
}
