/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef.{List, Nil}

import quasar.RenderTree.ops._

import monocle.macros.Lenses

import scalaz.{Equal, Show}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.show._

/** Describes a sequence of row-level transformations to apply to a dataset.
  *
  * `idStatus` is always the first transformation, followed by `stages`.
  */
@Lenses
final case class ScalarStages(idStatus: IdStatus, stages: List[ScalarStage])

object ScalarStages {
  /** Does not modify a dataset. */
  val Id: ScalarStages =
    ScalarStages(IdStatus.ExcludeId, Nil)

  implicit val equal: Equal[ScalarStages] =
    Equal.equalBy(ss => (ss.idStatus, ss.stages))

  implicit val renderTree: RenderTree[ScalarStages] =
    RenderTree.make(ss => NonTerminal(
      List("ScalarStages"),
      some(ss.idStatus.shows),
      ss.stages.render.children))

  implicit val show: Show[ScalarStages] =
    RenderTree.toShow
}
