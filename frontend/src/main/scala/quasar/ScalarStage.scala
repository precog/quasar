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

import slamdata.Predef._

import quasar.RenderTree.ops._
import quasar.api.table.ColumnType
import quasar.common.{CPath, CPathField}

import scala.math.Ordering

import scalaz.{Equal, Order, Show}
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.show._

import shims._

sealed trait ScalarStage extends Product with Serializable

object ScalarStage extends ScalarStageInstances {

  /** Stages that refer to their input exactly once. */
  sealed trait Focused extends ScalarStage

  /**
   * Wraps the input to the stage in a singleton object at key `name`, adding
   * another layer of structure.
   */
  final case class Wrap(name: String) extends Focused

  /**
   *`Masks` represents the disjunction of the provided `masks`. An empty map indicates
   * that all values should be dropped. Removes all values which are not in one of the
   * path/type designations. The inner set is assumed to be non-empty.
   */
  final case class Mask(masks: Map[CPath, Set[ColumnType]]) extends Focused

  /**
   * Pivots the indices and keys out of arrays and objects, respectively.
   *
   * `idStatus` determines how values are returned:
   *   - `IncludeId` wraps the id/value pair in a two element array.
   *   - `IdOnly` returns the id unwrapped.
   *   - `ExcludeId` returns the value unwrapped.
   *
   * No values outside of the pivot locus should be retained.
   */
  final case class Pivot(status: IdStatus, structure: ColumnType.Vector)
      extends Focused

  /**
   * Extracts the value at `path`, eliminating all surrounding structure.
   */
  final case class Project(path: CPath) extends Focused

  /**
   * Applies the provided `List[Focused]` to each tupled `CPathField`
   * projection in the `Map` values. Then cartesians those results, wrapping them in the
   * `CPathField` map key.
   */
  final case class Cartesian(cartouches: Map[CPathField, (CPathField, List[Focused])])
      extends ScalarStage
}

sealed abstract class ScalarStageInstances {
  import ScalarStage._

  implicit def scalarStageEqual[S <: ScalarStage]: Equal[S] =
    Equal.equal(equal)

  implicit def scalarStageRenderTree[S <: ScalarStage]: RenderTree[S] =
    RenderTree.make(asRenderedTree)

  implicit def scalarStageShow[S <: ScalarStage]: Show[S] =
    RenderTree.toShow

  ////

  private implicit val columnTypeOrdering: Ordering[ColumnType] =
    Order[ColumnType].toScalaOrdering

  private def equal(x: ScalarStage, y: ScalarStage): Boolean =
    (x, y) match {
      case (Wrap(n1), Wrap(n2)) => n1 === n2
      case (Mask(m1), Mask(m2)) => m1 === m2
      case (Pivot(s1, t1), Pivot(s2, t2)) => s1 === s2 && t1 === t2
      case (Project(p1), Project(p2)) => p1 === p2
      case (Cartesian(c1), Cartesian(c2)) => c1 === c2
      case _ => false
    }

  private def asRenderedTree(ss: ScalarStage): RenderedTree =
    ss match {
      case Wrap(n) => Terminal(List("Wrap"), some(n))

      case Mask(m) =>
        NonTerminal(List("Mask"), none, m.toList map {
          case (p, types) if types === ColumnType.Top =>
            Terminal(List(s"Column[$p]"), some("⊤"))

          case (p, types) =>
            Terminal(List(s"Column[$p]"), some(types.toList.sorted.mkString(", ")))
        })

      case Pivot(s, t) => Terminal(List("Pivot"), some(s"${s.shows}, ${t.shows}"))

      case Project(p) => Terminal(List("Project"), some(p.shows))

      case Cartesian(cs) =>
        val stringified = cs map { case (t, (s, v)) => (t.toString, (s.toString, v)) }
        NonTerminal(List("Cartesian"), none, stringified.render.children)
    }
}
