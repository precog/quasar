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

package quasar.fs.mount

import quasar.Predef._
import quasar._
import quasar.fp._
import quasar.fs._

import matryoshka._, FunctorT.ops._
import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

/** A collection of views, each a query mapped to certain (file) path, which may
  * refer to each other and to concrete files.
  */
final case class Views(map: Map[AFile, Fix[LogicalPlan]]) {
  def add(f: AFile, lp: Fix[LogicalPlan]): Views =
    Views(map + (f -> lp))

  def remove(f: AFile): Views =
    Views(map - f)

  def contains(p: AFile): Boolean = map.contains(p)

  /** Enumerate view files and view ancestor directories at a particular location. */
  def ls(dir: ADir): Set[PathSegment] =
    map.keySet.foldMap(_.relativeTo(dir).flatMap(firstSegmentName).toSet)

  /** Resolve a path to the query for the view found there if any. */
  def lookup(p: AFile): Option[Fix[LogicalPlan]] =
    map.get(p).map(lp => rewrite0(absolutize(lp, fileParent(p)), Set(p)))

  /** Resolve view references within a query. */
  def rewrite(lp: Fix[LogicalPlan]): Fix[LogicalPlan] = rewrite0(lp, Set())

  private def rewrite0(lp: Fix[LogicalPlan], expanded: Set[AFile]): Fix[LogicalPlan] = {
    lp.transCata(once {
      case LogicalPlan.ReadF(p) =>
        refineTypeAbs(p).swap.toOption.filterNot(expanded contains _).flatMap { file =>
          map.get(file).map { viewLp =>
            val q = absolutize(viewLp, fileParent(file))
            rewrite0(q, expanded + file).unFix
          }
        }
      case _ => None
    })
  }

  /** Rewrite relative paths to be based on the given dir. */
  private def absolutize(lp: Fix[LogicalPlan], dir: ADir): Fix[LogicalPlan] =
    lp.transCata {
      case read @ LogicalPlan.ReadF(p) =>
        refineTypeAbs(p).fold(κ(read), rel => LogicalPlan.ReadF(dir </> rel))
      case t => t
    }
}

object Views {
  def empty: Views = Views(Map.empty)
}
