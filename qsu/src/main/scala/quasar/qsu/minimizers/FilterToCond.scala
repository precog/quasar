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

package quasar.qsu
package minimizers

import slamdata.Predef._

import quasar.RenderTreeT
import quasar.common.effect.NameGenerator
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{construction, HoleF, MonadPlannerErr, RecFreeS}, RecFreeS._
import quasar.qsu.{QScriptUniform => QSU}

import matryoshka.{delayEqual, BirecursiveT, EqualT, ShowT}
import matryoshka.data.free._

import scalaz.Monad
import scalaz.syntax.equal._
import scalaz.syntax.monad._

import scala.collection
import scala.sys

final class FilterToCond[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean = {
    val (filters, _) = candidates partition {
      case QSFilter(_, _) => true
      case _ => false
    }

    filters.nonEmpty // && notShiftedRead
  }

  def extract[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, ?[_]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case QSFilter(src, predicate) =>
      // this is where we do the actual rewriting
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        updateGraph[T, G](
          QSU.Map(
            src.root,
            recFunc.Cond(
              predicate.flatMap(_ => fm.asRec),
              fm.asRec,
              recFunc.Undefined))).map(_ :++ src)
      }

      Some((src, rebuild _))

    case qgraph =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        if (fm === HoleF[T]) {
          src.point[G]
        } else {
          // this case should never happen
          updateGraph[T, G](QSU.Map(src.root, fm.asRec)) map { rewritten =>
            rewritten :++ src
          }
        }
      }

      Some((qgraph, rebuild _))
  }

  def apply[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, ?[_]]](
      qgraph: QSUGraph,
      singleSource: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    val fms: Map[Int, RecFreeMap] = candidates.zipWithIndex.map({
      case (Map(parent, fm), i) if parent.root === singleSource.root =>
        i -> fm

      case (parent, i) if parent.root === singleSource.root =>
        i -> recFunc.Hole

      case _ =>
        sys.error("assertion error")
    })(collection.breakOut)

    val collapsed = fm.asRec.flatMap(fms)

    updateGraph[T, G](QSU.Map(singleSource.root, collapsed)) map { g =>
      val back = g :++ singleSource
      Some((back, back))
    }
  }
}

object FilterToCond {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: FilterToCond[T] =
    new FilterToCond[T]
}

