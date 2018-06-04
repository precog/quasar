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

import slamdata.Predef.{Map => SMap, _}

import quasar.fp.symbolOrder
import quasar.contrib.iota.copkTraverse
import quasar.qscript.{construction, FreeMap => FM}

import matryoshka.BirecursiveT
import scalaz.{ICons, IList, INil, Monad, NonEmptyList, Scalaz, WriterT}, Scalaz._

/** Given a graph, source and targets vertices, unifies access to the target
  * values via `FreeMap`s over a common source.
  *
  * Returns the new source graph, a unary function to access the
  * original source value and a list of symbols tupled with unary
  * functions to access the values of each of the provided targets.
  */
final class UnifyTargets[T[_[_]]: BirecursiveT, F[_]: Monad] private (
    sourceName: String,
    targetPrefix: String,
    buildGraph: QScriptUniform[T, Symbol] => F[QSUGraph[T]],
  ) extends QSUTTypes[T] {

  import QScriptUniform.AutoJoin2

  def apply(graph: QSUGraph, source: Symbol, targets: NonEmptyList[Symbol])
      : F[(QSUGraph, FreeMap, NonEmptyList[(Symbol, FreeMap)])] = {

    val targetMap = targets map (t => (t, MappableRegion(_ === source, graph refocus t)))
    val targetExprs = targetMap.seconds map (_ map (_.root))

    val roots = targetExprs.traverse(_.traverse { g =>
      if (g === source)
        WriterT.writer((IList[Symbol](), source))
      else
        WriterT.writer((IList(g), g))
      }).written

    roots.distinct.zipWithIndex match {
      case INil() =>
        (graph refocus source,
          func.Hole,
          targetMap map { case (sym, fm) => (sym, fm.map(_.root) >> func.Hole) }).point[F]

      case ICons(h @ (head, _), tail) =>
        val targetAccesses = (h :: tail) map (_ map targetAccess)
        val accessIndex = SMap((source, sourceAccess) :: targetAccesses.toList : _*)

        val srcCombine =
          func.StaticMapS(
            sourceName -> func.LeftSide,
            targetName(0) -> func.RightSide)

        val autojoinedM =
          buildGraph(AutoJoin2(source, head, srcCombine)) flatMap { srcJoin =>
            tail.foldLeftM(srcJoin) { case (joins, (tgt, i)) =>
              val tgtCombine =
                func.ConcatMaps(
                  func.LeftSide,
                  func.MakeMapS(targetName(i), func.RightSide))

              buildGraph(AutoJoin2(joins.root, tgt, tgtCombine)) map (_ :++ joins)
            }
          }

        val targetMap0 = targetMap map { case (sym, fm) => (sym, fm map (_.root) >>= accessIndex) }

        autojoinedM map (g => (g :++ graph, sourceAccess, targetMap0))
    }
  }

  ////

  private val func = construction.Func[T]

  private val sourceAccess =
    func.ProjectKeyS(func.Hole, sourceName)

  private def targetAccess(idx: Int): FreeMap =
    func.ProjectKeyS(func.Hole, targetName(idx))

  private def targetName(idx: Int): String =
    s"${targetPrefix}_${idx}"
}

object UnifyTargets {
  def apply[T[_[_]]: BirecursiveT, F[_]: Monad](
      buildGraph: QScriptUniform[T, Symbol] => F[QSUGraph[T]])(
      graph: QSUGraph[T],
      source: Symbol,
      targets: NonEmptyList[Symbol])(
      sourceName: String,
      targetPrefix: String)
      : F[(QSUGraph[T], FM[T], NonEmptyList[(Symbol, FM[T])])] =
    new UnifyTargets[T, F](sourceName, targetPrefix, buildGraph).apply(graph, source, targets)
}
