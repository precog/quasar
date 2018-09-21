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

import slamdata.Predef._

import quasar.common.effect.NameGenerator
import quasar.contrib.scalaz.MonadState_
import quasar.qscript.MonadPlannerErr
import quasar.qsu.{QScriptUniform => QSU}

import scalaz.{Monad, Scalaz, StateT}, Scalaz._

import scala.collection.immutable.{Map => SMap}

final class PruneSymmetricDimEdits[T[_[_]]] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  private type RootsMemo = SMap[Symbol, Set[QSUGraph]]

  def apply[F[_]: Monad: NameGenerator: MonadPlannerErr](g: QSUGraph): F[QSUGraph] = {
    // we need to memoize to avoid O(n^2) retraversals
    type G[A] = StateT[StateT[F, RootsMemo, ?], QSUGraph.RevIdx[T], A]

    val backM = g rewriteM[G] {
      case g @ (AutoJoin2(_, _, _) | AutoJoin3(_, _, _, _)) =>
        provRoots[G](g) flatMap { rootsS =>
          val roots = rootsS.toList

          val shouldPrune = roots forall {
            case DimEdit(_, QSU.DTrans.Squash()) => true
            case _ => false
          }

          if (shouldPrune) {
            val tip = g.unfold

            for {
              back <- tip traverse { sg =>
                roots.foldLeftM(sg) {
                  case (g, target @ DimEdit(parent, _)) =>
                    g.replaceWithRename[G](target.root, parent.root)

                  case (g, _ ) => g.point[G]
                }
              }

              gPat = g.unfold.map(_.root)
              backPat = back.map(_.root)
              _ <- MonadState_[G, RevIdx].modify(_ - gPat + (backPat -> g.root))

              _ <- MonadState_[G, RootsMemo].put(SMap())  // wipe the roots memo if we rewrite the graph
            } yield QSUGraph.refold(g.root, back)

          } else {
            g.point[G]
          }
        }
    }

    backM.eval(g.generateRevIndex).eval(SMap())
  }

  // does not return Unreferenced()
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def provRoots[
      G[_]: Monad: MonadState_[?[_], RootsMemo]](
      g: QSUGraph)
      : G[Set[QSUGraph]] = stateMemo(g) {

    case Distinct(parent) => provRoots[G](parent)
    case QSFilter(parent, _) => provRoots[G](parent)
    case QSSort(parent, _, _) => provRoots[G](parent)
    case Map(parent, _) => provRoots[G](parent)

    case AutoJoin2(left, right, _) =>
      (provRoots[G](left) |@| provRoots[G](right))(_ ++ _)

    case AutoJoin3(left, center, right, _) =>
      (provRoots[G](left) |@| provRoots[G](center) |@| provRoots[G](right))(_ ++ _ ++ _)

    case Unreferenced() => Set().point[G]

    case g => Set(g).point[G]
  }

  private def stateMemo[
      G[_]: Monad: MonadState_[?[_], RootsMemo]](
      g: QSUGraph)(
      f: QSUGraph => G[Set[QSUGraph]])
      : G[Set[QSUGraph]] = {

    for {
      table <- MonadState_[G, RootsMemo].get

      back <- table.get(g.root) match {
        case Some(results) =>
          results.point[G]

        case None =>
          for {
            results <- f(g)
            _ <- MonadState_[G, RootsMemo].put(table + (g.root -> results))
          } yield results
      }
    } yield back
  }
}

object PruneSymmetricDimEdits {
  def apply[
      T[_[_]],
      F[_]: Monad: NameGenerator: MonadPlannerErr]
      (graph: QSUGraph[T])
      : F[QSUGraph[T]] =
    taggedInternalError("PruneSymmetricDimEdits", new PruneSymmetricDimEdits[T].apply[F](graph))
}
