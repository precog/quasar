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

import quasar.effect.NameGenerator
import quasar.fs.Planner.PlannerErrorME
import quasar.contrib.scalaz.MonadState_
import quasar.fp.symbolOrder
import quasar.fp.ski.ι
import quasar.qscript.{construction, Hole, HoleF, ReduceFunc, ReduceIndexF, SrcHole}
import ApplyProvenance.AuthenticatedQSU

import matryoshka._
import scalaz.{ISet, Monad, NonEmptyList, Scalaz, StateT}, Scalaz._

final class ReifyBuckets[T[_[_]]: BirecursiveT: EqualT: ShowT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  val prov = QProv[T]
  val qsu  = QScriptUniform.Optics[T]
  val func = construction.Func[T]

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] = {

    type G[A] = StateT[StateT[F, RevIdx, ?], QAuth, A]

    val bucketsReified = aqsu.graph.rewriteM[G] {
      case g @ LPReduce(source, reduce) =>
        for {
          res <- bucketsFor[G](source.root)

          (srcs, buckets0) = res

          reifiedG <- srcs.findMin match {
            case Some(sym) =>
              UnifyTargets[T, G](buildGraph[G](_))(g, sym, NonEmptyList(source.root))(GroupedKey, ReduceExprKey) map {
                case (newSrc, original, reduceExpr) =>
                  val buckets =
                    buckets0.map(_ flatMap { access =>
                      if (Access.valueHole.isEmpty(access))
                        func.Hole as access
                      else
                        original as access
                    })

                  g.overwriteAtRoot(mkReduce(newSrc.root, buckets, reduce as reduceExpr.seconds.head)) :++ newSrc
              }

            case None =>
              g.overwriteAtRoot(mkReduce(source.root, buckets0, reduce as HoleF)).point[G]
          }
        } yield reifiedG

      case g @ QSSort(source, Nil, keys) =>
        val src = source.root

        bucketsFor[G](src) map { case (_, buckets) =>
          g.overwriteAtRoot(qsu.qsSort(src, buckets, keys))
        }
    }

    bucketsReified.run(aqsu.auth).eval(aqsu.graph.generateRevIndex) map {
      case (auth, graph) => ApplyProvenance.AuthenticatedQSU(graph, auth)
    }
  }

  ////

  private val GroupedKey = "grouped"
  private val ReduceExprKey = "reduce_expr"

  private def bucketsFor[F[_]: Monad: PlannerErrorME: MonadState_[?[_], QAuth]]
      (vertex: Symbol)
      : F[(ISet[Symbol], List[FreeAccess[Hole]])] =
    for {
      qauth <- MonadState_[F, QAuth].get

      vdims <- qauth.lookupDimsE[F](vertex)

      ids = prov.buckets(prov.reduce(vdims)).toList

      res <- ids traverse {
        case id @ IdAccess.GroupKey(s, i) =>
          qauth.lookupGroupKeyE[F](s, i)
            .map(fm => (ISet.singleton(s), fm as Access.value[prov.D, Hole](SrcHole)))

        case other =>
          (ISet.empty[Symbol], HoleF[T] as Access.id[prov.D, Hole](other, SrcHole)).point[F]
      }
    } yield res.unfzip leftMap (_.foldMap(ι))

  private def buildGraph[F[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM: MonadState_[?[_], QAuth]](
      node: QScriptUniform[Symbol])
      : F[QSUGraph] =
    for {
      newGraph <- QSUGraph.withName[T, F]("rbu")(node)
      _        <- ApplyProvenance.computeProvenance[T, F](newGraph)
    } yield newGraph

  private def mkReduce[A](
      src: A,
      buckets: List[FreeAccess[Hole]],
      reducer: ReduceFunc[FreeMap])
      : QScriptUniform[A] =
    qsu.qsReduce(
      src,
      buckets,
      List(reducer),
      ReduceIndexF[T](0.right))
}

object ReifyBuckets {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Monad: NameGenerator: PlannerErrorME]
      (aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("ReifyBuckets", new ReifyBuckets[T].apply[F](aqsu))
}
