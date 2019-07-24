/*
 * Copyright 2014–2019 SlamData Inc.
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
import quasar.fp.symbolOrder
import quasar.fp.ski.ι
import quasar.qscript.{construction, Hole, HoleF, MonadPlannerErr, ReduceFunc, ReduceIndexF, SrcHole}
import ApplyProvenance.AuthenticatedQSU

import matryoshka._
import scalaz.{Equal, ISet, Monad, NonEmptyList, Scalaz, StateT}, Scalaz._

sealed abstract class ReifyBuckets[T[_[_]]: BirecursiveT: EqualT: ShowT] extends MraPhase[T] {
  import QSUGraph.Extractors._

  implicit def PEqual: Equal[P]

  lazy val B = Bucketing(qprov)
  val qsu = QScriptUniform.Optics[T]
  val func = construction.Func[T]

  import qprov.syntax._

  def apply[F[_]: Monad: NameGenerator: MonadPlannerErr](aqsu: AuthenticatedQSU[T, P])
      : F[AuthenticatedQSU[T, P]] = {

    type G[A] = StateT[StateT[F, RevIdx, ?], QAuth[P], A]

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
                      if (Access.value[Hole].isEmpty(access))
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

  private def bucketsFor[F[_]: Monad: MonadPlannerErr: MonadState_[?[_], QAuth[P]]]
      (vertex: Symbol)
      : F[(ISet[Symbol], List[FreeAccess[Hole]])] =
    for {
      qauth <- MonadState_[F, QAuth[P]].get

      vdims <- qauth.lookupDimsE[F](vertex)

      ids = B.buckets(vdims.reduce).toList

      res <- ids traverse {
        case id @ IdAccess.GroupKey(s, i) =>
          qauth.lookupGroupKeyE[F](s, i)
            .map(fm => (ISet.singleton(s), fm as Access.value[Hole](SrcHole)))

        case other =>
          (ISet.empty[Symbol], HoleF[T] as Access.id[Hole](other, SrcHole)).point[F]
      }
    } yield res.unfzip leftMap (_.foldMap(ι))

  private def buildGraph[F[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MonadState_[?[_], QAuth[P]]](
      node: QScriptUniform[Symbol])
      : F[QSUGraph] =
    for {
      newGraph <- QSUGraph.withName[T, F]("rbu")(node)
      _        <- ApplyProvenance.computeDims[T, F](qprov, newGraph)
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
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Monad: NameGenerator: MonadPlannerErr]
      (qp: QProv[T])(aqsu: AuthenticatedQSU[T, qp.P])(
      implicit P: Equal[qp.P])
      : F[AuthenticatedQSU[T, qp.P]] = {

    val rb = new ReifyBuckets[T] {
      val qprov: qp.type = qp
      val PEqual = P
    }

    taggedInternalError("ReifyBuckets", rb[F](aqsu))
  }
}
