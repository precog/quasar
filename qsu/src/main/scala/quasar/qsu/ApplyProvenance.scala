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

import quasar.fs.Planner.{PlannerErrorME, InternalError}
import quasar.contrib.scalaz.MonadState_
import quasar.ejson
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.ι
import quasar.qscript.{construction, ExcludeId, IdOnly, IdStatus, OnUndefined}

import matryoshka._
import matryoshka.implicits._
import monocle.macros.Lenses
import pathy.Path
import scalaz.{Applicative, Cord, Functor, IList, Monad, Show, StateT, ValidationNel}
import scalaz.Scalaz._

final class ApplyProvenance[T[_[_]]: BirecursiveT: EqualT: ShowT] private () extends QSUTTypes[T] {
  import ApplyProvenance._
  import QScriptUniform._
  import QSUGraph.Extractors

  type QSU[A] = QScriptUniform[A]

  type QAuthS[F[_]] = MonadState_[F, QAuth]
  def QAuthS[F[_]](implicit ev: QAuthS[F]): QAuthS[F] = ev

  private type V[F[_], A] = F[ValidationNel[Symbol, A]]
  implicit def V[F[_]: Applicative] = Applicative[F].compose[ValidationNel[Symbol, ?]]

  val dims = QProv[T]
  val func = construction.Func[T]
  val recFunc = construction.RecFunc[T]

  def apply[F[_]: Monad: PlannerErrorME](graph: QSUGraph): F[AuthenticatedQSU[T]] = {
    type X[A] = StateT[F, QAuth, A]

    val authGraph = graph.rewriteM[X] {
      case g @ Extractors.DimEdit(src, _) =>
        for {
          _ <- computeProvenance[X](g)
          _ <- QAuthS[X].modify(_.supplant(src.root, g.root))
        } yield g.overwriteAtRoot(src.unfold map (_.root))

      case g @ Extractors.Transpose(src, retain, rot) =>
        computeProvenance[X](g) as g.overwriteAtRoot {
          LeftShift(src.root, recFunc.Hole, retain.fold[IdStatus](IdOnly, ExcludeId), OnUndefined.Omit, RightTarget[T], rot)
        }

      case other =>
        computeProvenance[X](other) as other
    }

    authGraph.run(QAuth.empty[T]) map {
      case (qauth, graph) => AuthenticatedQSU(graph, qauth)
    }
  }

  def computeProvenance[F[_]: Monad: PlannerErrorME: QAuthS](g: QSUGraph): F[QDims] = {
    def flattened =
      s"${g.root} @ ${g.unfold.map(_.root).shows}"

    def unexpectedError: F[QDims] =
      PlannerErrorME[F].raiseError(
        InternalError(s"Encountered unexpected $flattened.", None))

    g.unfold match {
      case AutoJoin2(left, right, _) =>
        compute2[F](g, left, right)(dims.join(_, _))

      case AutoJoin3(left, center, right, _) =>
        compute3[F](g, left, center, right) { (l, c, r) =>
          dims.join(dims.join(l, c), r)
        }

      case QSAutoJoin(left, right, _, _) =>
        compute2[F](g, left, right)(dims.join(_, _))

      case DimEdit(src, DTrans.Squash()) =>
        compute1[F](g, src)(dims.squash)

      case DimEdit(src, DTrans.Group(k)) =>
        handleMissingDims(dimsFor[F](src)) flatMap { sdims =>
          val updated = dims.modifyIdentities(sdims)(IdAccess.groupKey modify {
            case (s, i) if s === src.root => (g.root, i)
            case other => other
          })

          val nextIdx = dims.nextGroupKeyIndex(g.root, updated)
          val idAccess = IdAccess.groupKey[dims.D](g.root, nextIdx)
          val nextDims = dims.swap(0, 1, dims.lshift(idAccess, updated))

          QAuthS[F].modify(
            _.addDims(g.root, nextDims)
              .duplicateGroupKeys(src.root, g.root)
              .addGroupKey(g.root, nextIdx, k))
            .as(nextDims)
        }

      case Distinct(src) =>
        compute1[F](g, src)(ι)

      case GroupBy(_, _) => unexpectedError

      case JoinSideRef(_) => unexpectedError

      case LeftShift(src, _, _, _, _, rot) =>
        val tid = IdAccess.identity[dims.D](g.root)
        compute1[F](g, src) { sdims =>
          rot match {
            case Rotation.ShiftMap   | Rotation.ShiftArray   => dims.lshift(tid, sdims)
            case Rotation.FlattenMap | Rotation.FlattenArray => dims.flatten(tid, sdims)
          }
        }

        case MultiLeftShift(src, shifts, _, _) =>
          val tid = IdAccess.identity[dims.D](g.root)
          compute1[F](g, src) { sdims =>
            IList.fromList(shifts).sortBy(_._3).foldRight(sdims) {
              case (shift, prv) => shift._3 match {
                case Rotation.ShiftMap   | Rotation.ShiftArray   => dims.lshift(tid, prv)
                case Rotation.FlattenMap | Rotation.FlattenArray => dims.flatten(tid, prv)
              }
            }
          }

      case LPFilter(_, _) => unexpectedError

      case LPJoin(_, _, _, _, _, _) => unexpectedError

      case LPReduce(src, _) =>
        compute1[F](g, src) { sdims =>
          dims.bucketAccess(g.root, dims.reduce(sdims))
        }

      case LPSort(_, _) => unexpectedError

      case QSFilter(src, _) =>
        compute1[F](g, src)(ι)

      case QSReduce(src, _, _, _) =>
        compute1[F](g, src) { sdims =>
          dims.bucketAccess(g.root, dims.reduce(sdims))
        }

      case QSSort(src, _, _) =>
        compute1[F](g, src)(ι)

      case Unary(_, _) => unexpectedError

      case Map(src, _) =>
        compute1[F](g, src)(ι)

      case Read(file) =>
        val rdims = dims.squash(segments(file).map(projStr).reverse)
        QAuthS[F].modify(_.addDims(g.root, rdims)) as rdims

      case Subset(from, _, count) =>
        compute2[F](g, from, count)(dims.join(_, _))

      case ThetaJoin(left, right, _, _, _) =>
        compute2[F](g, left, right)(dims.join(_, _))

      case Transpose(src, _, rot) =>
        val tid = IdAccess.identity[dims.D](g.root)
        compute1[F](g, src) { sdims =>
          rot match {
            case Rotation.ShiftMap   | Rotation.ShiftArray   => dims.lshift(tid, sdims)
            case Rotation.FlattenMap | Rotation.FlattenArray => dims.flatten(tid, sdims)
          }
        }

      case Union(left, right) =>
        compute2[F](g, left, right)(dims.union(_, _))

      case Unreferenced() =>
        QAuthS[F].modify(_.addDims(g.root, dims.empty)) as dims.empty
    }
  }

  ////

  private def compute1[F[_]: Monad: PlannerErrorME: QAuthS]
      (g: QSUGraph, src: QSUGraph)
      (f: QDims => QDims)
      : F[QDims] =
    handleMissingDims(dimsFor[F](src)) flatMap { sdims =>
      val gdims = f(sdims)
      QAuthS.modify(_.addDims(g.root, gdims)) as gdims
    }

  private def compute2[F[_]: Monad: PlannerErrorME: QAuthS]
      (g: QSUGraph, l: QSUGraph, r: QSUGraph)
      (f: (QDims, QDims) => QDims)
      : F[QDims] =
    handleMissingDims((dimsFor[F](l) |@| dimsFor(r))(f)) flatMap { ds =>
      QAuthS.modify(_.addDims(g.root, ds)) as ds
    }

  private def compute3[F[_]: Monad: PlannerErrorME: QAuthS]
      (g: QSUGraph, l: QSUGraph, c: QSUGraph, r: QSUGraph)
      (f: (QDims, QDims, QDims) => QDims)
      : F[QDims] =
    handleMissingDims((dimsFor[F](l) |@| dimsFor[F](c) |@| dimsFor[F](r))(f)) flatMap { ds =>
      QAuthS.modify(_.addDims(g.root, ds)) as ds
    }

  private def dimsFor[F[_]: Functor: QAuthS](g: QSUGraph): V[F, QDims] =
    QAuthS[F].gets(_ lookupDims g.root toSuccessNel g.root)

  private def handleMissingDims[F[_]: Monad: PlannerErrorME, A](v: V[F, A]): F[A] =
    (v: F[ValidationNel[Symbol, A]]).flatMap { v0 =>
      v0.map(_.point[F]) valueOr { syms =>
        PlannerErrorME[F].raiseError[A](
          InternalError(s"Dependent dimensions not found: ${syms.show}.", None))
      }
    }

  private def projStr(s: String): QProv.P[T] =
    dims.prov.proj(ejson.CommonEJson(ejson.Str[T[EJson]](s)).embed)

  private def segments(p: Path[_, _, _]): IList[String] = {
    val segs = Path.flatten[IList[String]](IList(), IList(), IList(), IList(_), IList(_), p)
    (segs.head :: segs.tail).join
  }
}

object ApplyProvenance {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: PlannerErrorME]
      (graph: QSUGraph[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("ApplyProvenance", new ApplyProvenance[T].apply[F](graph))

  def computeProvenance[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: PlannerErrorME: MonadState_[?[_], QAuth[T]]]
      (graph: QSUGraph[T])
      : F[QDims[T]] =
    new ApplyProvenance[T].computeProvenance[F](graph)

  @Lenses
  final case class AuthenticatedQSU[T[_[_]]](graph: QSUGraph[T], auth: QAuth[T])

  object AuthenticatedQSU {
    implicit def show[T[_[_]]: ShowT]: Show[AuthenticatedQSU[T]] =
      Show.show { case AuthenticatedQSU(g, d) =>
        Cord("AuthenticatedQSU {\n") ++
        g.show ++
        Cord("\n\n") ++
        d.show ++
        Cord("\n}")
      }
  }
}
