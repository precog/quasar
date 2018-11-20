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

import quasar.IdStatus, IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.contrib.matryoshka.ginterpret
import quasar.contrib.scalaz.MonadState_
import quasar.ejson
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.{ι, κ}
import quasar.qscript.{
  construction,
  Center,
  ExtractFunc,
  FreeMapA,
  LeftSide,
  LeftSide3,
  MapFuncsCore,
  MapFuncsDerived,
  MFC,
  MFD,
  MonadPlannerErr,
  OnUndefined,
  PlannerError,
  RightSide,
  RightSide3
}
import quasar.qscript.provenance.Dimensions

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import monocle.macros.Lenses
import monocle.Traversal
import pathy.Path
import scalaz.{-\/, \/-, Applicative, Cord, Foldable, Functor, IList, Monad, NonEmptyList, Show, StateT, ValidationNel}
import scalaz.Scalaz._
import scalaz.Tags.{Disjunction => Disj}

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

  import dims.prov.implicits._

  def apply[F[_]: Monad: MonadPlannerErr](graph: QSUGraph): F[AuthenticatedQSU[T]] = {
    type X[A] = StateT[F, QAuth, A]

    val authGraph = graph.rewriteM[X] {
      case g @ Extractors.DimEdit(src, _) =>
        for {
          _ <- computeDims[X](g)
          _ <- QAuthS[X].modify(_.supplant(src.root, g.root))
        } yield g.overwriteAtRoot(src.unfold map (_.root))

      case g @ Extractors.Transpose(src, retain, rot) =>
        computeDims[X](g) as g.overwriteAtRoot {
          LeftShift(src.root, recFunc.Hole, retain.fold[IdStatus](IdOnly, ExcludeId), OnUndefined.Omit, RightTarget[T], rot)
        }

      case other =>
        computeDims[X](other) as other
    }

    authGraph.run(QAuth.empty[T]) map {
      case (qauth, graph) => AuthenticatedQSU(graph, qauth)
    }
  }

  def computeDims[F[_]: Monad: MonadPlannerErr: QAuthS](g: QSUGraph): F[QDims] = {
    def flattened =
      s"${g.root} @ ${g.unfold.map(_.root).shows}"

    def unexpectedError: F[QDims] =
      MonadPlannerErr[F].raiseError(
        PlannerError.InternalError(s"Encountered unexpected $flattened.", None))

    g.unfold match {
      case AutoJoin2(left, right, combine) =>
        computeJoin2[F](g, left, right, combine)

      case AutoJoin3(left, center, right, combine) =>
        compute3[F](g, left, center, right) { (l, c, r) =>
          val refs = Foldable[FreeMapA].foldMap(combine) {
            case LeftSide3 => (true.disjunction, false.disjunction, false.disjunction)
            case Center => (false.disjunction, true.disjunction, false.disjunction)
            case RightSide3 => (false.disjunction, false.disjunction, true.disjunction)
          }

          val joined = dims.join(l, dims.join(c, r))

          val maybeDims = refs match {
            case (Disj(false), Disj(false), Disj(false)) =>
              some(joined)

            case (Disj(true), Disj(false), Disj(false)) =>
              computeFuncDims(combine)(κ(joined))

            case (Disj(false), Disj(true), Disj(false)) =>
              computeFuncDims(combine)(κ(joined))

            case (Disj(false), Disj(false), Disj(true)) =>
              computeFuncDims(combine)(κ(joined))

            case (Disj(true), Disj(true), Disj(false)) =>
              computeFuncDims(combine) {
                case LeftSide3 => l
                case Center => dims.join(c, r)
                case RightSide3 => dims.empty
              }

            case (Disj(true), Disj(false), Disj(true)) =>
              computeFuncDims(combine) {
                case LeftSide3 => l
                case Center => dims.empty
                case RightSide3 => dims.join(c, r)
              }

            case (Disj(false), Disj(true), Disj(true)) =>
              computeFuncDims(combine) {
                case LeftSide3 => dims.empty
                case Center => dims.join(l, c)
                case RightSide3 => r
              }

            case (Disj(true), Disj(true), Disj(true)) =>
              computeFuncDims(combine) {
                case LeftSide3 => l
                case Center => c
                case RightSide3 => r
              }
          }

          maybeDims getOrElse joined
        }

      case QSAutoJoin(left, right, _, combine) =>
        computeJoin2[F](g, left, right, combine)

      case DimEdit(src, DTrans.Squash()) =>
        compute1[F](g, src)(dims.squash)

      case DimEdit(src, DTrans.Group(k)) =>
        handleMissingDims(dimsFor[F](src)) flatMap { sdims =>
          val updated = dims.modifyIdentities(sdims)(IdAccess.groupKey modify {
            case (s, i) if s === src.root => (g.root, i)
            case other => other
          })

          val nextIdx = dims.nextGroupKeyIndex(g.root, updated)
          val idAccess = IdAccess.groupKey(g.root, nextIdx)
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

      case LeftShift(src, struct, idStatus, _, repair, rot) =>
        val tid = IdAccess.identity(g.root)
        compute1[F](g, src) { sdims =>
          val structDims =
            computeFuncDims(struct.linearize)(κ(sdims)) getOrElse sdims

          val shiftedDims = rot match {
            case Rotation.ShiftMap | Rotation.ShiftArray =>
              dims.lshift(tid, structDims)

            case Rotation.FlattenMap | Rotation.FlattenArray =>
              dims.flatten(tid, structDims)
          }

          val repairDims = computeFuncDims(repair) {
            case ShiftTarget.LeftTarget =>
              shiftedDims

            case ShiftTarget.RightTarget =>
              applyIdStatus(idStatus, shiftedDims)

            case ShiftTarget.AccessLeftTarget(Access.Value(_)) =>
              shiftedDims

            case ShiftTarget.AccessLeftTarget(_) =>
              dims.empty
          }

          repairDims getOrElse shiftedDims
        }

      case MultiLeftShift(src, shifts, _, repair) =>
        val tid = IdAccess.identity(g.root)
        compute1[F](g, src) { sdims =>
          val shiftsDims = shifts map {
            case (struct, idStatus, rot) =>
              val structDims =
                computeFuncDims(struct)(κ(sdims)) getOrElse sdims

              rot match {
                case Rotation.ShiftMap | Rotation.ShiftArray =>
                  dims.lshift(tid, structDims)

                case Rotation.FlattenMap | Rotation.FlattenArray =>
                  dims.flatten(tid, structDims)
              }
          }

          val shiftedSrcDims =
            shiftsDims.foldMapRight1Opt(ι)(dims.join(_, _)).getOrElse(sdims)

          val repairDims = computeFuncDims(repair) {
            case \/-(i) => applyIdStatus(shifts(i)._2, shiftedSrcDims)
            case -\/(Access.Value(_)) => shiftedSrcDims
            case -\/(_) => dims.empty
          }

          repairDims getOrElse shiftedSrcDims
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

      case QSReduce(src, _, _, repair) =>
        compute1[F](g, src) { sdims =>
          val rdims = dims.bucketAccess(g.root, dims.reduce(sdims))
          computeFuncDims(repair)(κ(rdims)) getOrElse rdims
        }

      case QSSort(src, _, _) =>
        compute1[F](g, src)(ι)

      case Unary(_, _) => unexpectedError

      case Map(src, fm) =>
        compute1[F](g, src) { sdims =>
          val sigil = dims.prov.fresh()
          val fdims = computeFuncDims(fm.linearize)(κ(Dimensions.origin(sigil)))
          val fhead = fdims.flatMap(Dimensions.join[dims.P].headOption)

          fhead.fold(sdims)(h =>
            Dimensions.join[dims.P]
              .modify(j => joinT.modify(_.transApoT(substitute(sigil, j.head)))(h) :::> j.tail)
              .apply(sdims))
        }

      case Read(file, idStatus) =>
        val tid = IdAccess.identity(g.root)

        val rdims = segments(file).toNel.fold(Dimensions.empty[dims.P]) { ss =>
          dims.squash(Dimensions.origin1(ss.map(projPathSegment).reverse))
        }

        val sdims = applyIdStatus(idStatus, dims.lshift(tid, rdims))

        QAuthS[F].modify(_.addDims(g.root, sdims)) as sdims

      case Subset(from, _, count) =>
        compute2[F](g, from, count)(dims.join(_, _))

      case ThetaJoin(left, right, _, _, combine) =>
        computeJoin2[F](g, left, right, combine)

      case Transpose(src, _, rot) =>
        val tid = IdAccess.identity(g.root)
        compute1[F](g, src) { sdims =>
          rot match {
            case Rotation.ShiftMap | Rotation.ShiftArray =>
              dims.lshift(tid, sdims)

            case Rotation.FlattenMap | Rotation.FlattenArray =>
              dims.flatten(tid, sdims)
          }
        }

      case Union(left, right) =>
        compute2[F](g, left, right)(dims.union(_, _))

      case Unreferenced() =>
        QAuthS[F].modify(_.addDims(g.root, dims.empty)) as dims.empty
    }
  }

  def computeFuncDims[A](fm: FreeMapA[A])(f: A => QDims): Option[QDims] =
    some(fm.para(ginterpret(f, computeFuncProvenanceƒ[A]))).filter(_.nonEmpty)

  def computeFuncProvenanceƒ[A]: GAlgebra[(FreeMapA[A], ?), MapFunc, QDims] = {
    case MFC(MapFuncsCore.ConcatArrays((_, l), (_, r))) =>
      // FIXME{ch1487}: Adjust rhs based on knowledge of lhs.
      dims.join(l, r)

    case MFC(MapFuncsCore.ConcatMaps((_, l), (_, r))) =>
      dims.join(l, r)

    case MFC(MapFuncsCore.Cond(_, (_, t), (_, f))) =>
      dims.union(t, f)

    case MFC(MapFuncsCore.DeleteKey((_, d), _)) =>
      d

    case MFC(MapFuncsCore.Guard(_, _, (_, a), (_, b))) =>
      dims.union(a, b)

    case MFC(MapFuncsCore.IfUndefined((_, a), (_, b))) =>
      dims.union(a, b)

    case MFC(MapFuncsCore.MakeArray((_, d))) =>
      dims.injectStatic(EJson.int[T[EJson]](0), d)

    case MFC(MapFuncsCore.MakeMap((ExtractFunc(MapFuncsCore.Constant(k)), _), (_, v))) =>
      dims.injectStatic(k, v)

    case MFC(MapFuncsCore.MakeMap(_, (_, v))) =>
      dims.injectDynamic(v)

    case MFC(MapFuncsCore.ProjectIndex((_, a), (ExtractFunc(MapFuncsCore.Constant(i)), _))) =>
      dims.projectStatic(i, a)

    case MFC(MapFuncsCore.ProjectIndex((_, a), _)) =>
      dims.projectDynamic(a)

    case MFC(MapFuncsCore.ProjectKey((_, m), (ExtractFunc(MapFuncsCore.Constant(k)), _))) =>
      dims.projectStatic(k, m)

    case MFC(MapFuncsCore.ProjectKey((_, m), _)) =>
      dims.projectDynamic(m)

    case MFD(MapFuncsDerived.Typecheck((_, d), _)) =>
      d

    case _ => dims.empty
  }

  ////

  private val joinT: Traversal[NonEmptyList[dims.P], dims.P] =
    Traversal.fromTraverse

  private def compute1[F[_]: Monad: MonadPlannerErr: QAuthS]
      (g: QSUGraph, src: QSUGraph)
      (f: QDims => QDims)
      : F[QDims] =
    handleMissingDims(dimsFor[F](src)) flatMap { sdims =>
      val gdims = f(sdims)
      QAuthS.modify(_.addDims(g.root, gdims)) as gdims
    }

  private def compute2[F[_]: Monad: MonadPlannerErr: QAuthS]
      (g: QSUGraph, l: QSUGraph, r: QSUGraph)
      (f: (QDims, QDims) => QDims)
      : F[QDims] =
    handleMissingDims((dimsFor[F](l) |@| dimsFor(r))(f)) flatMap { ds =>
      QAuthS.modify(_.addDims(g.root, ds)) as ds
    }

  private def compute3[F[_]: Monad: MonadPlannerErr: QAuthS]
      (g: QSUGraph, l: QSUGraph, c: QSUGraph, r: QSUGraph)
      (f: (QDims, QDims, QDims) => QDims)
      : F[QDims] =
    handleMissingDims((dimsFor[F](l) |@| dimsFor[F](c) |@| dimsFor[F](r))(f)) flatMap { ds =>
      QAuthS.modify(_.addDims(g.root, ds)) as ds
    }

  private def computeJoin2[F[_]: Monad: MonadPlannerErr: QAuthS](
      g: QSUGraph,
      left: QSUGraph,
      right: QSUGraph,
      jf: JoinFunc)
      : F[QDims] =
    compute2[F](g, left, right) { (l, r) =>
      val refs = Foldable[FreeMapA].foldMap(jf)(Set(_))
      val joined = dims.join(l, r)

      val maybeDims =
        if (refs.size === 0)
          some(joined)
        else if (refs.size === 1)
          computeFuncDims(jf)(κ(joined))
        else
          computeFuncDims(jf) {
            case LeftSide => l
            case RightSide => r
          }

      maybeDims getOrElse joined
    }

  private def dimsFor[F[_]: Functor: QAuthS](g: QSUGraph): V[F, QDims] =
    QAuthS[F].gets(_ lookupDims g.root toSuccessNel g.root)

  private def handleMissingDims[F[_]: Monad: MonadPlannerErr, A](v: V[F, A]): F[A] =
    (v: F[ValidationNel[Symbol, A]]).flatMap { v0 =>
      v0.map(_.point[F]) valueOr { syms =>
        MonadPlannerErr[F].raiseError[A](
          PlannerError.InternalError(s"Dependent dimensions not found: ${syms.show}.", None))
      }
    }

  private def projPathSegment(s: String): QProv.P[T] =
    dims.prov.prjPath(ejson.CommonEJson(ejson.Str[T[EJson]](s)).embed)

  private def segments(p: Path[_, _, _]): IList[String] = {
    val segs = Path.flatten[IList[String]](IList(), IList(), IList(), IList(_), IList(_), p)
    (segs.head :: segs.tail).join
  }
}

object ApplyProvenance {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: MonadPlannerErr]
      (graph: QSUGraph[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("ApplyProvenance", new ApplyProvenance[T].apply[F](graph))

  def computeDims[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: MonadPlannerErr: MonadState_[?[_], QAuth[T]]]
      (graph: QSUGraph[T])
      : F[QDims[T]] =
    new ApplyProvenance[T].computeDims[F](graph)

  def computeFuncDims[
      T[_[_]]: BirecursiveT: EqualT: ShowT, A](
      fm: FreeMapA[T, A])(
      f: A => QDims[T])
      : Option[QDims[T]] =
    new ApplyProvenance[T].computeFuncDims(fm)(f)

  def applyIdStatus[T[_[_]]: BirecursiveT: EqualT](
      status: IdStatus, qdims: QDims[T])
      : QDims[T] =
    status match {
      case IncludeId =>
        val dims = QProv[T]
        dims.join(
          dims.injectStatic(EJson.int[T[EJson]](0), qdims),
          dims.injectStatic(EJson.int[T[EJson]](1), qdims))

      case _ => qdims
    }

  @Lenses
  final case class AuthenticatedQSU[T[_[_]]](graph: QSUGraph[T], auth: QAuth[T])

  object AuthenticatedQSU {
    implicit def show[T[_[_]]: ShowT]: Show[AuthenticatedQSU[T]] =
      Show.show { case AuthenticatedQSU(g, d) =>
        Cord("AuthenticatedQSU {\n") ++
        g.show ++
        Cord("\n\n") ++
        d.filterVertices(g.foldMapDown(sg => Set(sg.root))).show ++
        Cord("\n}")
      }
  }
}
