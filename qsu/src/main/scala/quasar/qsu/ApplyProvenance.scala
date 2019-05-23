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
  JoinSide,
  LeftSide,
  LeftSide3,
  MapFuncsCore,
  MFC,
  MonadPlannerErr,
  OnUndefined,
  PlannerError,
  RightSide,
  RightSide3
}

import cats.data.NonEmptyList

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._

import monocle.macros.Lenses

import pathy.Path

import scalaz.{-\/, \/-, Applicative, Cord, Equal, Free, Functor, IList, Monad, Show, StateT, ValidationNel}
import scalaz.Scalaz._

import shims.{eqToScalaz, equalToCats}

sealed abstract class ApplyProvenance[T[_[_]]: BirecursiveT: EqualT: ShowT] extends MraPhase[T] {
  import ApplyProvenance._
  import QScriptUniform._
  import QSUGraph.Extractors

  implicit def PEqual: Equal[P]

  type QSU[A] = QScriptUniform[A]

  type QAuthS[F[_]] = MonadState_[F, QAuth[P]]
  def QAuthS[F[_]](implicit ev: QAuthS[F]): QAuthS[F] = ev

  private type V[F[_], A] = F[ValidationNel[Symbol, A]]
  implicit def V[F[_]: Applicative] = Applicative[F].compose[ValidationNel[Symbol, ?]]

  private lazy val B = Bucketing(qprov)
  private val ejs = ejson.Fixed[T[EJson]]
  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  import qprov.syntax._

  def apply[F[_]: Monad: MonadPlannerErr](graph: QSUGraph): F[AuthenticatedQSU[T, P]] = {
    type X[A] = StateT[F, QAuth[P], A]

    val authGraph = graph.rewriteM[X] {
      case g @ Extractors.DimEdit(src, _) =>
        for {
          _ <- computeDims[X](g)
        } yield g.overwriteAtRoot(src.unfold map (_.root))

      case g @ Extractors.Transpose(src, retain, rot) =>
        computeDims[X](g) as g.overwriteAtRoot {
          LeftShift(src.root, recFunc.Hole, retain.fold[IdStatus](IdOnly, ExcludeId), OnUndefined.Omit, RightTarget[T], rot)
        }

      case other =>
        computeDims[X](other) as other
    }

    authGraph.run(QAuth.empty[T, P]) map {
      case (qauth, graph) => AuthenticatedQSU(graph, qauth)
    }
  }

  def computeDims[F[_]: Monad: MonadPlannerErr: QAuthS](g: QSUGraph): F[P] = {
    def flattened =
      s"${g.root} @ ${g.unfold.map(_.root).shows}"

    def unexpectedError: F[P] =
      MonadPlannerErr[F].raiseError(
        PlannerError.InternalError(s"Encountered unexpected $flattened.", None))

    g.unfold match {
      case AutoJoin2(left, right, combine) =>
        computeJoin2[F](g, left, right, combine)

      case AutoJoin3(left, center, right, combine) =>
        compute3[F](g, left, center, right) { (l, c, r) =>
          if (combine.empty)
            l ∧ c ∧ r
          else
            applyFuncProvenanceN(combine map {
              case LeftSide3 => 0
              case Center => 1
              case RightSide3 => 2
            })(l, c, r)
        }

      case QSAutoJoin(left, right, _, combine) =>
        computeJoin2[F](g, left, right, combine)

      case DimEdit(src, DTrans.Squash()) =>
        compute1[F](g, src)(_.squash)

      case DimEdit(src, DTrans.Group(k)) =>
        handleMissingDims(dimsFor[F](src)) flatMap { sdims =>
          val updated = B.rename(src.root, g.root, sdims)
          val nextIdx = B.nextGroupKeyIndex(g.root, updated)
          val idAccess = IdAccess.groupKey(g.root, nextIdx)
          val nextDims = updated.inflateSubmerge(idAccess, IdType.Expr)

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
            if (struct.empty)
              sdims
            else
              applyFuncProvenance(struct.linearize, sdims)

          val shiftedDims = rot match {
            case Rotation.ShiftMap | Rotation.ShiftArray =>
              structDims.inflateExtend(tid, IdType.fromRotation(rot))

            case Rotation.FlattenMap | Rotation.FlattenArray =>
              structDims.inflateConjoin(tid, IdType.fromRotation(rot))
          }

          if (repair.empty)
            shiftedDims
          else
            applyFuncProvenanceN(repair flatMap {
              case ShiftTarget.LeftTarget =>
                Free.pure(0)

              case ShiftTarget.AccessLeftTarget(Access.Value(_)) =>
                Free.pure(0)

              case ShiftTarget.RightTarget =>
                Free.pure(1)

              case _ =>
                func.Undefined
            })(sdims, applyIdStatus(idStatus, shiftedDims))
        }

      // FIXME: This is incorrect, we need an indexed inflate identity to
      //        properly compute this.
      case MultiLeftShift(src, shifts, _, repair) =>
        val tid = IdAccess.identity(g.root)

        compute1[F](g, src) { sdims =>
          val shiftsDims = shifts map {
            case (struct, idStatus, rot) =>
              val structDims =
                if (struct.empty)
                  sdims
                else
                  applyFuncProvenance(struct, sdims)

              rot match {
                case Rotation.ShiftMap | Rotation.ShiftArray =>
                  structDims.inflateExtend(tid, IdType.fromRotation(rot))

                case Rotation.FlattenMap | Rotation.FlattenArray =>
                  structDims.inflateConjoin(tid, IdType.fromRotation(rot))
              }
          }

          val shiftIdDims = shiftsDims.zipWithIndex map {
            case (d, i) => applyIdStatus(shifts(i)._2, d)
          }

          if (repair.empty)
            shiftsDims.foldLeft(qprov.empty)(_ ∧ _)
          else
            applyFuncProvenanceN(repair flatMap {
              case -\/(Access.Value(_)) => Free.pure(0)
              case \/-(i) => Free.pure(i + 1)
              case -\/(_) => func.Undefined
            })(sdims, shiftIdDims: _*)
        }

      case LPFilter(_, _) => unexpectedError

      case LPJoin(_, _, _, _, _, _) => unexpectedError

      case LPReduce(src, _) =>
        compute1[F](g, src) { sdims =>
          B.bucketAccess(g.root, sdims.reduce)
        }

      case LPSort(_, _) => unexpectedError

      case QSFilter(src, _) =>
        compute1[F](g, src)(ι)

      case QSReduce(src, _, _, repair) =>
        compute1[F](g, src) { sdims =>
          val rdims = B.bucketAccess(g.root, sdims.reduce)

          if (repair.empty)
            rdims
          else
            applyFuncProvenance(repair, rdims)
        }

      case QSSort(src, _, _) =>
        compute1[F](g, src)(ι)

      case Unary(_, _) => unexpectedError

      case Map(src, fm) =>
        compute1[F](g, src) { sdims =>
          if (fm.empty)
            sdims
          else
            applyFuncProvenance(fm.linearize, sdims)
        }

      case Read(file, idStatus) =>
        val tid = IdAccess.identity(g.root)
        val rdims = segments(file).foldLeft(qprov.empty)(projPathSegment)
        val sdims = applyIdStatus(idStatus, rdims.inflateExtend(tid, IdType.Dataset))

        QAuthS[F].modify(_.addDims(g.root, sdims)) as sdims

      case Subset(from, _, count) =>
        compute2[F](g, from, count)(_ ∧ _)

      case ThetaJoin(left, right, _, _, combine) =>
        val refs = combine.foldLeft(Set[JoinSide]())(_ + _)

        compute2[F](g, left, right) { (l, r) =>
          if (refs.size === 0)
            l ∧ r
          else if (refs.size === 1)
            applyFuncProvenance(combine, l ∧ r)
          else
            applyFuncProvenanceN(combine map {
              case LeftSide => 0
              case RightSide => 1
            })(l, r)
        }

      case Transpose(src, _, rot) =>
        val tid = IdAccess.identity(g.root)

        compute1[F](g, src) { sdims =>
          rot match {
            case Rotation.ShiftMap | Rotation.ShiftArray =>
              sdims.inflateExtend(tid, IdType.fromRotation(rot))

            case Rotation.FlattenMap | Rotation.FlattenArray =>
              sdims.inflateConjoin(tid, IdType.fromRotation(rot))
          }
        }

      case Union(left, right) =>
        compute2[F](g, left, right)(_ ∨ _)

      case Unreferenced() =>
        QAuthS[F].modify(_.addDims(g.root, qprov.empty)) as qprov.empty
    }
  }

  def computeFuncDims[A](fm: FreeMapA[A])(f: A => P): Option[P] =
    some(computeFuncProvenance(fm)(f)).filterNot(_.isEmpty)

  def computeFuncProvenanceƒ[A]: GAlgebra[(FreeMapA[A], ?), MapFunc, P] = {
    case MFC(MapFuncsCore.ConcatArrays((_, l), (_, r))) =>
      // FIXME{ch1487}: Adjust rhs based on knowledge of lhs.
      l ∧ r

    case MFC(MapFuncsCore.MakeArray((_, d))) =>
      d.injectStatic(EJson.int(0), IdType.Array)

    case MFC(MapFuncsCore.MakeMap((ExtractFunc(MapFuncsCore.Constant(k)), _), (_, v))) =>
      v.injectStatic(k, IdType.Map)

    case MFC(MapFuncsCore.MakeMap((_, k), (_, v))) =>
      (k ∧ v).injectDynamic

    case MFC(MapFuncsCore.ProjectIndex((_, a), (ExtractFunc(MapFuncsCore.Constant(i)), _))) =>
      a.projectStatic(i, IdType.Array)

    case MFC(MapFuncsCore.ProjectIndex((_, a), (_, i))) =>
      (a ∧ i).projectDynamic

    case MFC(MapFuncsCore.ProjectKey((_, m), (ExtractFunc(MapFuncsCore.Constant(k)), _))) =>
      m.projectStatic(k, IdType.Map)

    case MFC(MapFuncsCore.ProjectKey((_, m), (_, k))) =>
      (m ∧ k).projectDynamic

    case func =>
      func.foldRight(qprov.empty) { case ((_, h), t) => h ∧ t }
  }

  ////

  private def applyFuncProvenance[A](fm: FreeMapA[A], arg: P): P =
    arg.traverseComponents[Id] { p =>
      computeFuncProvenance(fm)(κ(p))
    }

  private def applyFuncProvenanceN[A](fm: FreeMapA[Int])(x: P, xs: P*): P =
    qprov.applyComponentsN[Id, NonEmptyList](
      vs => computeFuncProvenance(fm)(vs.getUnsafe))(
      NonEmptyList.of(x, xs: _*))

  private def applyIdStatus(status: IdStatus, p: P): P =
    status match {
      case IncludeId =>
        p.traverseComponents[Id] { c =>
          qprov.and(
            c.injectStatic(EJson.int(0), IdType.Array),
            c.injectStatic(EJson.int(1), IdType.Array))
        }

      case _ => p
    }

  private def compute1[F[_]: Monad: MonadPlannerErr: QAuthS]
      (g: QSUGraph, src: QSUGraph)
      (f: P => P)
      : F[P] =
    handleMissingDims(dimsFor[F](src) map f) flatMap { ds =>
      QAuthS.modify(_.addDims(g.root, ds)) as ds
    }

  private def compute2[F[_]: Monad: MonadPlannerErr: QAuthS]
      (g: QSUGraph, l: QSUGraph, r: QSUGraph)
      (f: (P, P) => P)
      : F[P] =
    handleMissingDims((dimsFor[F](l) |@| dimsFor(r))(f)) flatMap { ds =>
      QAuthS.modify(_.addDims(g.root, ds)) as ds
    }

  private def compute3[F[_]: Monad: MonadPlannerErr: QAuthS]
      (g: QSUGraph, l: QSUGraph, c: QSUGraph, r: QSUGraph)
      (f: (P, P, P) => P)
      : F[P] =
    handleMissingDims((dimsFor[F](l) |@| dimsFor[F](c) |@| dimsFor[F](r))(f)) flatMap { ds =>
      QAuthS.modify(_.addDims(g.root, ds)) as ds
    }

  private def computeFuncProvenance[A](fm: FreeMapA[A])(f: A => P): P =
    fm.para(ginterpret(f, computeFuncProvenanceƒ[A]))

  private def computeJoin2[F[_]: Monad: MonadPlannerErr: QAuthS](
      g: QSUGraph,
      left: QSUGraph,
      right: QSUGraph,
      jf: JoinFunc)
      : F[P] =
    compute2[F](g, left, right) { (l, r) =>
      if (jf.empty)
        l ∧ r
      else
        applyFuncProvenanceN(jf map {
          case LeftSide => 0
          case RightSide => 1
        })(l, r)
    }

  private def dimsFor[F[_]: Functor: QAuthS](g: QSUGraph): V[F, P] =
    QAuthS[F].gets(_ lookupDims g.root toSuccessNel g.root)

  private def handleMissingDims[F[_]: Monad: MonadPlannerErr, A](v: V[F, A]): F[A] =
    (v: F[ValidationNel[Symbol, A]]).flatMap { v0 =>
      v0.map(_.point[F]) valueOr { syms =>
        MonadPlannerErr[F].raiseError[A](
          PlannerError.InternalError(s"Dependent dimensions not found: ${syms.show}.", None))
      }
    }

  private def projPathSegment(p: P, s: String): P =
    p.projectStatic(EJson.str(s), IdType.Dataset)

  private def segments(p: Path[_, _, _]): IList[String] = {
    val segs = Path.flatten[IList[String]](IList(), IList(), IList(), IList(_), IList(_), p)
    (segs.head :: segs.tail).join
  }
}

object ApplyProvenance {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: MonadPlannerErr](
      qprov: QProv[T], graph: QSUGraph[T])(
      implicit eqP: Equal[qprov.P])
      : F[AuthenticatedQSU[T, qprov.P]] =
    taggedInternalError("ApplyProvenance", mk(qprov).apply[F](graph))

  def computeDims[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: MonadPlannerErr]
      (qprov: QProv[T], graph: QSUGraph[T])(
      implicit P: Equal[qprov.P], F: MonadState_[F, QAuth[T, qprov.P]])
      : F[qprov.P] =
    mk(qprov).computeDims[F](graph)

  def computeFuncDims[T[_[_]]: BirecursiveT: EqualT: ShowT, A](
      qprov: QProv[T],
      fm: FreeMapA[T, A])(
      f: A => qprov.P)(
      implicit P: Equal[qprov.P])
      : Option[qprov.P] =
    mk(qprov).computeFuncDims(fm)(f)

  @Lenses
  final case class AuthenticatedQSU[T[_[_]], P](graph: QSUGraph[T], auth: QAuth[T, P])

  object AuthenticatedQSU {
    implicit def show[T[_[_]]: ShowT, P: Show]: Show[AuthenticatedQSU[T, P]] =
      Show.show { case AuthenticatedQSU(g, d) =>
        Cord("AuthenticatedQSU {\n") ++
        g.show ++
        Cord("\n\n") ++
        d.filterVertices(g.foldMapDown(sg => Set(sg.root))).show ++
        Cord("\n}")
      }
  }

  ////

  private def mk[T[_[_]]: BirecursiveT: EqualT: ShowT](
      qp: QProv[T])(
      implicit eqP: Equal[qp.P]) =
    new ApplyProvenance[T] {
      val qprov: qp.type = qp
      val PEqual = eqP
    }
}
