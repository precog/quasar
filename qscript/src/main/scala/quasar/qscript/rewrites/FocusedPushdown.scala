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

package quasar.qscript.rewrites

import slamdata.Predef.{Map => SMap, _}

import quasar.ParseType
import quasar.ParseInstruction.{Mask, Pivot, Project, Wrap}
import quasar.common.{CPath, CPathArray, CPathField, CPathIndex, CPathMeta, CPathNode}
import quasar.contrib.iota._
import quasar.contrib.scalaz.free._
import quasar.fp.{Injectable, liftFG, liftFGM}
import quasar.fp.ski.κ
import quasar.qscript._
import quasar.qscript.MapFuncCore._
import quasar.qscript.RecFreeS._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.CoEnv

import scalaz.{-\/, \/-, Const, Foldable, Free, Functor, Kleisli, IMap, State}
import scalaz.Scalaz._ // apply-traverse syntax conflict

final class FocusedPushdown[T[_[_]]: BirecursiveT: EqualT] private () extends TTypes[T] {

  private val mapFunc = construction.Func[T]
  private val recMapFunc = construction.RecFunc[T]

  // Assumes `extractProject` and `extractMask` have been applied to the input.
  def extractPivot[F[a] <: ACopK[a]: Functor, A](
      implicit
      IR: Const[InterpretedRead[A], ?] :<<: F,
      R: Const[Read[A], ?] :<<: F,
      QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => Option[F[T[F]]] = {

    val Res = Resource[A]

    _ match {
      case qc @ LeftShift(
          Embed(Res(a, instrs)),
          hole @ FreeA(_),
          shiftStatus,
          shiftType,
          OnUndefined.Omit,
          FocusedRepair(repair)) =>

        val pivotInstr =
          Pivot(CPath.Identity, shiftStatus, ShiftType.toParseType(shiftType))

        val iread =
          IR[T[F]](Const(InterpretedRead(a, instrs :+ pivotInstr)))

        some(repair.fold(κ(iread), κ(QC(Map(iread.embed, repair.asRec)))))

      case _ => none
    }
  }

  // Assumes `extractProject` has been applied to the input.
  def extractMask[F[a] <: ACopK[a]: Functor, A](
      implicit
      IR: Const[InterpretedRead[A], ?] :<<: F,
      R: Const[Read[A], ?] :<<: F,
      QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => F[T[F]] = {

    val Res = Resource[A]

    _ match {
      case Map(Embed(Res(a, instrs)), ProjectedRec(MaskedObject(keys))) =>
        val mask = keys.foldLeft(SMap[CPath, Set[ParseType]]())(
          (m, k) => m.updated(CPath.Identity \ k, ParseType.Top))
        IR[T[F]](Const(InterpretedRead(a, instrs :+ Mask(mask))))

      case Map(Embed(Res(a, instrs)), ProjectedRec(MaskedArray(indices))) =>
        val mask = indices.foldLeft(SMap[CPath, Set[ParseType]]())(
          (m, i) => m.updated(CPath.Identity \ i, ParseType.Top))
        IR[T[F]](Const(InterpretedRead(a, instrs :+ Mask(mask))))

      case Map(Embed(Res(a, instrs)), ProjectedRec(fm)) if isInnerExpr(fm) =>
        val mask = fm.foldLeft(SMap[CPath, Set[ParseType]]())(_.updated(_, ParseType.Top))
        QC(Map(IR[T[F]](Const(InterpretedRead(a, instrs :+ Mask(mask)))).embed, compactedExpr(fm).asRec))

      case LeftShift(
            Embed(Res(a, instrs)),
            hole @ FreeA(_),
            idStatus,
            shiftType,
            onUndef,
            repair @ FocusedRepair(_)) =>

        val maskInstr =
          Mask(SMap(CPath.Identity -> Set(ShiftType.toParseType(shiftType))))

        QC(LeftShift(
          IR[T[F]](Const(InterpretedRead(a, instrs :+ maskInstr))).embed,
          hole,
          idStatus,
          shiftType,
          onUndef,
          repair))

      case LeftShift(
            Embed(Res(a, instrs)),
            ProjectedRec(fm),
            idStatus,
            shiftType,
            onUndef,
            repair @ FocusedRepair(_)) if isInnerExpr(fm) =>

        val maskInstr =
          Mask(fm.foldLeft(SMap[CPath, Set[ParseType]]())(_.updated(_, ParseType.Top)))

        QC(LeftShift(
          IR[T[F]](Const(InterpretedRead(a, instrs :+ maskInstr))).embed,
          compactedExpr(fm).asRec,
          idStatus,
          shiftType,
          onUndef,
          repair))

      case qc => QC(qc)
    }
  }

  def extractProject[F[a] <: ACopK[a]: Functor, A](
      implicit
      ER: Const[InterpretedRead[A], ?] :<<: F,
      SR: Const[Read[A], ?] :<<: F,
      QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => F[T[F]] = {

    val Res = Resource[A]

    def commonPrefix(xs: List[CPathNode], ys: List[CPathNode]): List[CPathNode] =
      (xs zip ys) flatMap {
        case (x, y) if x === y => List(x)
        case _ => Nil
      }

    def projectFunc(nodes: List[CPathNode]): Option[FreeMap] =
      nodes.foldLeftM(mapFunc.Hole) {
        case (src, CPathField(key)) => some(mapFunc.ProjectKeyS(src, key))
        case (src, CPathIndex(idx)) => some(mapFunc.ProjectIndexI(src, idx))
        case _ => none
      }

    def quotientCommonPrefix(projectedFunc: FreeMapA[CPath]): Option[(CPath, FreeMap)] = {
      // Extract all paths, leaving behind a pointer so we can resubstitute later
      val (ipaths, ifm) =
        (projectedFunc.cataM[State[Int, ?], (SMap[Int, CPath], FreeMapA[Int])] {
          case CoEnv(-\/(p)) =>
            for {
              i <- State.get[Int]
              _ <- State.put(i + 1)
            } yield (SMap(i -> p), Free.pure(i))

          case CoEnv(\/-(mf)) =>
            (
              mf.foldLeft(SMap[Int, CPath]())(_ ++ _._1),
              rollMF(mf.map(_._2)).embed
            ).pure[State[Int, ?]]
        }).eval(0)

      // The common prefix among all paths
      val prefixNodes =
        ipaths.values.toList.foldMapLeft1Opt(_.nodes)(
          (pfx, p) => commonPrefix(pfx, p.nodes))

      prefixNodes.filter(_.nonEmpty) flatMap { pnodes =>
        val prefixLen = pnodes.length
        val prefixPath = CPath(pnodes)

        val projectFuncs = ipaths.toList.traverse {
          case (i, p) => projectFunc(p.nodes.drop(prefixLen)) strengthL i
        }

        projectFuncs map { kvs =>
          val m = SMap(kvs: _*)
          (prefixPath, ifm.flatMap(m))
        }
      }
    }

    _ match {
      case Map(Embed(Res(a, instrs)), ProjectedRec(FreeA(p))) =>
        ER(Const(InterpretedRead(a, instrs :+ Project(p))))

      case qc @ Map(Embed(Res(a, instrs)), ProjectedRec(fm)) if isInnerExpr(fm) =>
        quotientCommonPrefix(fm).fold(QC(qc)) {
          case (prefix, rebuiltFm) =>
            val newInstrs = instrs :+ Project(prefix)
            QC(Map(ER[T[F]](Const(InterpretedRead(a, newInstrs))).embed, rebuiltFm.asRec))
        }

      case LeftShift(
          Embed(Res(a, instrs)),
          ProjectedRec(FreeA(p)),
          idStatus,
          shiftType,
          onUndef,
          rep @ FocusedRepair(_)) =>
        QC(LeftShift(
          ER[T[F]](Const(InterpretedRead(a, instrs :+ Project(p)))).embed,
          recMapFunc.Hole,
          idStatus,
          shiftType,
          onUndef,
          rep))

      case qc @ LeftShift(
          Embed(Res(a, instrs)),
          ProjectedRec(struct),
          idStatus,
          shiftType,
          onUndef,
          rep @ FocusedRepair(_)) if isInnerExpr(struct) =>
        quotientCommonPrefix(struct).fold(QC(qc)) {
          case (prefix, rebuiltFm) =>
            val newInstrs = instrs :+ Project(prefix)
            QC(LeftShift(
              ER[T[F]](Const(InterpretedRead(a, newInstrs))).embed,
              rebuiltFm.asRec,
              idStatus,
              shiftType,
              onUndef,
              rep))
        }

      case qc => QC(qc)
    }
  }

  def extractWrap[F[a] <: ACopK[a]: Functor, A](
      implicit
      IR: Const[InterpretedRead[A], ?] :<<: F,
      R: Const[Read[A], ?] :<<: F,
      QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => F[T[F]] = {

    val Res = Resource[A]

    _ match {
      case Map(Embed(Res(a, instrs)), WrappedRec(p)) =>
        val wrappedInstrs =
          p.foldRight(instrs)((s, ins) => ins :+ Wrap(CPath.Identity, s))

        IR[T[F]](Const(InterpretedRead(a, wrappedInstrs)))

      case qc => QC(qc)
    }
  }

  ////

  /**
   * Treats the input as a masked expression and returns a concrete expression
   * accessing the masked values, adjusting array projections as necessary to
   * accomodate array compaction.
   */
  private def compactedExpr(prj: FreeMapA[CPath]): FreeMap = {
    def indices(p: CPath): IMap[CPath, Set[Int]] =
      p.nodes.foldLeft((CPath.Identity, IMap.empty[CPath, Set[Int]])) {
        case ((p, m), CPathIndex(i)) => (p \ i, m.insertWith(_ ++ _, p, Set(i)))
        case ((p, m), n) => (p \ n, m)
      }._2

    val remapping =
      Foldable[FreeMapA].foldMap(prj)(indices)
        .map(_.toList.sorted.zipWithIndex.toMap)

    prj flatMap { cpath =>
      cpath.nodes.foldLeft((CPath.Identity, mapFunc.Hole)) {
        case ((p, expr), CPathIndex(i)) =>
          (p \ i, mapFunc.ProjectIndexI(expr, remapping.lookup(p).fold(i)(_(i))))

        case ((p, expr), n @ CPathField(k)) =>
          (p \ k, mapFunc.ProjectKeyS(expr, k))

        case ((p, expr), m @ CPathMeta(k)) =>
          (p \ m, mapFunc.ProjectKeyS(mapFunc.Meta(expr), k))

        case (acc, CPathArray) =>
          acc
      }._2
    }
  }
}

object FocusedPushdown {
  def apply[T[_[_]]: BirecursiveT: EqualT, F[a] <: ACopK[a]: Functor, G[a] <: ACopK[a], A](
      implicit
      GF: Injectable[G, F],
      IRF: Const[InterpretedRead[A], ?] :<<: F,
      RF: Const[Read[A], ?] :<<: F,
      QCF: QScriptCore[T, ?] :<<: F,
      QCG: QScriptCore[T, ?] :<<: G)
      : G[T[F]] => F[T[F]] = {

    val pushdown = new FocusedPushdown[T]

    val extractMappable: QScriptCore[T, T[F]] => F[T[F]] =
      pushdown.extractProject[F, A] >>>
      liftFG[QScriptCore[T, ?], F, T[F]](pushdown.extractMask[F, A]) >>>
      liftFG[QScriptCore[T, ?], F, T[F]](pushdown.extractWrap[F, A])

    val extractMappableG: G[T[F]] => F[T[F]] =
      gtf => QCG.prj(gtf).fold(GF.inject(gtf))(extractMappable)

    val extractMappableF: F[T[F]] => F[T[F]] =
      liftFG[QScriptCore[T, ?], F, T[F]](extractMappable)

    val extractPivot: F[T[F]] => Option[F[T[F]]] =
      liftFGM[Option, QScriptCore[T, ?], F, T[F]](pushdown.extractPivot[F, A])

    extractMappableG >>> orOriginal(Kleisli(extractPivot) map extractMappableF)
  }
}
