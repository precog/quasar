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
import quasar.IdStatus.ExcludeId
import quasar.ParseInstruction
import quasar.ParseInstruction.{Mask, Pivot, Wrap}
import quasar.common.{CPath, CPathIndex}
import quasar.contrib.iota._
import quasar.ejson
import quasar.fp.{liftFG, Injectable}
import quasar.qscript._
import quasar.qscript.MapFuncCore._
import quasar.qscript.RecFreeS._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.{ginterpretM, CoEnv}

import scalaz.{\/, -\/, \/-, Const, Functor}
import scalaz.Scalaz._ // apply-traverse syntax conflict

final class RewritePushdown[T[_[_]]: BirecursiveT: EqualT] extends TTypes[T] {

  import MapFuncsCore.{Constant, IntLit, ProjectIndex, ProjectKey}

  // We perform this rewrite before `extraShift` because sometimes
  // a no-op Map is added in the transformation from `QScriptEducated`,
  // which interferes with the pattern matching in `extraShift`.
  def elideNoopMap[F[a] <: ACopK[a]: Functor](implicit QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => F[T[F]] = {
    case Map(Embed(src), mf) if mf === HoleR => src
    case qc => QC.inj(qc)
  }

  def rewriteLeftShift[F[a] <: ACopK[a]: Functor, A](
      implicit
        ER: Const[InterpretedRead[A], ?] :<<: F,
        SR: Const[ShiftedRead[A], ?] :<<: F,
        QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => F[T[F]] = {

    // LeftShift(ShiftedRead(_, ExcludeId), /foo/bar/, _, _, Omit, f(RightSide))
    case qc @ LeftShift(Embed(src), struct, shiftStatus, shiftType, OnUndefined.Omit, repair) => {
      import construction.Func

      val linearStruct: FreeMap = struct.linearize
      val pathOpt: Option[CPath] = findPath(linearStruct)

      // arrays are compacted during static array projection
      // resulting in a single-element array
      def compactArrayDerefs(mf: CoMapFuncR[T, Hole]): CoMapFuncR[T, Hole] =
        mf.run match {
          case \/-(MFC(ProjectIndex(src, ExtractFunc(Constant(Embed(EX(ejson.Int(_)))))))) =>
            val func: MapFunc[FreeMap] = MFC(ProjectIndex(src, IntLit(0)))
            CoEnv(func.right[Hole])
          case f => CoEnv(f)
        }

      val mfOpt: Option[FreeMap] =
        repair.traverseM[Option, Hole] {
          case LeftSide => None  // repair must not contain LeftSide
          case RightSide =>
            val compacted: FreeMap = linearStruct.transCata[FreeMap](compactArrayDerefs)
            Func[T].ProjectKeyS(compacted, ShiftedKey).some
        }

      val srOpt: Option[ShiftedRead[A]] = SR.prj(src) flatMap { srConst =>
        val sr = srConst.getConst

        sr.idStatus match {
          case ExcludeId => sr.some  // only rewrite if ShiftedRead has ExcludeId
          case _ => None
        }
      }

      val rewritten: Option[F[T[F]]] = (mfOpt |@| srOpt |@| pathOpt) {
        case (mf, sr, path) =>
          val tpe = ShiftType.toParseType(shiftType)

          // arrays are compacted during static array projection
          // resulting in a single-element array
          val compactedPath: CPath =
            CPath(path.nodes.map {
              case CPathIndex(_) => CPathIndex(0)
              case node => node
            })

          val instructions: List[ParseInstruction] = List(
            Mask(SMap((path, Set(tpe)))),
            Wrap(compactedPath, ShiftedKey),
            Pivot(SMap((compactedPath \ ShiftedKey, (shiftStatus, tpe)))))

          val src: T[F] = ER.inj(Const[InterpretedRead[A], T[F]](
            InterpretedRead[A](sr.path, instructions))).embed

          QC.inj(Map(src, mf.asRec))
      }

      rewritten.getOrElse(QC.inj(qc))
    }

    case qc => QC.inj(qc)
  }

  /* Matches on a `FreeMap` of the form
   * ```
   * Project<Key or Index>(
   *   Project<Key or Index>(...(
   *     Project<Key or Index>(Hole, Const(<Str or Int>))),
   *     Const(<Str or Int>)),
   *   Const(<Str or Int>))
   * ```
   * returning the `CPath` when that form is matched.
   */
  def findPath(fm: FreeMap): Option[CPath] = {
    // A `FreeMap` effectively has two leaf types: `Constant` and `Hole`.
    // When we hit a `Constant` we need to keep searching even though
    // we haven't found a path to shift, and so we encode this as `Unit`.
    type Acc = Unit \/ CPath

    type CoFreeMap = CoEnv[Hole, MapFunc, (FreeMap, Acc)]

    def transformHole(hole: Hole): Option[Acc] =
      CPath.Identity.right.some

    def transformFunc(func: MapFunc[(FreeMap, Acc)]): Option[Acc] =
      func match {
        case MFC(Constant(Embed(EC(ejson.Str(_))))) =>
          ().left.some // this is awkward but we have to keep going

        case MFC(Constant(Embed(EX(ejson.Int(_))))) =>
          ().left.some // this is awkward but we have to keep going

        // the source of a `ProjectKey` must have encoded as a `CPath`
        case MFC(ProjectKey(
            (_, \/-(srcPath)),
            (ExtractFunc(Constant(Embed(EC(ejson.Str(str))))), _))) =>
          (str \: srcPath).right.some

        // the source of a `ProjectIndex` must have encoded as a `CPath`
        case MFC(ProjectIndex(
            (_, \/-(srcPath)),
            (ExtractFunc(Constant(Embed(EX(ejson.Int(idx))))), _))) if idx.isValidInt =>
          (idx.toInt \: srcPath).right.some

        case _ => none
      }

    def transform: CoFreeMap => Option[Acc] =
      ginterpretM[(FreeMap, ?), Option, MapFunc, Hole, Acc](
        transformHole(_),
        transformFunc(_))

    val transformed: Option[Acc] =
      fm.paraM[Option, Acc](transform)

    transformed flatMap {
      case -\/(_) => none
      case \/-(path) => CPath(path.nodes.reverse).some
    }
  }
}

object RewritePushdown {
  def apply[T[_[_]]: BirecursiveT: EqualT, F[a] <: ACopK[a]: Functor, G[a] <: ACopK[a], A](
      implicit GF: Injectable[G, F],
               ESRF: Const[InterpretedRead[A], ?] :<<: F,
               SRF: Const[ShiftedRead[A], ?] :<<: F,
               QCF: QScriptCore[T, ?] :<<: F,
               QCG: QScriptCore[T, ?] :<<: G)
      : G[T[F]] => F[T[F]] = {

    val pushdown = new RewritePushdown[T]

    val rewrite1: G[T[F]] => F[T[F]] =
      gtf => QCG.prj(gtf).fold(GF.inject(gtf))(pushdown.elideNoopMap[F])

    val rewrite2: F[T[F]] => F[T[F]] =
      liftFG[QScriptCore[T, ?], F, T[F]](pushdown.rewriteLeftShift[F, A])

    rewrite1 andThen rewrite2
  }
}
