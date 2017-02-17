/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.qscript

import quasar.Predef._
import quasar.Planner._
import quasar.fp.{ ExternallyManaged => EM, _ }

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

class Merge[T[_[_]]: BirecursiveT: EqualT: ShowT] extends TTypes[T] {
  case class ZipperSides(
    lSide: FreeMap,
    rSide: FreeMap)

  case class ZipperTails[G[_]](
    lTail: List[G[EM]],
    rTail: List[G[EM]])

  case class ZipperAcc[G[_]](
    acc: List[G[EM]],
    sides: ZipperSides,
    tails: ZipperTails[G])

  // TODO: Convert to NEL
  private def linearize[G[_]: Traverse]: Algebra[G, List[G[EM]]] =
    fl => fl.as[EM](Extern) :: fl.fold

  private def delinearizeInner[G[_]: Traverse, A](
    implicit DE: Const[DeadEnd, ?] :<: G):
      Coalgebra[G, List[G[A]]] = {
    case Nil    => DE.inj(Const[DeadEnd, List[G[A]]](Root))
    case h :: t => h.as(t)
  }

  private def delinearizeInnerCoEnv[A](
    implicit DE: Const[DeadEnd, ?] :<: QScriptTotal):
      Coalgebra[CoEnv[Hole, QScriptTotal, ?], List[CoEnv[Hole, QScriptTotal, A]]] = {
    case Nil    => CoEnv(\/-(DE.inj(Const[DeadEnd, List[CoEnv[Hole, QScriptTotal, A]]](Root))))
    case h :: t => h.as(t)
  }

  private def delinearizeTargets[G[_]: Functor, A]:
      ElgotCoalgebra[Hole \/ ?, G, List[G[A]]] = {
    case Nil    => SrcHole.left[G[List[G[A]]]]
    case h :: t => h.as(t).right
  }

  private def delinearizeTargetsCoEnv[A]:
      Coalgebra[CoEnv[Hole, QScriptTotal, ?], List[CoEnv[Hole, QScriptTotal, A]]] = {
    case Nil    => CoEnv(-\/(SrcHole))
    case h :: t => h.as(t)
  }

  private def consZipped[G[_]]: Algebra[ListF[G[EM], ?], ZipperAcc[G]] = {
    case ConsF(head, ZipperAcc(acc, sides, tails)) =>
      ZipperAcc(head :: acc, sides, tails)
    case NilF() =>
      ZipperAcc(Nil, ZipperSides(HoleF[T], HoleF[T]), ZipperTails(Nil, Nil))
  }

  private def zipper[G[_]](
    implicit mergeable: Mergeable.Aux[T, G]):
      ElgotCoalgebra[
        ZipperAcc[G] \/ ?,
        ListF[G[EM], ?],
        (ZipperSides, ZipperTails[G])] = {
    case (zs @ ZipperSides(lm, rm), zt @ ZipperTails(l :: ls, r :: rs)) =>
      mergeable.mergeSrcs(lm, rm, l, r).fold[ZipperAcc[G] \/ ListF[G[EM], (ZipperSides, ZipperTails[G])]](
        ZipperAcc(Nil, zs, zt).left) {
          case SrcMerge(inn, lmf, rmf) =>
            ConsF(inn, (ZipperSides(lmf, rmf), ZipperTails(ls, rs))).right[ZipperAcc[G]]
      }
    case (sides, tails) =>
      ZipperAcc(Nil, sides, tails).left
  }

  def mergeT[G[_]: Traverse](left: T[G], right: T[G])(
    implicit
      mergeable: Mergeable.Aux[T, G],
      DE: Const[DeadEnd, ?] :<: G,
      FI: Injectable.Aux[G, QScriptTotal]):
      SrcMerge[T[G], FreeQS] = {
    val lLin: List[G[EM]] = left.cata(linearize[G]).reverse
    val rLin: List[G[EM]] = right.cata(linearize[G]).reverse

    val ZipperAcc(common, ZipperSides(lMap, rMap), ZipperTails(lTail, rTail)) =
      elgot(
        (ZipperSides(HoleF[T], HoleF[T]), ZipperTails(lLin, rLin)))(
        consZipped[G], zipper[G])

    val leftF: FreeQS =
      lTail.reverse.ana[Free[G, Hole]](delinearizeTargets[G, EM] >>> (CoEnv(_))).mapSuspension(FI.inject)

    val rightF: FreeQS =
      rTail.reverse.ana[Free[G, Hole]](delinearizeTargets[G, EM] >>> (CoEnv(_))).mapSuspension(FI.inject)

    SrcMerge[T[G], FreeQS](common.reverse.ana[T[G]](delinearizeInner[G, EM]),
      rebaseBranch(leftF, lMap),
      rebaseBranch(rightF, rMap))
  }

  // TODO remove duplication between `mergeFreeQS` and `mergeT`
  private def mergeFreeQS(left: FreeQS, right: FreeQS): SrcMerge[FreeQS, FreeQS] = {
    val lLin: List[CoEnv[Hole, QScriptTotal, EM]] = left.cata(linearize).reverse
    val rLin: List[CoEnv[Hole, QScriptTotal, EM]] = right.cata(linearize).reverse

    val ZipperAcc(common, ZipperSides(lMap, rMap), ZipperTails(lTail, rTail)) =
      elgot(
        (ZipperSides(HoleF[T], HoleF[T]), ZipperTails(lLin, rLin)))(
        consZipped[CoEnv[Hole, QScriptTotal, ?]], zipper[CoEnv[Hole, QScriptTotal, ?]])

    val leftF: FreeQS =
      lTail.reverse.ana[FreeQS](delinearizeTargetsCoEnv[EM])
    val rightF: FreeQS =
      rTail.reverse.ana[FreeQS](delinearizeTargetsCoEnv[EM])

    val mergeSrc: FreeQS =
      common.reverse.ana[FreeQS](delinearizeInnerCoEnv[EM])

    SrcMerge[FreeQS, FreeQS](
      mergeSrc,
      rebaseBranch(leftF, lMap),
      rebaseBranch(rightF, rMap))
  }

  private def mergeBranches(rewrite: Rewrite[T])(left: FreeQS, right: FreeQS): PlannerError \/ SrcMerge[FreeQS, FreeMap] =
    (left ≟ right).fold(SrcMerge(left, HoleF[T], HoleF[T]).right[PlannerError],
      {
        val SrcMerge(src, lBranch, rBranch) = mergeFreeQS(left, right)
        val (combine, lacc, racc) = concat(LeftSideF[T], RightSideF[T])

        def rebase0(l: FreeQS)(r: FreeQS): Option[FreeQS] = rebase(l, r).some

        val baseSrc: Option[CoEnv[Hole, QScriptTotal, FreeQS]] =
          rewrite.unifySimpleBranchesCoEnv[QScriptTotal, FreeQS](src, lBranch, rBranch, combine)(rebase0)

        baseSrc.cata(src =>
          SrcMerge(src.embed, lacc, racc).right[PlannerError],
          InternalError.fromMsg(s"failed to merge branches").left[SrcMerge[FreeQS, FreeMap]])
      })

  // FIXME: We shouldn't have to guess which side is which.
  // Fixing #1556 should address this issue.
  def tryMergeBranches(rewrite: Rewrite[T])(left1: FreeQS, right1: FreeQS, left2: FreeQS, right2: FreeQS):
      PlannerError \/ (SrcMerge[FreeQS, FreeMap], SrcMerge[FreeQS, FreeMap]) =
    mergeBranches(rewrite)(left1, left2).fold(
      {
        _ => for {
          resL <- mergeBranches(rewrite)(left1, right2)
          resR <- mergeBranches(rewrite)(right1, left2)
        } yield (resL, resR)
      },
      {
        resL => for {
          resR <- mergeBranches(rewrite)(right1, right2)
        } yield (resL, resR)
      })
}
