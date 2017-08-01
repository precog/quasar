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

import slamdata.Predef._
import quasar.Planner._
import quasar.contrib.matryoshka._
import quasar.ejson.implicits._
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

  private def delinearizeInner[G[_]: Functor, H[_], A]
    (HtoG: H ~> G)
    (implicit DE: Const[DeadEnd, ?] :<: H):
      Coalgebra[G, List[G[A]]] = {
    case Nil    => HtoG(DE.inj(Const[DeadEnd, List[G[A]]](Root)))
    case h :: t => h.as(t)
  }

  private def delinearizeTargets[G[_]: Functor, A]:
      ElgotCoalgebra[Hole \/ ?, G, List[G[A]]] = {
    case Nil    => SrcHole.left[G[List[G[A]]]]
    case h :: t => h.as(t).right
  }

  // FIXME unify with more general target delinearization
  private def delinearizeTargetsCoEnv[A]:
      Coalgebra[CoEnvQS, List[CoEnvQS[A]]] = {
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

  private def makeList[S[_[_]], G[_]: Traverse, H[_]]
    (input: S[H])
    (implicit R: Recursive.Aux[S[H], G])
      : List[G[EM]] =
    R.cata(input)(linearize[G]).reverse

  private def makeZipper[S[_[_]], G[_]: Traverse, H[_]]
    (left: S[H], right: S[H])
    (implicit
      mergeable: Mergeable.Aux[T, G],
      R: Recursive.Aux[S[H], G])
      : ZipperAcc[G] =
    elgot((
      ZipperSides(HoleF[T], HoleF[T]),
      ZipperTails(makeList[S, G, H](left), makeList[S, G, H](right))))(consZipped[G], zipper[G])

  def mergePair[S[_[_]], G[_]: Traverse, H[_]]
    (left: S[H], right: S[H])
    (HtoG: H ~> G, unify: List[G[EM]] => FreeQS)
    (implicit
      mergeable: Mergeable.Aux[T, G],
      C: Corecursive.Aux[S[H], G],
      R: Recursive.Aux[S[H], G],
      DE: Const[DeadEnd, ?] :<: H)
      : SrcMerge[S[H], FreeQS] = {

    val ZipperAcc(common, ZipperSides(lMap, rMap), ZipperTails(lTail, rTail)) =
      makeZipper[S, G, H](left, right)

    SrcMerge[S[H], FreeQS](
      common.reverse.ana[S[H]](delinearizeInner[G, H, EM](HtoG)),
      rebaseBranch(unify(lTail), lMap),
      rebaseBranch(unify(rTail), rMap))
  }

  def mergeT[G[_]: Traverse](left: T[G], right: T[G])(
    implicit
      mergeable: Mergeable.Aux[T, G],
      coalesce: Coalesce.Aux[T, G, G],
      N: Normalizable[G],
      DE: Const[DeadEnd, ?] :<: G,
      QC: QScriptCore :<: G,
      FI: Injectable.Aux[G, QScriptTotal]):
      SrcMerge[T[G], FreeQS] = {

    def insertIdentityMap: G[T[G]] => G[T[G]] = in => {
      QC.prj(in).cata({
        case qs @ Filter(_, _) => QC.inj(Map(QC.inj(qs).embed, HoleF))
        case qs @ Sort(_, _, _) => QC.inj(Map(QC.inj(qs).embed, HoleF))
        case qs => QC.inj(qs)
      }, in)
    }

    def norm: T[G] => T[G] = _.transCata[T[G]](
      insertIdentityMap >>> repeatedly(coalesce.coalesceQCNormalize[G](idPrism[G])))

    def unify: List[G[EM]] => FreeQS =
      _.reverse.ana[Free[G, Hole]](delinearizeTargets[G, EM] >>> (CoEnv(_))).mapSuspension(FI.inject)

    mergePair[T, G, G](norm(left), norm(right))(NaturalTransformation.refl, unify)
  }

  private def mergeFreeQS(left: FreeQS, right: FreeQS): SrcMerge[FreeQS, FreeQS] = {
    def trans = λ[QScriptTotal ~> CoEnvQS](qs => CoEnv(qs.right[Hole]))

    def unify: List[CoEnvQS[EM]] => FreeQS =
      _.reverse.ana[FreeQS](delinearizeTargetsCoEnv[EM])

    mergePair[Free[?[_], Hole], CoEnvQS, QScriptTotal](left, right)(trans, unify)
  }

  private def mergeBranches(rewrite: Rewrite[T])(left: FreeQS, right: FreeQS): PlannerError \/ SrcMerge[FreeQS, FreeMap] =
    (left ≟ right).fold(SrcMerge(left, HoleF[T], HoleF[T]).right[PlannerError],
      {
        val SrcMerge(src, lBranch, rBranch) = mergeFreeQS(left, right)
        val (combine, lacc, racc) = concat(LeftSideF[T], RightSideF[T])

        def rebase0(l: FreeQS)(r: FreeQS): Option[FreeQS] = rebase(l, r).some

        val baseSrc: Option[CoEnvQS[FreeQS]] =
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
