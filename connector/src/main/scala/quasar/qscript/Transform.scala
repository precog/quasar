/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef.{ Eq => _, _ }
import quasar._, Planner._
import quasar.ejson.{Int => _, _}
import quasar.fp.{ ExternallyManaged => EM, _ }
import quasar.frontend.{logicalplan => lp}
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._
import quasar.sql.JoinDir
import quasar.std.StdLib._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _}, Scalaz.{ToIdOps => _, _}, Inject._, Leibniz._
import shapeless.{nat, Sized}

// Need to keep track of our non-type-ensured guarantees:
// - all conditions in a ThetaJoin will refer to both sides of the join
// - the common source in a Join or Union will be the longest common branch
// - any unreferenced src will be `Unreferenced`, and no `Unreferenced` will
//   ever be referenced
// - ReduceIndices will not exceed the reducer bounds, and every reducer will be
//   referenced at least once.

// TODO: Could maybe require only Functor[F], once CoEnv exposes the proper
//       instances
class Transform
  [T[_[_]]: BirecursiveT: EqualT: ShowT,
    F[_]: Traverse: Normalizable]
  (implicit
    C:  Coalesce.Aux[T, F, F],
    DE: Const[DeadEnd, ?] :<: F,
    QC: QScriptCore[T, ?] :<: F,
    TJ: ThetaJoin[T, ?] :<: F,
    PB: ProjectBucket[T, ?] :<: F,
    // TODO: Remove this one once we have multi-sorted AST
    FI: Injectable.Aux[F, QScriptTotal[T, ?]],
    render: Delay[RenderTree, F],
    mergeable: Mergeable.Aux[T, F],
    mergeableCoEnv: Mergeable.Aux[T, CoEnv[Hole, QScriptTotal[T, ?], ?]],
    eq: Delay[Equal, F],
    show: Delay[Show, F]) extends TTypes[T] {

  private val prov = new provenance.ProvenanceT[T]
  private val rewrite = new Rewrite[T]

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
  def linearize[G[_]: Functor: Foldable]: Algebra[G, List[G[EM]]] =
    fl => fl.as[EM](Extern) :: fl.fold

  private def delinearizeInner[A]:
      Coalgebra[F, List[F[A]]] = {
    case Nil    => DE.inj(Const[DeadEnd, List[F[A]]](Root))
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

  private def merge(left: T[F], right: T[F]): SrcMerge[T[F], FreeQS] = {
    val lLin: List[F[EM]] = left.cata(linearize).reverse
    val rLin: List[F[EM]] = right.cata(linearize).reverse

    val ZipperAcc(common, ZipperSides(lMap, rMap), ZipperTails(lTail, rTail)) =
      elgot(
        (ZipperSides(HoleF[T], HoleF[T]), ZipperTails(lLin, rLin)))(
        consZipped[F], zipper[F])

    val leftF: FreeQS =
      lTail.reverse.ana[Free[F, Hole]](delinearizeTargets[F, EM] >>> (CoEnv(_))).mapSuspension(FI.inject)

    val rightF: FreeQS =
      rTail.reverse.ana[Free[F, Hole]](delinearizeTargets[F, EM] >>> (CoEnv(_))).mapSuspension(FI.inject)

    SrcMerge[T[F], FreeQS](common.reverse.ana[T[F]](delinearizeInner),
      rebaseBranch(leftF, lMap),
      rebaseBranch(rightF, rMap))
  }

  private case class AutoJoinBase(src: T[F], buckets: List[prov.Provenance]) {
    def asTarget(vals: FreeMap): Target[F] = Target(Ann(buckets, vals), src)
  }

  private case class AutoJoinResult(base: AutoJoinBase, lval: FreeMap, rval: FreeMap)
  private case class AutoJoin3Result(base: AutoJoinBase, lval: FreeMap, cval: FreeMap, rval: FreeMap)
  private case class AutoJoinNResult(base: AutoJoinBase, vals: NonEmptyList[FreeMap])

  private def joinBranches(left: FreeQS, right: FreeQS): PlannerError \/ SrcMerge[FreeQS, FreeMap] = {
    val SrcMerge(src, lBranch, rBranch) = mergeFreeQS(left, right)

    val (combine, lacc, racc) = concat(LeftSideF[T], RightSideF[T])

    def rebase0(l: FreeQS)(r: FreeQS): Option[FreeQS] =
      rebase(l, r).some

    val baseSrc: Option[CoEnv[Hole, QScriptTotal, FreeQS]] =
      rewrite.unifySimpleBranchesCoEnv[QScriptTotal, FreeQS](src, lBranch, rBranch, combine)(rebase0)

    baseSrc.cata(src =>
      SrcMerge(src.embed, lacc, racc).right[PlannerError],
      InternalError.fromMsg(s"failed autojoin").left[SrcMerge[FreeQS, FreeMap]])
  }

  /** This unifies a pair of sources into a single one, with additional
    * expressions to access the combined bucketing info, as well as the left and
    * right values.
    */
  private def autojoin(left: Target[F], right: Target[F]): AutoJoinResult = {
    val lann = left.ann
    val rann = right.ann
    val lval: JoinFunc = lann.values.as[JoinSide](LeftSide)
    val rval: JoinFunc = rann.values.as[JoinSide](RightSide)
    val SrcMerge(src, lBranch, rBranch) = merge(left.value, right.value)

    val lprovs = prov.genBuckets(lann.provenance) ∘ (_ ∘ (_.as[JoinSide](LeftSide)))
    val rprovs = prov.genBuckets(rann.provenance) ∘ (_ ∘ (_.as[JoinSide](RightSide)))

    val (combine, newLprov, newRprov, lacc, racc) =
      (lprovs, rprovs) match {
        case (None, None) =>
          val (combine, lacc, racc) = concat(lval, rval)
          (combine, lann.provenance, rann.provenance, lacc, racc)
        case (None, Some((rProvs, rBuck))) =>
          val (combine, bacc, lacc, racc) = concat3(rBuck, lval, rval)
          (combine, lann.provenance, prov.rebase(bacc, rProvs), lacc, racc)
        case (Some((lProvs, lBuck)), None) =>
          val (combine, bacc, lacc, racc) = concat3(lBuck, lval, rval)
          (combine, prov.rebase(bacc, lProvs), rann.provenance, lacc, racc)
        case (Some((lProvs, lBuck)), Some((rProvs, rBuck))) =>
          val (combine, lbacc, rbacc, lacc, racc) =
            concat4(lBuck, rBuck, lval, rval)
          (combine, prov.rebase(lbacc, lProvs), prov.rebase(rbacc, rProvs), lacc, racc)
      }

    AutoJoinResult(
      AutoJoinBase(
        rewrite.unifySimpleBranches[F, T[F]](src, lBranch, rBranch, combine)(rewrite.rebaseT).getOrElse(
          TJ.inj(ThetaJoin(src, lBranch, rBranch, prov.genComparisons(newLprov, newRprov), Inner, combine))).embed,
        prov.joinProvenances(newLprov, newRprov)),
      lacc,
      racc)
  }

  /** A convenience for a pair of autojoins, does the same thing, but returns
    * access to all three values.
    */
  private def autojoin3(left: Target[F], center: Target[F], right: Target[F]): AutoJoin3Result = {
    val AutoJoinResult(lbase, lval, cval) = autojoin(left, center)
    val AutoJoinResult(base , bval, rval) = autojoin(lbase asTarget HoleF, right)
    AutoJoin3Result(base, lval >> bval, cval >> bval, rval)
  }

  private def autojoinN[G[_]: Foldable1](ts: G[Target[F]]): AutoJoinNResult = {
    val (t, vs) = ts.foldMapLeft1((_, HoleF[T].wrapNel)) {
      case ((l, vals), r) =>
        val AutoJoinResult(b, lv, rv) = autojoin(l, r)
        (b asTarget HoleF, rv <:: vals.map(_ >> lv))
    }

    AutoJoinNResult(AutoJoinBase(t.value, t.ann.provenance), vs.reverse map (_ >> t.ann.values))
  }

  private def merge3Map(
    values: Func.Input[Target[F], nat._3])(
    func: (FreeMap, FreeMap, FreeMap) => MapFunc[FreeMap]
  ): Target[F] = {
    val AutoJoin3Result(base, lval, cval, rval) = autojoin3(values(0), values(1), values(2))
    base asTarget Free.roll(func(lval, cval, rval))
  }

  private def shiftValues(input: Target[F]): Target[F] = {
    val Ann(provs, value) = input.ann
    val (sides, leftAccess, rightAccess) =
      concat(
        Free.point[MapFunc, JoinSide](LeftSide),
        Free.point[MapFunc, JoinSide](RightSide))

    Target(Ann(
      provenance.Value(Free.roll[MapFunc, Hole](ProjectIndex(rightAccess, IntLit(0)))) :: prov.rebase(leftAccess, provs),
      Free.roll(ProjectIndex(rightAccess, IntLit(1)))),
      QC.inj(LeftShift(input.value, value, IncludeId, sides)).embed)
  }

  private def shiftIds(input: Target[F]):
      Target[F] = {
    val Ann(provs, value) = input.ann
    val (sides, leftAccess, rightAccess) =
      concat(
        Free.point[MapFunc, JoinSide](LeftSide),
        Free.point[MapFunc, JoinSide](RightSide))

    Target(Ann(
      provenance.Value(rightAccess) :: prov.rebase(leftAccess, provs),
      rightAccess),
      QC.inj(LeftShift(input.value, value, IdOnly, sides)).embed)
  }

  private def flatten(input: Target[F]): Target[F] = {
    val Target(Ann(provs, value), fa) = input
    Target(Ann(prov.nestProvenances(provs), value), fa)
  }

  // NB: More complicated LeftShifts are generated as an optimization:
  // before: ThetaJoin(cs, Map(Hole, mf), LeftShift(Hole, struct, repair), comb)
  // after: LeftShift(cs, struct, comb.flatMap(LeftSide => mf.map(_ => LeftSide), RS => repair))
  private def invokeExpansion1(
    func: UnaryFunc,
    values: Func.Input[Target[F], nat._1]):
      Target[F] =
    func match {
      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - 12
      //   id(p, x:bar) - 18
      //   id(p, x:foo) - 1
      //   id(p, x:bar) - 2
      // (one bucket)
      case  structural.FlattenArray | structural.FlattenMap =>
        flatten(shiftValues(values(0)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - foo
      //   id(p, x:bar) - bar
      //   id(p, y:foo) - foo
      //   id(p, y:bar) - bar
      // (one bucket)
      case structural.FlattenArrayIndices | structural.FlattenMapKeys =>
        flatten(shiftIds(values(0)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - 12
      //   id(p, x, bar) - 18
      //   id(p, y, foo) - 1
      //   id(p, y, bar) - 2
      // (two buckets)
      case structural.ShiftArray | structural.ShiftMap =>
        shiftValues(values(0))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - foo
      //   id(p, x, bar) - bar
      //   id(p, y, foo) - foo
      //   id(p, y, bar) - bar
      // (two buckets)
      case structural.ShiftArrayIndices | structural.ShiftMapKeys =>
        shiftIds(values(0))
    }

  private def invokeExpansion2(func: BinaryFunc, values: Func.Input[Target[F], nat._2]):
      Target[F] =
    func match {
      case set.Range =>
        val join: AutoJoinResult = autojoin(values(0), values(1))
        val (sides, leftAccess, rightAccess) =
          concat(
            Free.point[MapFunc, JoinSide](LeftSide),
            Free.point[MapFunc, JoinSide](RightSide))

        Target(
          Ann(provenance.Nada[T]() :: prov.rebase(leftAccess, join.base.buckets), rightAccess),
          QC.inj(LeftShift(
            join.base.src,
            Free.roll(Range(join.lval, join.rval)),
            ExcludeId,
            sides)).embed)
    }

  private def invokeReduction1(
    func: UnaryFunc,
    values: Func.Input[Target[F], nat._1]):
      Target[F] = {
    val Ann(provs, reduce) = values(0).ann
    // NB: If there’s no provenance, then there’s nothing to reduce. We’re
    //     already holding a single value.
    provs.tailOption.fold(values(0)) { tail =>
      prov.genBuckets(tail) match {
        case Some((newProvs, buckets)) =>
          Target(Ann(
            prov.rebase(Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))), newProvs),
            Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
            QC.inj(Reduce[T, T[F]](
              values(0).value,
              buckets,
              List(
                ReduceFuncs.Arbitrary(buckets),
                ReduceFunc.translateUnaryReduction[FreeMap](func)(reduce)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(ReduceIndex(0)))),
                Free.roll(MakeArray(Free.point(ReduceIndex(1)))))))).embed)
        case None =>
          Target(
            Ann(provs, HoleF),
            QC.inj(Reduce[T, T[F]](
              values(0).value,
              NullLit(),
              List(ReduceFunc.translateUnaryReduction[FreeMap](func)(reduce)),
              Free.point(ReduceIndex(0)))).embed)
      }
    }
  }

  private def invokeReduction2(func: BinaryFunc, values: Func.Input[Target[F], nat._2])
      : Target[F] = {
    val join: AutoJoinResult = autojoin(values(0), values(1))

    // NB: If there’s no provenance, then there’s nothing to reduce. We’re
    //     already holding a single value.
    join.base.buckets.tailOption.fold(Target(EmptyAnn[T], join.base.src)) { tail =>
      prov.genBuckets(tail) match {
        case Some((newProvs, buckets)) =>
          Target(Ann(
            prov.rebase(Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))), newProvs),
            Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
            QC.inj(Reduce[T, T[F]](
              values(0).value,
              buckets,
              List(
                ReduceFuncs.Arbitrary(buckets),
                ReduceFunc.translateBinaryReduction[FreeMap](func)(join.lval, join.rval)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(ReduceIndex(0)))),
                Free.roll(MakeArray(Free.point(ReduceIndex(1)))))))).embed)
        case None =>
          Target(
            Ann(join.base.buckets, HoleF),
            QC.inj(Reduce[T, T[F]](
              values(0).value,
              NullLit(),
              List(ReduceFunc.translateBinaryReduction[FreeMap](func)(join.lval, join.rval)),
              Free.point(ReduceIndex(0)))).embed)
      }
    }
  }

  private def invokeThetaJoin(values: Func.Input[Target[F], nat._3], tpe: JoinType)
      : PlannerError \/ Target[F] = {
    val joinError: PlannerError \/ (ThetaJoin[T[F]] \/ JoinFunc) = {
      val join =
        QC.inj(reifyResult(values(2).ann, values(2).value))
          .embed
          .transCata[T[F]](rewrite.normalize)
          .project

      // FIXME: #1539 This won’t work where we join a collection against itself
      TJ.prj(join).fold(
        QC.prj(join) match {
          case Some(Map(_, mf)) if mf.count ≟ 0 =>
            mf.as[JoinSide](LeftSide).right[ThetaJoin[T[F]]].right[PlannerError]
          case _ =>
            InternalError.fromMsg(s"non theta join condition found: ${values(2).value.shows} with provenance: ${values(2).ann.shows}").left[ThetaJoin[T[F]] \/ JoinFunc]
        })(
        _.left[JoinFunc].right[PlannerError])
    }

    joinError.flatMap {
      case -\/(ThetaJoin(condSrc, _condL, _condR, BoolLit(true), Inner, cond)) =>

        val SrcMerge(inSrc, _inL, _inR): SrcMerge[T[F], FreeQS] =
          merge(values(0).value, values(1).value)

        val SrcMerge(resultSrc, inAccess, condAccess): SrcMerge[T[F], FreeQS] =
          merge(inSrc, condSrc)

        val inL: FreeQS = rebase(_inL, inAccess)
        val inR: FreeQS = rebase(_inR, inAccess)

        val condL: FreeQS = rebase(_condL, condAccess)
        val condR: FreeQS = rebase(_condR, condAccess)

        val Ann(leftBuckets, leftValue) = values(0).ann
        val Ann(rightBuckets, rightValue) = values(1).ann

        // NB: #1556 This is a magic structure. Improve LogicalPlan to not imply this structure.
        val combine: JoinFunc = Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit[T, JoinSide](JoinDir.Left.name), leftValue.as(LeftSide))),
          Free.roll(MakeMap(StrLit[T, JoinSide](JoinDir.Right.name), rightValue.as(RightSide)))))

        // FIXME: We shouldn't have to guess which side is which.
        // Fixing #1556 should address this issue.
        val pair: PlannerError \/ (SrcMerge[FreeQS, FreeMap], SrcMerge[FreeQS, FreeMap]) =
          joinBranches(inL, condL).fold(
            {
              _ => for {
                resL <- joinBranches(inL, condR)
                resR <- joinBranches(inR, condL)
              } yield (resL, resR)
            },
            {
              resL => for {
                resR <- joinBranches(inR, condR)
              } yield (resL, resR)
            })

        pair.map {
          case (resL, resR) =>
            // FIXME: The provenances are not correct here - need to use `resL` and `resR` provs
            Target(Ann(prov.joinProvenances(leftBuckets, rightBuckets), HoleF),
              TJ.inj(ThetaJoin(
                resultSrc,
                resL.src,
                resR.src,
                cond.flatMap {
                  case LeftSide => resL.rval.as(LeftSide)
                  case RightSide => resR.rval.as(RightSide)
                },
                tpe,
                combine.flatMap {
                  case LeftSide => resL.lval.as(LeftSide)
                  case RightSide => resR.lval.as(RightSide)
                })).embed)
        }

      case -\/(tj) =>
        InternalError.fromMsg(s"incompatible theta join found with condition ${tj.on} and join type ${tj.f}").left[Target[F]]

      case \/-(cond) =>
        val merged: SrcMerge[T[F], FreeQS] = merge(values(0).value, values(1).value)

        val Ann(leftBuckets, leftValue) = values(0).ann
        val Ann(rightBuckets, rightValue) = values(1).ann

        // NB: #1556 This is a magic structure. Improve LogicalPlan to not imply this structure.
        val combine: JoinFunc = Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit[T, JoinSide](JoinDir.Left.name), leftValue.as(LeftSide))),
          Free.roll(MakeMap(StrLit[T, JoinSide](JoinDir.Right.name), rightValue.as(RightSide)))))

        // FIXME: The provenances are not correct here
        Target(Ann(prov.joinProvenances(leftBuckets, rightBuckets), HoleF),
          TJ.inj(ThetaJoin(
            merged.src,
            merged.lval,
            merged.rval,
            cond,
            tpe,
            combine)).embed).right[PlannerError]
    }
  }

  private def ProjectTarget(prefix: Target[F], field: T[EJson]): Target[F] = {
    val Ann(provs, values) = prefix.ann
    Target(Ann(provenance.Proj(field) :: provs, HoleF),
      PB.inj(BucketField(prefix.value, values, Free.roll(Constant[T, FreeMap](field)))).embed)
  }

  private def pathToProj(path: pathy.Path[_, _, _]): Target[F] =
    pathy.Path.peel(path).fold[Target[F]](
      Target(EmptyAnn[T], DE.inj(Const[DeadEnd, T[F]](Root)).embed)) {
      case (p, n) =>
        ProjectTarget(pathToProj(p), ejson.CommonEJson(ejson.Str[T[EJson]](n.fold(_.value, _.value))).embed)
    }

  def fromData(data: Data): Data \/ T[EJson] = {
    data.hyloM[Data \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[Data \/ ?, EJson, Data, T[EJson]](
        _.left,
        _.embed.right),
      Data.toEJson[EJson].apply(_).right)
  }

  /** We carry around metadata with pointers to provenance and result value as
    * we build up QScript. At the point where a particular chain ends (E.g., the
    * final result, or the `count` of a Subset, we need to make sure we
    * extract the result pointed to by the metadata.
    */
  def reifyResult[A](ann: Ann, src: A): QScriptCore[A] =
    quasar.qscript.Map(src, ann.values)

  // TODO: Replace disjunction with validation.
  def lpToQScript: AlgebraM[PlannerError \/ ?, lp.LogicalPlan, Target[F]] = {
    case lp.Read(path) =>
      // TODO: Compilation of SQL² should insert a ShiftMap at each FROM,
      //       however doing that would break the old Mongo backend, and we can
      //       handle it here for now. But it should be moved to the SQL²
      //       compiler when the old Mongo backend is replaced. (#1298)
      shiftValues(pathToProj(path)).right

    case lp.Constant(data) =>
      fromData(data).fold[PlannerError \/ MapFunc[FreeMap]](
        {
          case Data.NA => Undefined[T, FreeMap]().right
          case d       => NonRepresentableData(d).left
        },
        Constant[T, FreeMap](_).right) ∘ (mf =>
        Target(
          Ann(Nil, Free.roll[MapFunc, Hole](mf)),
          QC.inj(Unreferenced[T, T[F]]()).embed))

    case lp.Free(name) =>
      (Planner.UnboundVariable(name): PlannerError).left[Target[F]]

    case lp.Let(name, form, body) =>
      Planner.InternalError.fromMsg("un-elided Let").left[Target[F]]

    case lp.Typecheck(expr, typ, cont, fallback) =>
      merge3Map(Func.Input3(expr, cont, fallback))(Guard(_, typ, _, _)).right[PlannerError]

    case lp.InvokeUnapply(func @ NullaryFunc(_, _, _, _), Sized())
        if func.effect ≟ Mapping =>
      Target(
        Ann(Nil, Free.roll[MapFunc, Hole](MapFunc.translateNullaryMapping(func))),
        QC.inj(Unreferenced[T, T[F]]()).embed).right

    case lp.InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Mapping =>
      val Ann(buckets, value) = a1.ann
      Target(
        Ann(buckets, Free.roll[MapFunc, Hole](MapFunc.translateUnaryMapping(func)(value))),
        a1.value).right

    case lp.InvokeUnapply(structural.ObjectProject, Sized(a1, a2)) =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      // FIXME: This is a workaround because ProjectBucket doesn’t currently
      //        propagate provenance. (#1573)
      Target(
        Ann[T](base.buckets, Free.roll(ProjectField(lval, rval))),
        base.src).right
      // (Ann[T](buckets, HoleF[T]),
      //   PB.inj(BucketField(src, lval, rval)).embed).right

    case lp.InvokeUnapply(structural.ArrayProject, Sized(a1, a2)) =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      // FIXME: This is a workaround because ProjectBucket doesn’t currently
      //        propagate provenance. (#1573)
      Target(
        Ann[T](base.buckets, Free.roll(ProjectIndex(lval, rval))),
        base.src).right
      // (Ann[T](buckets, HoleF[T]),
      //   PB.inj(BucketIndex(src, lval, rval)).embed).right

    case lp.InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Mapping =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      Target(
        Ann[T](base.buckets, Free.roll(MapFunc.translateBinaryMapping(func)(lval, rval))),
        base.src).right[PlannerError]

    case lp.InvokeUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3))
        if func.effect ≟ Mapping =>
      merge3Map(Func.Input3(a1, a2, a3))(MapFunc.translateTernaryMapping(func)).right[PlannerError]

    case lp.InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Reduction =>
      invokeReduction1(func, Func.Input1(a1)).right

    case lp.InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Reduction =>
      invokeReduction2(func, Func.Input2(a1, a2)).right

    case lp.InvokeUnapply(set.Distinct, Sized(a1)) =>
      // TODO: This currently duplicates a _portion_ of the bucket in the
      //       reducer list, we could perhaps avoid doing that, or normalize it
      //       away later.
      invokeReduction1(
        agg.Arbitrary,
        Func.Input1(
          Target(
            Ann(
              prov.swapProvenances(provenance.Value(a1.ann.values) :: a1.ann.provenance),
              a1.ann.values),
            a1.value))).right

    case lp.InvokeUnapply(set.DistinctBy, Sized(a1, a2)) =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      invokeReduction1(
        agg.Arbitrary,
        Func.Input1(
          Target(
            Ann(
              prov.swapProvenances(provenance.Value(rval) :: base.buckets),
              lval),
            base.src))).right

    case lp.InvokeUnapply(set.Sample, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge(a1.value, a2.value)

      Target(a1.ann, QC.inj(Subset(merged.src, merged.lval, Sample, Free.roll(FI.inject(QC.inj(reifyResult(a2.ann, merged.rval)))))).embed).right

    case lp.InvokeUnapply(set.Take, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge(a1.value, a2.value)

      Target(a1.ann, QC.inj(Subset(merged.src, merged.lval, Take, Free.roll(FI.inject(QC.inj(reifyResult(a2.ann, merged.rval)))))).embed).right

    case lp.InvokeUnapply(set.Drop, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge(a1.value, a2.value)

      Target(a1.ann, QC.inj(Subset(merged.src, merged.lval, Drop, Free.roll(FI.inject(QC.inj(reifyResult(a2.ann, merged.rval)))))).embed).right

    case lp.Sort(src, ords) =>
      val (kexprs, dirs) = ords.unzip
      val AutoJoinNResult(base, NonEmptyList(dset, keys)) = autojoinN(src <:: kexprs)
      val orderings = keys.toNel.flatMap(_.alignBoth(dirs).sequence) \/> InternalError.fromMsg(s"Mismatched sort keys and dirs")

      orderings map (os =>
        Target(
          Ann[T](base.buckets, dset),
          QC.inj(Sort(
            base.src,
            prov.genBuckets(base.buckets.drop(1)).fold(NullLit[T, Hole]())(_._2),
            os)).embed))

    case lp.TemporalTrunc(part, src) =>
      val Ann(buckets, value) = src.ann
      Target(
        Ann(buckets, Free.roll[MapFunc, Hole](TemporalTrunc(part, value))),
        src.value).right

    case lp.InvokeUnapply(set.Filter, Sized(a1, a2)) =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      Target(
        Ann[T](base.buckets, lval),
        QC.inj(Filter(base.src, rval)).embed).right

    case lp.InvokeUnapply(identity.Squash, Sized(a1)) =>
      val Ann(buckets, value) = a1.ann
      Target(Ann(prov.squashProvenances(buckets), value), a1.value).right

    case lp.InvokeUnapply(func @ UnaryFunc(Expansion, _, _, _, _, _, _), Sized(a1)) =>
      invokeExpansion1(func, Func.Input1(a1)).right

    case lp.InvokeUnapply(func @ BinaryFunc(Expansion, _, _, _, _, _, _), Sized(a1, a2)) =>
      invokeExpansion2(func, Func.Input2(a1, a2)).right

    case lp.InvokeUnapply(set.GroupBy, Sized(a1, a2)) =>
      val join: AutoJoinResult = autojoin(a1, a2)
      Target(Ann(prov.swapProvenances(provenance.Value(join.rval) :: join.base.buckets), join.lval), join.base.src).right

    case lp.InvokeUnapply(set.Union, Sized(a1, a2)) =>
      val SrcMerge(src, lfree, rfree) = merge(a1.value, a2.value)
      val lbranch = Free.roll(FI.inject(QC.inj(reifyResult(a1.ann, lfree))))
      val rbranch = Free.roll(FI.inject(QC.inj(reifyResult(a2.ann, rfree))))

      // TODO: Need to align provenances, so each component is at the same
      //       location on both sides
      (prov.genBuckets(a1.ann.provenance), prov.genBuckets(a2.ann.provenance)) match {
        case (None, None) =>
          Target(
            Ann[T](prov.unionProvenances(a1.ann.provenance, a2.ann.provenance), HoleF),
            QC.inj(Union(src, lbranch, rbranch)).embed).right
        case (None, Some((rProvs, rBuck))) =>
          val (merged, rbacc, vacc) = concat(rBuck, HoleF[T])
          Target(
            Ann(prov.unionProvenances(a1.ann.provenance, prov.rebase(rbacc, rProvs)), vacc),
            QC.inj(Union(src,
              Free.roll(FI.inject(QC.inj(Map(lbranch, merged)))),
              Free.roll(FI.inject(QC.inj(Map(rbranch, merged)))))).embed).right
        case (Some((lProvs, lBuck)), None) =>
          val (merged, lbacc, vacc) = concat(lBuck, HoleF[T])
          Target(
            Ann(prov.unionProvenances(prov.rebase(lbacc, lProvs), a2.ann.provenance), vacc),
            QC.inj(Union(src,
              Free.roll(FI.inject(QC.inj(Map(lbranch, merged)))),
              Free.roll(FI.inject(QC.inj(Map(rbranch, merged)))))).embed).right
        case (Some((lProvs, lBuck)), Some((rProvs, rBuck))) =>
          val (merged, lbacc, lvacc) = concat(lBuck, HoleF[T])
          val (_, rbacc, rvacc) = concat(rBuck, HoleF[T])
          (lbacc ≟ rbacc && lvacc ≟ rvacc).fold(
            Target(
              Ann(prov.unionProvenances(prov.rebase(lbacc, lProvs), prov.rebase(rbacc, rProvs)), lvacc),
              QC.inj(Union(src,
                Free.roll(FI.inject(QC.inj(Map(lbranch, merged)))),
                Free.roll(FI.inject(QC.inj(Map(rbranch, merged)))))).embed).right,
            InternalError.fromMsg("unaligned union provenances").left)
      }

    case lp.InvokeUnapply(set.Intersect, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge(a1.value, a2.value)

      Target(Ann(prov.joinProvenances(a1.ann.provenance, a2.ann.provenance), HoleF),
        TJ.inj(ThetaJoin(
          merged.src,
          merged.lval,
          merged.rval,
          Free.roll(Eq(Free.point(LeftSide), Free.point(RightSide))),
          Inner,
          LeftSideF)).embed).right

    case lp.InvokeUnapply(set.Except, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge(a1.value, a2.value)

      Target(Ann(a1.ann.provenance, HoleF),
        TJ.inj(ThetaJoin(
          merged.src,
          merged.lval,
          merged.rval,
          BoolLit(false),
          LeftOuter,
          LeftSideF)).embed).right

    case lp.InvokeUnapply(func @ TernaryFunc(Transformation, _, _, _, _, _, _), Sized(a1, a2, a3)) =>
      invokeThetaJoin(
        Func.Input3(a1, a2, a3),
        func match {
          case set.InnerJoin      => Inner
          case set.LeftOuterJoin  => LeftOuter
          case set.RightOuterJoin => RightOuter
          case set.FullOuterJoin  => FullOuter
        })
  }
}
