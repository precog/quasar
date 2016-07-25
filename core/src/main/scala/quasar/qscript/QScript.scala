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

import quasar._
import quasar.ejson.{Int => _, _}
import quasar.fp._
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._
import quasar.Planner._
import quasar.Predef._
import quasar.std.StdLib._

import matryoshka._, Recursive.ops._, FunctorT.ops._, TraverseT.nonInheritedOps._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _}, Scalaz._, Inject._, Leibniz._
import shapeless.{nat, Sized}

// Need to keep track of our non-type-ensured guarantees:
// - all conditions in a ThetaJoin will refer to both sides of the join
// - each `Free` structure in a *Join or Union will have exactly one `point`
// - the common source in a Join or Union will be the longest common branch
// - all Reads have a Root (or another Read?) as their source
// - in `Pathable`, the only `MapFunc` node allowed is a `ProjectField`

sealed abstract class JoinType
final case object Inner extends JoinType
final case object FullOuter extends JoinType
final case object LeftOuter extends JoinType
final case object RightOuter extends JoinType

object JoinType {
  implicit val equal: Equal[JoinType] = Equal.equalRef
  implicit val show: Show[JoinType] = Show.showFromToString
}

trait Helpers[T[_[_]]] {
  def equiJF: JoinFunc[T] =
    Free.roll(Eq(Free.point(LeftSide), Free.point(RightSide)))
}

// TODO: Could maybe require only Functor[F], once CoEnv exposes the proper
//       instances
class Transform[T[_[_]]: Recursive: Corecursive: FunctorT: EqualT: ShowT, F[_]: Traverse: Normalizable](
  implicit DE: Const[DeadEnd, ?] :<: F,
           SP: SourcedPathable[T, ?] :<: F,
           QC: QScriptCore[T, ?] :<: F,
           TJ: ThetaJoin[T, ?] :<: F,
           PB: ProjectBucket[T, ?] :<: F,
           // TODO: Remove this one once we have multi-sorted AST
           FI: F :<: QScriptProject[T, ?],
           mergeable:  Mergeable.Aux[T, F],
           eq:         Delay[Equal, F],
           show:       Delay[Show, F])
    extends Helpers[T] {

  val prov = new Provenance[T]

  type Target[A] = EnvT[Ann[T], F, A]
  type TargetT = Target[T[Target]]
  type FreeEnv = Free[Target, Hole]

  def DeadEndTarget(deadEnd: DeadEnd): TargetT =
    EnvT[Ann[T], F, T[Target]]((EmptyAnn[T], DE.inj(Const[DeadEnd, T[Target]](deadEnd))))

  val RootTarget: TargetT = DeadEndTarget(Root)
  val EmptyTarget: TargetT = DeadEndTarget(Empty)

  type Envs = List[Target[Hole]]

  case class ZipperSides(
    lSide: FreeMap[T],
    rSide: FreeMap[T])

  case class ZipperTails(
    lTail: Envs,
    rTail: Envs)

  case class ZipperAcc(
    acc: Envs,
    sides: ZipperSides,
    tails: ZipperTails)

  def linearize[F[_]: Functor: Foldable]: Algebra[F, List[F[Hole]]] =
    fl => fl.map(_ => SrcHole: Hole) :: fl.fold

  def linearizeEnv[E, F[_]: Functor: Foldable]:
      Algebra[EnvT[E, F, ?], List[EnvT[E, F, Hole]]] =
    fl => fl.map(_ => SrcHole: Hole) :: fl.lower.fold


  def delinearizeInner[A]: Coalgebra[Target, List[Target[A]]] = {
    case Nil => EnvT((EmptyAnn, DE.inj(Const[DeadEnd, List[Target[A]]](Root))))
    case h :: t => h.map(_ => t)
  }

  def delinearizeTargets[F[_]: Functor, A]:
      ElgotCoalgebra[Hole \/ ?, Target, List[Target[A]]] = {
    case Nil    => SrcHole.left[Target[List[Target[A]]]]
    case h :: t => h.map(_ => t).right
  }

  val consZipped: Algebra[ListF[Target[Hole], ?], ZipperAcc] = {
    case NilF() => ZipperAcc(Nil, ZipperSides(HoleF[T], HoleF[T]), ZipperTails(Nil, Nil))
    case ConsF(head, ZipperAcc(acc, sides, tails)) => ZipperAcc(head :: acc, sides, tails)
  }

  val zipper: ElgotCoalgebra[
      ZipperAcc \/ ?,
      ListF[Target[Hole], ?],
      (ZipperSides, ZipperTails)] = {
    case (zs @ ZipperSides(lm, rm), zt @ ZipperTails(l :: ls, r :: rs)) =>
      mergeable.mergeSrcs(lm, rm, l, r).fold[ZipperAcc \/ ListF[Target[Hole], (ZipperSides, ZipperTails)]](
        ZipperAcc(Nil, zs, zt).left) {
        case SrcMerge(inn, lmf, rmf) =>
          ConsF(inn, (ZipperSides(lmf, rmf), ZipperTails(ls, rs))).right[ZipperAcc]
      }
    case (sides, tails) =>
      ZipperAcc(Nil, sides, tails).left
  }

  type MergeResult = (T[Target], FreeMap[T], FreeMap[T], Free[Target, Hole], Free[Target, Hole])

  def merge(left: T[Target], right: T[Target]): MergeResult = {
    val lLin: Envs = left.cata(linearizeEnv).reverse
    val rLin: Envs = right.cata(linearizeEnv).reverse

    val ZipperAcc(common, ZipperSides(lMap, rMap), ZipperTails(lTail, rTail)) =
      elgot(
        (ZipperSides(HoleF[T], HoleF[T]), ZipperTails(lLin, rLin)))(
        consZipped, zipper)

    val leftF =
      foldIso(CoEnv.freeIso[Hole, Target])
        .get(lTail.reverse.ana[T, CoEnv[Hole, Target, ?]](delinearizeTargets[F, Hole] >>> (CoEnv(_))))

    val rightF =
      foldIso(CoEnv.freeIso[Hole, Target])
        .get(rTail.reverse.ana[T, CoEnv[Hole, Target, ?]](delinearizeTargets[F, Hole] >>> (CoEnv(_))))

    val commonSrc: T[Target] =
      common.reverse.ana[T, Target](delinearizeInner)

    (commonSrc, lMap, rMap, leftF, rightF)
  }

  def rebaseBranch(br: Free[Target, Hole], fm: FreeMap[T]):
      Free[Target, Hole] =
    // TODO: Special-casing the `point` case should be unnecessary after
    //       optimizations are applied across all branches.
    if (fm ≟ Free.point(SrcHole))
      br
    else
      br >> Free.roll(EnvT((EmptyAnn[T], QC.inj(Map(Free.point[Target, Hole](SrcHole), fm)))))

  def useMerge(
    res: MergeResult,
    ap: (T[Target], Free[Target, Hole], Free[Target, Hole]) => (F[T[Target]], List[FreeMap[T]], FreeMap[T], FreeMap[T])):
      (F[T[Target]], List[FreeMap[T]], FreeMap[T], FreeMap[T]) = {

    val (src, lMap, rMap, lBranch, rBranch) = res

    if (lBranch ≟ Free.point(SrcHole) && rBranch ≟ Free.point(SrcHole)) {
      val (buck, newBucks) = concatBuckets(src.project.ask.provenance)
      val (mf, baccess, laccess, raccess) = concat3(buck, lMap, rMap)
      (QC.inj(Map(src, mf)), newBucks.map(_ >> baccess), laccess, raccess)
    } else {
      val leftRebase = rebaseBranch(lBranch, lMap)
      val rightRebase = rebaseBranch(rBranch, rMap)
      ap(src, leftRebase, rightRebase)
    }
  }

  def someAnn[A](
    v: Target[Free[Target, A]] \/ A,
    default: T[Target]):
      Ann[T] =
    v.fold(_.ask, κ(default.project.ask))

  /** This unifies a pair of sources into a single one, with additional
    * expressions to access the combined bucketing info, as well as the left and
    * right values.
    */
  def autojoin(left: T[Target], right: T[Target]):
      (F[T[Target]], List[FreeMap[T]], FreeMap[T], FreeMap[T]) = {
    useMerge(merge(left, right), (src, lBranch, rBranch) => {
      val lcomp = lBranch.resume
      val rcomp = rBranch.resume
      val (combine, lacc, racc) =
        concat[T, JoinSide](Free.point(LeftSide), Free.point(RightSide))

      val lann = someAnn(lcomp, src)
      val rann = someAnn(rcomp, src)
      val commonProvLength = src.project.ask.provenance.length

      (TJ.inj(ThetaJoin(
        src,
        lBranch.mapSuspension(FI.compose(envtLowerNT)),
        rBranch.mapSuspension(FI.compose(envtLowerNT)),
        // FIXME: not quite right – e.g., if there is a reduction in a branch the
        //        condition won’t line up.
        Free.roll(Eq(
          concatBuckets(lann.provenance.drop(lann.provenance.length - commonProvLength))._1.map(κ(LeftSide)),
          concatBuckets(rann.provenance.drop(rann.provenance.length - commonProvLength))._1.map(κ(RightSide)))),
        Inner,
        combine)),
        prov.joinProvenances(
          lann.provenance.map(_ >> lacc),
          rann.provenance.map(_ >> racc)),
        lann.values >> lacc,
        rann.values >> racc)
    })
  }

  /** A convenience for a pair of autojoins, does the same thing, but returns
    * access to all three values.
    */
  def autojoin3(left: T[Target], center: T[Target], right: T[Target]):
      (F[T[Target]], List[FreeMap[T]], FreeMap[T], FreeMap[T], FreeMap[T]) = {
    val (lsrc, lbuckets, lval, cval) = autojoin(left, center)
    val (fullSrc, fullBuckets, bval, rval) =
      autojoin(EnvT((Ann[T](lbuckets, HoleF), lsrc)).embed, right)

    (fullSrc, fullBuckets, bval >> lval, bval >> cval, rval)
  }

  def merge2Map(
    values: Func.Input[T[Target], nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      TargetT = {
    val (src, buckets, lval, rval) = autojoin(values(0), values(1))
    val (bucks, newBucks) = concatBuckets(buckets)
    val (merged, b, v) = concat(bucks, func(lval, rval).embed)

    EnvT((
      Ann[T](newBucks.map(_ >> b), v),
      QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, merged))))
  }

  // TODO unify with `merge2Map`
  def merge3Map(
    values: Func.Input[T[Target], nat._3])(
    func: (FreeMap[T], FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      Target[T[Target]] = {
    val (src, buckets, lval, cval, rval) = autojoin3(values(0), values(1), values(2))
    val (bucks, newBucks) = concatBuckets(buckets)
    val (merged, b, v) = concat(bucks, func(lval, cval, rval).embed)

    EnvT((
      Ann[T](newBucks.map(_ >> b), v),
      QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, merged))))
  }

  def shiftValues(input: T[Target], f: FreeMap[T] => MapFunc[T, FreeMap[T]]):
      Target[T[Target]] = {
    val Ann(provs, value) = input.project.ask
    val (merged, shiftAccess, valAccess) = concat(Free.roll(f(value)), value)
    val (sides, leftAccess, rightAccess) =
      concat(
        Free.point[MapFunc[T, ?], JoinSide](LeftSide),
        Free.point[MapFunc[T, ?], JoinSide](RightSide))

    EnvT((
      Ann(
        prov.shiftMap(shiftAccess >> rightAccess) :: provs.map(_ >> leftAccess),
        valAccess >> rightAccess),
      SP.inj(LeftShift(input, merged, sides))))
  }

  def shiftIds(input: T[Target], f: FreeMap[T] => MapFunc[T, FreeMap[T]]):
      Target[T[Target]] = {
    val Ann(provs, value) = input.project.ask
    val (sides, leftAccess, rightAccess) =
      concat(
        Free.point[MapFunc[T, ?], JoinSide](LeftSide),
        Free.point[MapFunc[T, ?], JoinSide](RightSide))

    EnvT((
      Ann(
        prov.shiftMap(rightAccess) :: provs.map(_ >> leftAccess),
        rightAccess),
      SP.inj(LeftShift(input, Free.roll(f(value)), sides))))
  }

  def flatten(input: Target[T[Target]]): Target[T[Target]] = {
    val EnvT((Ann(buckets, value), fa)) = input
    EnvT((Ann(prov.nestProvenances(buckets), value), fa))
  }

  // NB: More complicated LeftShifts are generated as an optimization:
  // before: ThetaJoin(cs, Map(Hole, mf), LeftShift(Hole, struct, repair), comb)
  // after: LeftShift(cs, struct, comb.flatMap(LeftSide => mf.map(_ => LeftSide), RS => repair))
  def invokeExpansion1(
    func: UnaryFunc,
    values: Func.Input[T[Target], nat._1]):
      Target[T[Target]] =
    func match {
      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - 12
      //   id(p, x:bar) - 18
      //   id(p, x:foo) - 1
      //   id(p, x:bar) - 2
      // (one bucket)
      case structural.FlattenMap =>
        flatten(shiftValues(values(0), DupMapKeys(_)))
      case structural.FlattenArray =>
        flatten(shiftValues(values(0), DupArrayIndices(_)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - foo
      //   id(p, x:bar) - bar
      //   id(p, y:foo) - foo
      //   id(p, y:bar) - bar
      // (one bucket)
      case structural.FlattenMapKeys =>
        flatten(shiftIds(values(0), DupMapKeys(_)))
      case structural.FlattenArrayIndices =>
        flatten(shiftIds(values(0), DupArrayIndices(_)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - 12
      //   id(p, x, bar) - 18
      //   id(p, y, foo) - 1
      //   id(p, y, bar) - 2
      // (two buckets)
      case structural.ShiftMap   => shiftValues(values(0), DupMapKeys(_))
      case structural.ShiftArray => shiftValues(values(0), DupArrayIndices(_))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - foo
      //   id(p, x, bar) - bar
      //   id(p, y, foo) - foo
      //   id(p, y, bar) - bar
      // (two buckets)
      case structural.ShiftMapKeys      => shiftIds(values(0), DupMapKeys(_))
      case structural.ShiftArrayIndices => shiftIds(values(0), DupArrayIndices(_))
    }

  def invokeExpansion2(func: BinaryFunc, values: Func.Input[T[Target], nat._2]):
      TargetT =
    func match {
      case set.Range =>
        val (src, buckets, lval, rval) = autojoin(values(0), values(1))
        val (sides, leftAccess, rightAccess) =
          concat(
            Free.point[MapFunc[T, ?], JoinSide](LeftSide),
            Free.point[MapFunc[T, ?], JoinSide](RightSide))

        EnvT((
          Ann[T](
            NullLit[T, Hole]() :: buckets.map(_ >> leftAccess),
            rightAccess),
          SP.inj(LeftShift(
            EnvT((EmptyAnn[T], src)).embed,
            Free.roll(Range(lval, rval)),
            sides))))
    }

  def invokeReduction1(
    func: UnaryFunc,
    values: Func.Input[T[Target], nat._1]):
      TargetT = {
    val Ann(provs, reduce) = values(0).project.ask
    // NB: If there’s no provenance, then there’s nothing to reduce. We’re
    //     already holding a single value.
    provs.tailOption.fold(values(0).project) { tail =>
      val (newProvs, provAccess) = concatBuckets(tail)

      EnvT[Ann[T], F, T[Target]]((
        Ann[T](
          provAccess.map(_ >> Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
        QC.inj(Reduce[T, T[Target]](
          values(0),
          newProvs,
          List(
            ReduceFuncs.Arbitrary(newProvs),
            ReduceFunc.translateUnaryReduction[FreeMap[T]](func)(reduce)),
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(Free.point(ReduceIndex(0)))),
            Free.roll(MakeArray(Free.point(ReduceIndex(1))))))))))
    }
  }

  def invokeReduction2(
    func: BinaryFunc,
    values: Func.Input[T[Target], nat._2]):
      TargetT = {
    val (src, provs, lMap, rMap) = autojoin(values(0), values(1))

    // NB: If there’s no provenance, then there’s nothing to reduce. We’re
    //     already holding a single value.
    provs.tailOption.fold(EnvT((EmptyAnn[T], src))) { tail =>
      val (newProvs, provAccess) = concatBuckets(tail)

      EnvT[Ann[T], F, T[Target]]((
        Ann[T](
          provAccess.map(_ >> Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
          Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
        QC.inj(Reduce[T, T[Target]](
          values(0),
          newProvs,
          List(
            ReduceFuncs.Arbitrary(newProvs),
            ReduceFunc.translateBinaryReduction[FreeMap[T]](func)(lMap, rMap)),
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(Free.point(ReduceIndex(0)))),
            Free.roll(MakeArray(Free.point(ReduceIndex(1))))))))))
    }
  }

  def invokeThetaJoin(
    values: Func.Input[T[Target], nat._3],
    tpe: JoinType):
      PlannerError \/ TargetT = {
    val condError: PlannerError \/ JoinFunc[T] = {
      // FIXME: This won’t work where we join a collection against itself
      //        We only apply _some_ optimizations at this point to maintain the
      //        TJ at the end, but that‘s still not guaranteed
      TJ.prj(values(2).transCata[F](_.lower).transCata((new Optimize[T]).applyMost[F]).project).fold(
        (InternalError("non theta join condition found"): PlannerError).left[JoinFunc[T]])(
        _.combine.right[PlannerError])
    }

    // NB: This is a magic structure. Improve LP to not imply this structure.
    val combine: JoinFunc[T] = Free.roll(ConcatMaps(
      Free.roll(MakeMap(StrLit[T, JoinSide]("left"), Free.point[MapFunc[T, ?], JoinSide](LeftSide))),
      Free.roll(MakeMap(StrLit[T, JoinSide]("right"), Free.point[MapFunc[T, ?], JoinSide](RightSide)))))

    condError.map { cond =>
      val (commonSrc, lMap, rMap, leftSide, rightSide) = merge(values(0), values(1))
      val Ann(leftBuckets, leftValue) = leftSide.resume.fold(_.ask, _ => EmptyAnn[T])
      val Ann(rightBuckets, rightValue) = leftSide.resume.fold(_.ask, _ => EmptyAnn[T])

      val buckets: List[FreeMap[T]] =
        prov.joinProvenances(leftBuckets, rightBuckets)

      // cond >>= {
      //   case LeftSide => leftValue.map(κ(LeftSide))
      //   case RightSide => rightValue.map(κ(RightSide))
      // }

      EnvT((
        Ann[T](buckets, HoleF[T]),
        TJ.inj(ThetaJoin(
          commonSrc,
          rebaseBranch(leftSide, lMap).mapSuspension(FI.compose(envtLowerNT)),
          rebaseBranch(rightSide, rMap).mapSuspension(FI.compose(envtLowerNT)),
          cond,
          tpe,
          combine))))
     }
   }

  def ProjectTarget(prefix: TargetT, field: FreeMap[T]): TargetT = {
    val Ann(provenance, values) = prefix.ask
    EnvT[Ann[T], F, T[Target]]((
      Ann[T](prov.projectField(field) :: provenance, values),
      PB.inj(BucketField(prefix.embed, HoleF[T], field))))
  }

  def pathToProj(path: pathy.Path[_, _, _]): TargetT =
    pathy.Path.peel(path).fold[TargetT](
      RootTarget) {
      case (p, n) =>
        ProjectTarget(pathToProj(p), StrLit(n.fold(_.value, _.value)))
    }

  def fromData[T[_[_]]: Corecursive](data: Data): PlannerError \/ T[EJson] = {
    data.hyloM[PlannerError \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[PlannerError \/ ?, EJson, Data, T[EJson]](
        NonRepresentableData(_).left,
        _.embed.right[PlannerError]),
      Data.toEJson[EJson].apply(_).right)
  }

  // TODO: Replace disjunction with validation.
  def lpToQScript: LogicalPlan[T[Target]] => PlannerError \/ TargetT = {
    case LogicalPlan.ReadF(path) =>
      pathToProj(path).right

    case LogicalPlan.ConstantF(data) =>
      fromData(data).map(d =>
        EnvT((
          EmptyAnn[T],
          QC.inj(Map(
            RootTarget.embed,
            Free.roll[MapFunc[T, ?], Hole](Nullary[T, FreeMap[T]](d)))))))

    case LogicalPlan.FreeF(name) =>
      (Planner.UnboundVariable(name): PlannerError).left[TargetT]

    case LogicalPlan.LetF(name, form, body) =>
      (Planner.InternalError("un-elided Let"): PlannerError).left[TargetT]

    case LogicalPlan.TypecheckF(expr, typ, cont, fallback) =>
      merge3Map(Func.Input3(expr, cont, fallback))(Guard(_, typ, _, _)).right[PlannerError]

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Mapping =>
      val Ann(buckets, value) = a1.project.ask
      val (buck, newBuckets) = concatBuckets(buckets)
      val (mf, bucketAccess, valAccess) =
        concat[T, Hole](buck, Free.roll(MapFunc.translateUnaryMapping(func)(HoleF[T])))

      EnvT((
        Ann(newBuckets.map(_ >> bucketAccess), valAccess),
        QC.inj(Map(a1, mf)))).right

    case LogicalPlan.InvokeFUnapply(structural.ObjectProject, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      EnvT((
        Ann[T](buckets, HoleF[T]),
        PB.inj(BucketField(EnvT((EmptyAnn[T], src)).embed, lval, rval)))).right

    case LogicalPlan.InvokeFUnapply(structural.ArrayProject, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      EnvT((
        Ann[T](buckets, HoleF[T]),
        PB.inj(BucketIndex(EnvT((EmptyAnn[T], src)).embed, lval, rval)))).right

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Mapping =>
      merge2Map(Func.Input2(a1, a2))(MapFunc.translateBinaryMapping(func)).right[PlannerError]

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))
        if func.effect ≟ Mapping =>
      merge3Map(Func.Input3(a1, a2, a3))(MapFunc.translateTernaryMapping(func)).right[PlannerError]

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Reduction =>
      invokeReduction1(func, Func.Input1(a1)).right

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Reduction =>
      invokeReduction2(func, Func.Input2(a1, a2)).right

    case LogicalPlan.InvokeFUnapply(set.Take, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      val left: F[Free[F, Hole]] = QC.inj(Map[T, Free[F, Hole]](Free.point[F, Hole](SrcHole), lval))
      val right: F[Free[F, Hole]] = QC.inj(Map[T, Free[F, Hole]](Free.point[F, Hole](SrcHole), rval))
      EnvT((
        Ann[T](buckets, HoleF[T]),
        QC.inj(Take(
          EnvT((EmptyAnn[T], src)).embed,
          Free.roll(left).mapSuspension(FI),
          Free.roll(left).mapSuspension(FI))))).right

    case LogicalPlan.InvokeFUnapply(set.Drop, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      val left: F[Free[F, Hole]] = QC.inj(Map[T, Free[F, Hole]](Free.point[F, Hole](SrcHole), lval))
      val right: F[Free[F, Hole]] = QC.inj(Map[T, Free[F, Hole]](Free.point[F, Hole](SrcHole), rval))
      EnvT((
        Ann[T](buckets, HoleF[T]),
        QC.inj(Drop(
          EnvT((EmptyAnn[T], src)).embed,
          Free.roll(left).mapSuspension(FI),
          Free.roll(left).mapSuspension(FI))))).right

    case LogicalPlan.InvokeFUnapply(set.OrderBy, Sized(a1, a2, a3)) =>
      val (src, bucketsSrc, ordering, buckets, directions) = autojoin3(a1, a2, a3)

      // The ana over the freeIso converts from Free to T[CoEnv]. It’s the first step of freeTransCata.
      val bucketsList: List[FreeMap[T]] = buckets.ana(CoEnv.freeIso[Hole, MapFunc[T, ?]].reverseGet).project match {
        case StaticArray(as) => as.map(_.cata(CoEnv.freeIso[Hole, MapFunc[T, ?]].get))
        case mf => List(mf.embed.cata(CoEnv.freeIso[Hole, MapFunc[T, ?]].get))
      }

      val directionsList: PlannerError \/ List[SortDir] = {
        val orderStrs: PlannerError \/ List[String] =
          directions.ana(CoEnv.freeIso[Hole, MapFunc[T, ?]].reverseGet).project match {
            case StaticArray(as) => {
              as.traverse(x => StrLit.unapply(x.project)) \/> InternalError("unsupported ordering type")
            }
            case StrLit(str) => List(str).right
            case _ => InternalError("unsupported ordering function").left
          }
        orderStrs.flatMap {
          _.traverse {
            case "ASC" => SortDir.Ascending.right
            case "DESC" => SortDir.Descending.right
            case _ => InternalError("unsupported ordering direction").left
          }
        }
      }

      val lists: PlannerError \/ List[(FreeMap[T], SortDir)] =
        directionsList.map { bucketsList.zip(_) }

      lists.map(pairs =>
        EnvT((
          Ann[T](bucketsSrc, HoleF[T]),
          QC.inj(Sort(
            EnvT((EmptyAnn[T], src)).embed,
            ordering,
            pairs)))))

    case LogicalPlan.InvokeFUnapply(set.Filter, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      EnvT((
        Ann[T](buckets, HoleF[T]),
        QC.inj(Map(
          EnvT((
            EmptyAnn[T],
            QC.inj(Filter(EnvT((EmptyAnn[T], src)).embed, rval)))).embed,
          lval)))).right

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Squashing =>
      val Ann(buckets, value) = a1.project.ask
      val (buck, newBuckets) = concatBuckets(prov.squashProvenances(buckets))
      val (mf, buckAccess, valAccess) = concat(buck, value)

      EnvT((
        Ann(newBuckets.map(_ >> buckAccess), valAccess),
        QC.inj(Map(a1, mf)))).right

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Expansion =>
      invokeExpansion1(func, Func.Input1(a1)).right

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Expansion =>
      invokeExpansion2(func, Func.Input2(a1, a2)).right

    case LogicalPlan.InvokeFUnapply(set.GroupBy, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      EnvT((Ann(prov.swapProvenances(rval :: buckets), lval), src)).right

    case LogicalPlan.InvokeFUnapply(set.Union, Sized(a1, a2)) =>
      val (qs, buckets, lacc, racc) = useMerge(merge(a1, a2), (src, lfree, rfree) => {
        (SP.inj(Union(src,
          lfree.mapSuspension(FI.compose(envtLowerNT)),
          rfree.mapSuspension(FI.compose(envtLowerNT)))),
          prov.unionProvenances(
            someAnn(lfree.resume, src).provenance,
            someAnn(rfree.resume, src).provenance),
          HoleF,
          HoleF)
      })

      EnvT((Ann(buckets, HoleF), qs)).right

    case LogicalPlan.InvokeFUnapply(set.Intersect, Sized(a1, a2)) =>
      val (src, lMap, rMap, left, right) = merge(a1, a2)
      EnvT((
        src.project.ask,
        TJ.inj(ThetaJoin(
          src,
          rebaseBranch(left, lMap).mapSuspension(FI.compose(envtLowerNT)),
          rebaseBranch(right, rMap).mapSuspension(FI.compose(envtLowerNT)),
          equiJF,
          Inner,
          Free.point(LeftSide))))).right

    case LogicalPlan.InvokeFUnapply(set.Except, Sized(a1, a2)) =>
      val (src, lMap, rMap, left, right) = merge(a1, a2)
      EnvT((
        left.resume.fold(_.ask, κ(src.project.ask)),
        TJ.inj(ThetaJoin(
          src,
          rebaseBranch(left, lMap).mapSuspension(FI.compose(envtLowerNT)),
          rebaseBranch(right, rMap).mapSuspension(FI.compose(envtLowerNT)),
          Free.roll(Nullary(CommonEJson.inj(ejson.Bool[T[EJson]](false)).embed)),
          LeftOuter,
          Free.point(LeftSide))))).right

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))
        if func.effect ≟ Transformation =>
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

class Optimize[T[_[_]]: Recursive: Corecursive: EqualT: ShowT] extends Helpers[T] {

  // TODO: These optimizations should give rise to various property tests:
  //       • elideNopMap ⇒ no `Map(???, HoleF)`
  //       • normalize ⇒ a whole bunch, based on MapFuncs
  //       • elideNopJoin ⇒ no `ThetaJoin(???, HoleF, HoleF, LeftSide === RightSide, ???, ???)`
  //       • coalesceMaps ⇒ no `Map(Map(???, ???), ???)`
  //       • coalesceMapJoin ⇒ no `Map(ThetaJoin(???, …), ???)`

  // TODO: Turn `elideNop` into a type class?
  // def elideNopFilter[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F):
  //     QScriptCore[T, T[F]] => F[T[F]] = {
  //   case Filter(src, Patts.True) => src.project
  //   case qc                      => QC.inj(qc)
  // }

  def elideNopMap[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[F]] => F[T[F]] = {
    case Map(src, mf) if mf ≟ HoleF => src.project
    case x                          => QC.inj(x)
  }

  def elideNopMapCo[F[_]: Functor, A](implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[CoEnv[A, F, ?]]] => CoEnv[A, F, T[CoEnv[A, F, ?]]] = {
    case Map(src, mf) if mf ≟ HoleF => src.project
    case x                          => CoEnv(QC.inj(x).right)
  }

  // FIXME: This really needs to ensure that the condition is that of an
  //        autojoin, otherwise it’ll elide things that are truly meaningful.
  def elideNopJoin[F[_]](
    implicit TJ: ThetaJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F, FI: F :<: QScriptProject[T, ?]):
      ThetaJoin[T, ?] ~> F =
    new (ThetaJoin[T, ?] ~> F) {
      def apply[A](tj: ThetaJoin[T, A]) = tj match {
        case ThetaJoin(src, l, r, on, Inner, combine)
            if l ≟ Free.point(SrcHole) && r ≟ Free.point(SrcHole) && on ≟ equiJF =>
          QC.inj(Map(src, combine.map(_ => SrcHole: Hole)))
        case x @ ThetaJoin(src, l, r, on, _, combine) if on ≟ BoolLit(true) =>
          (l.resume.leftMap(_.map(_.resume)), r.resume.leftMap(_.map(_.resume))) match {
            case (-\/(m1), -\/(m2)) => (FI.prj(m1) >>= QC.prj, FI.prj(m2) >>= QC.prj) match {
              case (Some(Map(\/-(SrcHole), mf1)), Some(Map(\/-(SrcHole), mf2))) =>
                QC.inj(Map(src, combine >>= {
                  case LeftSide  => mf1
                  case RightSide => mf2
                }))
              case (_, _) => TJ.inj(x)
            }
            case (-\/(m1), \/-(SrcHole)) => (FI.prj(m1) >>= QC.prj) match {
              case Some(Map(\/-(SrcHole), mf1)) =>
                QC.inj(Map(src, combine >>= {
                  case LeftSide  => mf1
                  case RightSide => HoleF
                }))
              case _ => TJ.inj(x)
            }
            case (\/-(SrcHole), -\/(m2)) => (FI.prj(m2) >>= QC.prj) match {
              case Some(Map(\/-(SrcHole), mf2)) =>
                QC.inj(Map(src, combine >>= {
                  case LeftSide  => HoleF
                  case RightSide => mf2
                }))
              case _ => TJ.inj(x)
            }
            case (_, _) => TJ.inj(x)
          }
        case x => TJ.inj(x)
      }
    }

  def rebaseT[F[_]: Traverse](target: FreeQS[T], src: T[F])(implicit FI: F :<: QScriptProject[T, ?]): Option[T[F]] =
    (target.map(_ => src.transAna(FI))).ana(CoEnv.freeIso[T[QScriptProject[T, ?]], QScriptProject[T, ?]].reverseGet).cata(recover(_.embed)).transAnaM(FI.prj)

  def elideConstantJoin[F[_]: Traverse](
    implicit TJ: ThetaJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F, FI: F :<: QScriptProject[T, ?]):
      ThetaJoin[T, T[F]] => F[T[F]] = {
    case x @ ThetaJoin(src0, l, r, on, Inner, combine) if on ≟ BoolLit(true) =>
      (l.resume.leftMap(_.map(_.resume)), r.resume.leftMap(_.map(_.resume))) match {
        case (-\/(m1), -\/(m2)) => (FI.prj(m1) >>= QC.prj, FI.prj(m2) >>= QC.prj) match {
          case (Some(Map(\/-(SrcHole), mf1)), Some(Map(\/-(SrcHole), mf2))) =>  // both sides are a Map
            (mf1.resume, mf2.resume) match { // if both sides are Nullary, we hit the first case
              case (-\/(Nullary(_)), _) =>
                rebaseT(r, src0).map(tf => QC.inj(Map(tf, combine >>= {
                  case LeftSide  => mf1
                  case RightSide => HoleF
                }))).getOrElse(TJ.inj(x))
              case (_, -\/(Nullary(_))) =>
                rebaseT(l, src0).map(tf => QC.inj(Map(tf, combine >>= {
                  case LeftSide  => HoleF
                  case RightSide => mf2
                }))).getOrElse(TJ.inj(x))
              case (_, _) => TJ.inj(x)
            }
          case (Some(Map(\/-(SrcHole), mf1)), _) =>  // left side is a Map
            mf1.resume match {
              case -\/(Nullary(_)) =>
                rebaseT(r, src0).map(tf => QC.inj(Map(tf, combine >>= {
                  case LeftSide  => mf1
                  case RightSide => HoleF
                }))).getOrElse(TJ.inj(x))
              case _ => TJ.inj(x)
            }
          case (_, Some(Map(\/-(SrcHole), mf2))) =>  // right side is a Map
            mf2.resume match {
              case -\/(Nullary(_)) =>
                rebaseT(l, src0).map(tf => QC.inj(Map(tf, combine >>= {
                  case LeftSide  => HoleF
                  case RightSide => mf2
                }))).getOrElse(TJ.inj(x))
              case _ => TJ.inj(x)
            }
          case (_, _)=> TJ.inj(x)
        }
        case (_, _) => TJ.inj(x)
      }
    case x => TJ.inj(x)
  }

  def simplifyProjections:
      ProjectBucket[T, ?] ~> QScriptCore[T, ?] =
    new (ProjectBucket[T, ?] ~> QScriptCore[T, ?]) {
      def apply[A](proj: ProjectBucket[T, A]) = proj match {
        case BucketField(src, value, field) =>
          Map(src, Free.roll(MapFuncs.ProjectField(value, field)))
        case BucketIndex(src, value, index) =>
          Map(src, Free.roll(MapFuncs.ProjectIndex(value, index)))
      }
    }

  def coalesceQC[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F, FI: F :<: QScriptProject[T, ?]):
      QScriptCore[T, T[F]] => Option[QScriptCore[T, T[F]]] = {
    case Map(Embed(src), mf) => QC.prj(src) >>= {
      case Map(srcInner, mfInner) => Map(srcInner, mf >> mfInner).some
      case Reduce(srcInner, bucket, funcs, repair) => Reduce(srcInner, bucket, funcs, mf >> repair).some
      case _ => None
    }
    // TODO: For Take and Drop, we should be able to pull _most_ of a Reduce repair function to after T/D
    case Take(src, from, count) => // Pull more work to _after_ limiting the dataset
      from.resume.swap.toOption >>= FI.prj >>= QC.prj >>= {
        case Map(fromInner, mf) => Map(QC.inj(Take(src, fromInner, count)).embed, mf).some
        case _ => None
      }
    case Drop(src, from, count) => // Pull more work to _after_ limiting the dataset
      from.resume.swap.toOption >>= FI.prj >>= QC.prj >>= {
        case Map(fromInner, mf) => Map(QC.inj(Drop(src, fromInner, count)).embed, mf).some
        case _ => None
      }
    case _ => None
  }

  // TODO merge with `coalesceQC`
  def coalesceQCCo[F[_]: Functor, A](implicit QC: QScriptCore[T, ?] :<: F, FI: F :<: QScriptProject[T, ?]):
      QScriptCore[T, T[CoEnv[A, F, ?]]] => Option[QScriptCore[T, T[CoEnv[A, F, ?]]]] = {
    case Map(Embed(src), mf) =>
      src.run.toOption >>= QC.prj >>= {
        case Map(srcInner, mfInner) => Map(srcInner, mf >> mfInner).some
        case Reduce(srcInner, bucket, funcs, repair) => Reduce(srcInner, bucket, funcs, mf >> repair).some
        case _ => None
      }
    case Take(src, from, count) => // Pull more work to _after_ limiting the dataset
      from.resume.swap.toOption >>= FI.prj >>= QC.prj >>= {
        case Map(fromInner, mf) => Map(CoEnv(QC.inj(Take(src, fromInner, count)).right[A]).embed, mf).some
        case _ => None
      }
    case Drop(src, from, count) => // Pull more work to _after_ limiting the dataset
      from.resume.swap.toOption >>= FI.prj >>= QC.prj >>= {
        case Map(fromInner, mf) => Map(CoEnv(QC.inj(Drop(src, fromInner, count)).right[A]).embed, mf).some
        case _ => None
      }
    case _ => None
  }

  def coalesceMapShift[F[_]: Functor](implicit SP: SourcedPathable[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[F]] => F[T[F]] = {
    case x @ Map(Embed(src), mf) => (SP.prj(src) >>= {
      case LeftShift(srcInner, struct, repair) =>
        SP.inj(LeftShift(srcInner, struct, mf >> repair)).some
      case _ => None
    }).getOrElse(QC.inj(x))
    case x => QC.inj(x)
  }

  def simplifySP[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F):
      SourcedPathable[T, T[F]] => F[T[F]] = {
    case x @ LeftShift(src, struct, repair) =>
      if (!repair.element(RightSide))
        QC.inj(Map(src, repair ∘ κ(SrcHole)))
      else if (!repair.element(LeftSide))
        (QC.prj(src.project) >>= {
          case Map(innerSrc, mf) =>
            SP.inj(LeftShift(innerSrc, struct >> mf, repair)).some
          case _ => None
        }).getOrElse(SP.inj(x))
      else
        SP.inj(x)
    case x => SP.inj(x)
  }

  def coalesceMapJoin[F[_]: Functor](
    implicit QC: QScriptCore[T, ?] :<: F, TJ: ThetaJoin[T, ?] :<: F):
      QScriptCore[T, T[F]] => F[T[F]] = {
    case x @ Map(Embed(src), mf) =>
      TJ.prj(src).fold(
        QC.inj(x))(
        tj => TJ.inj(ThetaJoin.combine.modify(mf >> (_: JoinFunc[T]))(tj)))
    case x => QC.inj(x)
  }

  def compactReduction[F[_]: Functor]:
      QScriptCore[T, T[F]] => QScriptCore[T, T[F]] = {
    case x @ Reduce(src, bucket, reducers0, repair0) => {
      // `reducers`: the reduce funcs that are used
      // `indices`: the indices into `reducers0` that are used
      val (reducers, indices): (List[ReduceFunc[FreeMap[T]]], List[Int]) = {
        val used: Set[Int] = repair0.foldLeft(Set[Int]()) {
          case (acc, redIdx) => acc + redIdx.idx
        }
        reducers0.zipWithIndex.filter {
          case (_, idx) => used.contains(idx)
        }.unzip
      }

      // reset the indices in `repair0`
      val repair: Free[MapFunc[T, ?], Int] = repair0.map {
        case ReduceIndex(idx) => indices.indexOf(idx)
      }

      if (repair.element(-1))
        x
      else
        Reduce(src, bucket, reducers, repair.map(ReduceIndex(_)))
    }
    case x => x
  }

  // The order of optimizations is roughly this:
  // - elide NOPs
  // - read conversion given to us by the filesystem
  // - convert any remaning projects to maps
  // - coalesce nodes
  // - normalize mapfunc
  // TODO: Apply this to FreeQS structures.
  def applyAll[F[_]: Traverse: Normalizable](
    implicit QC: QScriptCore[T, ?] :<: F,
             SP: SourcedPathable[T, ?] :<: F,
             TJ: ThetaJoin[T, ?] :<: F,
             PB: ProjectBucket[T, ?] :<: F,
             FI: F :<: QScriptProject[T, ?]):
      F[T[F]] => F[T[F]] =
    (quasar.fp.free.injectedNT[F](simplifyProjections).apply(_: F[T[F]])) ⋙
      Normalizable[F].normalize ⋙
      quasar.fp.free.injectedNT[F](elideNopJoin[F]) ⋙
      liftFG(elideConstantJoin[F]) ⋙
      liftFF(repeatedly(coalesceQC[F])) ⋙
      liftFG(coalesceMapShift[F]) ⋙
      liftFG(coalesceMapJoin[F]) ⋙
      liftFG(simplifySP[F]) ⋙
      Normalizable[F].normalize ⋙
      liftFF(compactReduction[F]) ⋙
      liftFG(elideNopMap[F])

  // Only used when processing user-provided ThetaJoins
  def applyMost[F[_]: Functor: Normalizable](
    implicit QC: QScriptCore[T, ?] :<: F,
             SP: SourcedPathable[T, ?] :<: F,
             TJ: ThetaJoin[T, ?] :<: F,
             PB: ProjectBucket[T, ?] :<: F,
             FI: F :<: QScriptProject[T, ?]):
      F[T[F]] => F[T[F]] =
    (quasar.fp.free.injectedNT[F](simplifyProjections).apply(_: F[T[F]])) ⋙
      Normalizable[F].normalize ⋙
      // quasar.fp.free.injectedNT[F](elideNopJoin[F]) ⋙
      liftFF(repeatedly(coalesceQC[F])) ⋙
      liftFG(coalesceMapShift[F]) ⋙
      liftFG(coalesceMapJoin[F]) ⋙
      liftFG(simplifySP[F]) ⋙
      Normalizable[F].normalize ⋙
      liftFF(compactReduction[F]) ⋙
      liftFG(elideNopMap[F])

  def applyToFreeQS[F[_]: Traverse: Normalizable, A](
    implicit QC: QScriptCore[T, ?] :<: F,
             TJ: ThetaJoin[T, ?] :<: F,
             PB: ProjectBucket[T, ?] :<: F,
             FI: F :<: QScriptProject[T, ?]):
      F[T[CoEnv[A, F, ?]]] => CoEnv[A, F, T[CoEnv[A, F, ?]]] =
    (quasar.fp.free.injectedNT[F](simplifyProjections).apply(_: F[T[CoEnv[A, F, ?]]])) ⋙
      Normalizable[F].normalize ⋙
      quasar.fp.free.injectedNT[F](elideNopJoin[F]) ⋙
      //liftFG(elideConstantJoin[CoEnv[A, F, ?]]) ⋙
      liftFF(repeatedly(coalesceQCCo[F, A])) ⋙
      Normalizable[F].normalize ⋙
      liftFF(compactReduction[CoEnv[A, F, ?]]) ⋙
      (fa => QC.prj(fa).fold(CoEnv(fa.right[A]))(elideNopMapCo[F, A]))
}
