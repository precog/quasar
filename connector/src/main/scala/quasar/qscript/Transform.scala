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
// - the common source in a Join or Union will be the longest common branch
// - any unreferenced src will be `Unreferenced`, and no `Unreferenced` will
//   ever be referenced
// - ReduceIndices will not exceed the reducer bounds, and every reducer will be
//   referenced at least once.

// TODO: Could maybe require only Functor[F], once CoEnv exposes the proper
//       instances
class Transform[T[_[_]]: Recursive: Corecursive: FunctorT: EqualT: ShowT, F[_]: Traverse: Normalizable](
  implicit DE: Const[DeadEnd, ?] :<: F,
           QC: QScriptCore[T, ?] :<: F,
           TJ: ThetaJoin[T, ?] :<: F,
           PB: ProjectBucket[T, ?] :<: F,
           // TODO: Remove this one once we have multi-sorted AST
           FI: F :<: QScriptTotal[T, ?],
           mergeable:  Mergeable.Aux[T, F],
           eq:         Delay[Equal, F],
           show:       Delay[Show, F]) {

  val prov = new Provenance[T]

  type Target[A] = EnvT[Ann[T], F, A]
  type TargetT = Target[T[Target]]
  type FreeEnv = Free[Target, Hole]

  def DeadEndTarget(deadEnd: DeadEnd): TargetT =
    EnvT[Ann[T], F, T[Target]]((EmptyAnn[T], DE.inj(Const[DeadEnd, T[Target]](deadEnd))))

  val RootTarget: TargetT = DeadEndTarget(Root)

  type Envs = List[Target[ExternallyManaged]]

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

  def linearize[F[_]: Functor: Foldable]:
      Algebra[F, List[F[ExternallyManaged]]] =
    fl => fl.as[ExternallyManaged](Extern) :: fl.fold

  def linearizeEnv[E, F[_]: Functor: Foldable]:
      Algebra[EnvT[E, F, ?], List[EnvT[E, F, ExternallyManaged]]] =
    fl => fl.as[ExternallyManaged](Extern) :: fl.lower.fold


  def delinearizeInner[A]: Coalgebra[Target, List[Target[A]]] = {
    case Nil => EnvT((EmptyAnn, DE.inj(Const[DeadEnd, List[Target[A]]](Root))))
    case h :: t => h.map(_ => t)
  }

  def delinearizeTargets[F[_]: Functor, A]:
      ElgotCoalgebra[Hole \/ ?, Target, List[Target[A]]] = {
    case Nil    => SrcHole.left[Target[List[Target[A]]]]
    case h :: t => h.map(κ(t)).right
  }

  val consZipped: Algebra[ListF[Target[ExternallyManaged], ?], ZipperAcc] = {
    case NilF() => ZipperAcc(Nil, ZipperSides(HoleF[T], HoleF[T]), ZipperTails(Nil, Nil))
    case ConsF(head, ZipperAcc(acc, sides, tails)) => ZipperAcc(head :: acc, sides, tails)
  }

  val zipper: ElgotCoalgebra[
      ZipperAcc \/ ?,
      ListF[Target[ExternallyManaged], ?],
      (ZipperSides, ZipperTails)] = {
    case (zs @ ZipperSides(lm, rm), zt @ ZipperTails(l :: ls, r :: rs)) =>
      mergeable.mergeSrcs(lm, rm, l, r).fold[ZipperAcc \/ ListF[Target[ExternallyManaged], (ZipperSides, ZipperTails)]](
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
        .get(lTail.reverse.ana[T, CoEnv[Hole, Target, ?]](delinearizeTargets[F, ExternallyManaged] >>> (CoEnv(_))))

    val rightF =
      foldIso(CoEnv.freeIso[Hole, Target])
        .get(rTail.reverse.ana[T, CoEnv[Hole, Target, ?]](delinearizeTargets[F, ExternallyManaged] >>> (CoEnv(_))))

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
      concatBuckets(src.project.ask.provenance) match {
        case Some((buck, newBucks)) =>
          val (mf, baccess, laccess, raccess) = concat3(buck, lMap, rMap)
          (QC.inj(Map(src, mf)), newBucks.list.toList.map(_ >> baccess), laccess, raccess)
        case None =>
          (QC.inj(Map(src, HoleF[T])), Nil, lMap, rMap)
      }
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

      val left = concatBuckets(lann.provenance.drop(lann.provenance.length - commonProvLength))
      val right = concatBuckets(rann.provenance.drop(rann.provenance.length - commonProvLength))

      val condition = (left, right) match {
        case (None, None) =>
          BoolLit[T, JoinSide](true) // when both sides are empty, perform a full cross
        case (Some(l), Some(r)) =>
          Free.roll[MapFunc[T, ?], JoinSide](Eq(
            l._1.map(κ(LeftSide)),
            r._1.map(κ(RightSide))))
        case (_, _) =>
          BoolLit[T, JoinSide](false) // this case should never be hit
      }

      val tj = TJ.inj(ThetaJoin(
        src,
        lBranch.mapSuspension(FI.compose(envtLowerNT)),
        rBranch.mapSuspension(FI.compose(envtLowerNT)),
        // FIXME: not quite right – e.g., if there is a reduction in a branch the
        //        condition won’t line up.
        condition,
        Inner,
        combine))

      (tj,
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

    // the holes in `bval` reference `fullSrc`
    // so we replace the holes in `lval` with `bval` because the holes in `lval >> bval` must reference `fullSrc`
    // and `bval` applied to `fullSrc` gives us access to `lsrc`, so we apply `lval` after `bval`
    (fullSrc, fullBuckets, lval >> bval, cval >> bval, rval)
  }

  def merge2Map(
    values: Func.Input[T[Target], nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      TargetT = {
    val (src, buckets, lval, rval) = autojoin(values(0), values(1))
    concatBuckets(buckets) match {
      case Some((bucks, newBucks)) => {
        val (merged, b, v) = concat(bucks, Free.roll(func(lval, rval)))
        EnvT((
          Ann[T](newBucks.list.toList.map(_ >> b), v),
          QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, merged))))
      }
      case None =>
        EnvT((
          EmptyAnn[T],
          QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, Free.roll(func(lval, rval))))))
    }
  }

  // TODO unify with `merge2Map`
  def merge3Map(
    values: Func.Input[T[Target], nat._3])(
    func: (FreeMap[T], FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      Target[T[Target]] = {
    val (src, buckets, lval, cval, rval) = autojoin3(values(0), values(1), values(2))
    concatBuckets(buckets) match {
      case Some((bucks, newBucks)) => {
        val (merged, b, v) = concat(bucks, Free.roll(func(lval, cval, rval)))
        EnvT((
          Ann[T](newBucks.list.toList.map(_ >> b), v),
          QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, merged))))
      }
      case None =>
        EnvT((
          EmptyAnn[T],
          QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, Free.roll(func(lval, cval, rval))))))
    }
  }

  def shiftValues(input: T[Target], f: FreeMap[T] => MapFunc[T, FreeMap[T]]):
      Target[T[Target]] = {
    val Ann(provs, value) = input.project.ask
    val (sides, leftAccess, rightAccess) =
      concat(
        Free.point[MapFunc[T, ?], JoinSide](LeftSide),
        Free.point[MapFunc[T, ?], JoinSide](RightSide))

    EnvT((
      Ann(
        prov.shiftMap(Free.roll[MapFunc[T, ?], Hole](ProjectIndex(rightAccess, IntLit(0)))) :: provs.map(_ >> leftAccess),
        Free.roll(ProjectIndex(rightAccess, IntLit(1)))),
      QC.inj(LeftShift(input, Free.roll(f(value)), sides))))
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
      QC.inj(LeftShift(input, Free.roll(f(value)), sides))))
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
        flatten(shiftValues(values(0), ZipMapKeys(_)))
      case structural.FlattenArray =>
        flatten(shiftValues(values(0), ZipArrayIndices(_)))

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
      case structural.ShiftMap   => shiftValues(values(0), ZipMapKeys(_))
      case structural.ShiftArray => shiftValues(values(0), ZipArrayIndices(_))

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
          QC.inj(LeftShift(
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
      concatBuckets(tail) match {
        case Some((newProvs, provAccess)) =>
          EnvT[Ann[T], F, T[Target]]((
            Ann[T](
              provAccess.list.toList.map(_ >> Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
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
        case None =>
          EnvT[Ann[T], F, T[Target]]((
            EmptyAnn[T],
            QC.inj(Reduce[T, T[Target]](
              values(0),
              NullLit(),
              List(ReduceFunc.translateUnaryReduction[FreeMap[T]](func)(reduce)),
              Free.point(ReduceIndex(0))))))
      }
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
      concatBuckets(tail) match {
        case Some((newProvs, provAccess)) =>
          EnvT[Ann[T], F, T[Target]]((
            Ann[T](
              provAccess.list.toList.map(_ >> Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
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
        case None =>
          EnvT[Ann[T], F, T[Target]]((
            EmptyAnn[T],
            QC.inj(Reduce[T, T[Target]](
              values(0),
              NullLit(),
              List(ReduceFunc.translateBinaryReduction[FreeMap[T]](func)(lMap, rMap)),
              Free.point(ReduceIndex(0))))))
      }
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
      TJ.prj(values(2).transCata[F](v => (new Optimize).applyAll[F].apply(v.lower)).project).fold(
        (InternalError(s"non theta join condition found: ${values(2).shows}"): PlannerError).left[JoinFunc[T]])(
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

  def fromData[T[_[_]]: Corecursive](data: Data): Data \/ T[EJson] = {
    data.hyloM[Data \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[Data \/ ?, EJson, Data, T[EJson]](
        _.left,
        _.embed.right),
      Data.toEJson[EJson].apply(_).right)
  }

  // TODO: Replace disjunction with validation.
  def lpToQScript: LogicalPlan[T[Target]] => PlannerError \/ TargetT = {
    case LogicalPlan.ReadF(path) =>
      // TODO: Compilation of SQL² should insert a ShiftMap at each FROM,
      //       however doing that would break the old Mongo backend, and we can
      //       handle it here for now. But it should be moved to the SQL²
      //       compiler when the old Mongo backend is replaced. (#1298)
      shiftValues(pathToProj(path).embed, ZipMapKeys(_)).right

    case LogicalPlan.ConstantF(data) =>
      fromData(data).fold(
        {
          case Data.NA => Undefined[T, FreeMap[T]]().right
          case d => NonRepresentableData(d).left
        },
        Constant[T, FreeMap[T]](_).right) ∘ (mf =>
        EnvT((
          EmptyAnn[T],
          QC.inj(Map(
            RootTarget.embed,
            Free.roll[MapFunc[T, ?], Hole](mf))))))

    case LogicalPlan.FreeF(name) =>
      (Planner.UnboundVariable(name): PlannerError).left[TargetT]

    case LogicalPlan.LetF(name, form, body) =>
      (Planner.InternalError("un-elided Let"): PlannerError).left[TargetT]

    case LogicalPlan.TypecheckF(expr, typ, cont, fallback) =>
      merge3Map(Func.Input3(expr, cont, fallback))(Guard(_, typ, _, _)).right[PlannerError]

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Mapping =>
      val Ann(buckets, value) = a1.project.ask
      concatBuckets(buckets) match {
        case Some((buck, newBuckets)) =>
          val (mf, bucketAccess, valAccess) =
            concat[T, Hole](buck, Free.roll(MapFunc.translateUnaryMapping(func)(HoleF[T])))
          EnvT((
            Ann(newBuckets.list.toList.map(_ >> bucketAccess), valAccess),
            QC.inj(Map(a1, mf)))).right
        case None =>
          EnvT((
            EmptyAnn[T],
            QC.inj(Map(a1, Free.roll(MapFunc.translateUnaryMapping(func)(HoleF[T])))))).right
      }

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
          Free.roll(right).mapSuspension(FI))))).right

    case LogicalPlan.InvokeFUnapply(set.Drop, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      val left: F[Free[F, Hole]] = QC.inj(Map[T, Free[F, Hole]](Free.point[F, Hole](SrcHole), lval))
      val right: F[Free[F, Hole]] = QC.inj(Map[T, Free[F, Hole]](Free.point[F, Hole](SrcHole), rval))
      EnvT((
        Ann[T](buckets, HoleF[T]),
        QC.inj(Drop(
          EnvT((EmptyAnn[T], src)).embed,
          Free.roll(left).mapSuspension(FI),
          Free.roll(right).mapSuspension(FI))))).right

    case LogicalPlan.InvokeFUnapply(set.OrderBy, Sized(a1, a2, a3)) =>
      val (src, bucketsSrc, ordering, buckets, directions) = autojoin3(a1, a2, a3)

      val bucketsList: List[FreeMap[T]] = buckets.toCoEnv[T].project match {
        case StaticArray(as) => as.map(_.fromCoEnv)
        case mf => List(mf.embed.fromCoEnv)
      }

      val directionsList: PlannerError \/ List[SortDir] = {
        val orderStrs: PlannerError \/ List[String] =
          directions.toCoEnv[T].project match {
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
      concatBuckets(prov.squashProvenances(buckets)) match {
        case Some((buck, newBuckets)) =>
          val (mf, buckAccess, valAccess) = concat(buck, value)
          EnvT((
            Ann(newBuckets.list.toList.map(_ >> buckAccess), valAccess),
            QC.inj(Map(a1, mf)))).right
        case None =>
          EnvT((
            EmptyAnn[T],
            QC.inj(Map(a1, value)))).right
      }

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
        (QC.inj(Union(src,
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
          EquiJF,
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
          Free.roll(Constant(EJson.fromCommon[T].apply(ejson.Bool[T[EJson]](false)))),
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
