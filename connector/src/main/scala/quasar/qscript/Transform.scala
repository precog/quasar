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

import slamdata.Predef.{ Eq => _, _ }
import quasar._, Planner._
import quasar.common.JoinType
import quasar.contrib.matryoshka._
import quasar.ejson._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.frontend.{logicalplan => lp}
import quasar.qscript.MapFuncCore._
import quasar.qscript.MapFuncsCore._
import quasar.sql.JoinDir
import quasar.std.StdLib._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
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
    eq: Delay[Equal, F],
    show: Delay[Show, F]) extends TTypes[T] {

  private val IC = Inject[MapFuncCore[?], MapFunc[?]]
  private val ID = Inject[MapFuncDerived[?], MapFunc[?]]
  private val prov = new provenance.ProvenanceT[T]
  private val rewrite = new Rewrite[T]
  private val merge = new Merge[T]

  private case class AutoJoinBase(src: T[F], buckets: List[prov.Provenance]) {
    def asTarget(vals: FreeMap): Target[F] = Target(Ann(buckets, vals), src)
  }

  private case class AutoJoinResult(base: AutoJoinBase, lval: FreeMap, rval: FreeMap)
  private case class AutoJoin3Result(base: AutoJoinBase, lval: FreeMap, cval: FreeMap, rval: FreeMap)
  private case class AutoJoinNResult(base: AutoJoinBase, vals: NonEmptyList[FreeMap])

  /** This unifies a pair of sources into a single one, with additional
    * expressions to access the combined bucketing info, as well as the left and
    * right values.
    */
  private def autojoin(left: Target[F], right: Target[F]): AutoJoinResult = {
    val lann = left.ann
    val rann = right.ann
    val lval: JoinFunc = lann.values.as[JoinSide](LeftSide)
    val rval: JoinFunc = rann.values.as[JoinSide](RightSide)

    (left.value, right.value) match {
      case (Embed(QC(Unreferenced())), r) =>
        val buckets = prov.joinProvenances(lann.provenance, rann.provenance)
        AutoJoinResult(AutoJoinBase(r, buckets), lann.values, rann.values)
      case (l, Embed(QC(Unreferenced()))) =>
        val buckets = prov.joinProvenances(lann.provenance, rann.provenance)
        AutoJoinResult(AutoJoinBase(l, buckets), lann.values, rann.values)
      case (l, r) =>
        val SrcMerge(src, lBranch, rBranch) = merge.mergeT(l, r)

        val lprovs = prov.genBuckets(lann.provenance) ∘ (_ ∘ (_.as[JoinSide](LeftSide)))
        val rprovs = prov.genBuckets(rann.provenance) ∘ (_ ∘ (_.as[JoinSide](RightSide)))

        val (combine, newLprov, newRprov, lacc, racc) =
          (lprovs, rprovs) match {
            case (None, None) =>
              val (combine, lacc, racc) = concat(lval, rval)
              (combine, lann.provenance, rann.provenance, lacc, racc)
            case (None, Some((rProvs, rBuck))) =>
              val (combine, bacc, lacc, racc) = naiveConcat3(rBuck, lval, rval)
              (combine, lann.provenance, prov.rebase(bacc, rProvs), lacc, racc)
            case (Some((lProvs, lBuck)), None) =>
              val (combine, bacc, lacc, racc) = naiveConcat3(lBuck, lval, rval)
              (combine, prov.rebase(bacc, lProvs), rann.provenance, lacc, racc)
            case (Some((lProvs, lBuck)), Some((rProvs, rBuck))) =>
              val (combine, lbacc, rbacc, lacc, racc) =
                naiveConcat4(lBuck, rBuck, lval, rval)
              (combine, prov.rebase(lbacc, lProvs), prov.rebase(rbacc, rProvs), lacc, racc)
          }

        AutoJoinResult(
          AutoJoinBase(
            rewrite.unifySimpleBranches[F, T[F]](src, lBranch, rBranch, combine)(rebaseT).getOrElse(
              TJ.inj(ThetaJoin(src, lBranch, rBranch, prov.genComparisons(newLprov, newRprov), JoinType.Inner, combine))).embed,
            prov.joinProvenances(newLprov, newRprov)),
          lacc,
          racc)
    }
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
      provenance.Value(Free.roll[MapFunc, Hole](MFC(ProjectIndex(rightAccess, IntLit(0))))) :: prov.rebase(leftAccess, provs),
      Free.roll(MFC(ProjectIndex(rightAccess, IntLit(1))))),
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
            Free.roll(MFC(Range(join.lval, join.rval))),
            ExcludeId,
            sides)).embed)
    }

  private def invokeReduction1
      (func: UnaryFunc, values: Func.Input[Target[F], nat._1]): Target[F] = {
    val Ann(provs, reduce) = values(0).ann
    // NB: If there’s no provenance, then there’s nothing to reduce. We’re
    //     already holding a single value.
    provs.tailOption.fold(values(0)) { tail =>
      prov.genBuckets(tail) match {
        case Some((newProvs, buckets)) =>
          Target(Ann(
            prov.rebase(Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))), newProvs),
            Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1))))),
            QC.inj(Reduce[T, T[F]](
              values(0).value,
              buckets,
              List(ReduceFunc.translateUnaryReduction[FreeMap](func)(reduce)),
              Free.roll(MFC(ConcatArrays(
                Free.roll(MFC(MakeArray(Free.point(ReduceIndex(none))))),
                Free.roll(MFC(MakeArray(Free.point(ReduceIndex(0.some)))))))))).embed)
        case None =>
          Target(
            Ann(provs, HoleF),
            QC.inj(Reduce[T, T[F]](
              values(0).value,
              NullLit(),
              List(ReduceFunc.translateUnaryReduction[FreeMap](func)(reduce)),
              Free.point(ReduceIndex(0.some)))).embed)
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
            prov.rebase(Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))), newProvs),
            Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1))))),
            QC.inj(Reduce[T, T[F]](
              values(0).value,
              buckets,
              List(
                ReduceFunc.translateBinaryReduction[FreeMap](func)(join.lval, join.rval)),
              Free.roll(MFC(ConcatArrays(
                Free.roll(MFC(MakeArray(Free.point(ReduceIndex(none))))),
                Free.roll(MFC(MakeArray(Free.point(ReduceIndex(0.some)))))))))).embed)
        case None =>
          Target(
            Ann(join.base.buckets, HoleF),
            QC.inj(Reduce[T, T[F]](
              values(0).value,
              NullLit(),
              List(ReduceFunc.translateBinaryReduction[FreeMap](func)(join.lval, join.rval)),
              Free.point(ReduceIndex(0.some)))).embed)
      }
    }
  }

  // NB: #1556 This is a magic structure. Improve LogicalPlan to not imply this structure.
  private def magicJoinStructure(left: FreeMap, right: FreeMap): JoinFunc =
    Free.roll(MFC(ConcatMaps(
      Free.roll(MFC(MakeMap(StrLit[T, JoinSide](JoinDir.Left.name), left.as(LeftSide)))),
      Free.roll(MFC(MakeMap(StrLit[T, JoinSide](JoinDir.Right.name), right.as(RightSide)))))))

  private def ProjectTarget(prefix: Target[F], field: T[EJson]): Target[F] = {
    val Ann(provs, values) = prefix.ann
    Target(Ann(provenance.Proj(field) :: provs, HoleF),
      PB.inj(BucketField(prefix.value, values, Free.roll(MFC(Constant[T, FreeMap](field))))).embed)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
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
          case Data.NA => MFC(Undefined[T, FreeMap]()).right
          case d       => NonRepresentableData(d).left
        },
        { x: T[EJson] => MFC(Constant[T, FreeMap](x)).right } ) ∘ (mf =>
        Target(
          Ann(Nil, Free.roll[MapFunc, Hole](mf)),
          QC.inj(Unreferenced[T, T[F]]()).embed))

    case lp.Free(name) =>
      (Planner.UnboundVariable(name): PlannerError).left[Target[F]]

    case lp.Let(name, form, body) =>
      Planner.InternalError.fromMsg("un-elided Let").left[Target[F]]

    case lp.Typecheck(expr, typ, cont, fallback) =>
      merge3Map(Func.Input3(expr, cont, fallback))((a1, a2, a3) => MFC(Guard(a1, typ, a2, a3))).right[PlannerError]

    case lp.InvokeUnapply(func @ NullaryFunc(_, _, _, _), Sized())
        if func.effect ≟ Mapping =>
      Target(
        Ann(Nil, Free.roll[MapFunc, Hole](MapFunc.translateNullaryMapping(IC)(func))),
        QC.inj(Unreferenced[T, T[F]]()).embed).right

    case lp.InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Mapping =>
      val Ann(buckets, value) = a1.ann
      Target(
        Ann(buckets, Free.roll[MapFunc, Hole](MapFunc.translateUnaryMapping(IC, ID)(func)(value))),
        a1.value).right

    case lp.InvokeUnapply(structural.ObjectProject, Sized(a1, a2)) =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      // FIXME: This is a workaround because ProjectBucket doesn’t currently
      //        propagate provenance. (#1573)
      Target(
        Ann[T](base.buckets, Free.roll(MFC(ProjectField(lval, rval)))),
        base.src).right
      // (Ann[T](buckets, HoleF[T]),
      //   PB.inj(BucketField(src, lval, rval)).embed).right

    case lp.InvokeUnapply(structural.ArrayProject, Sized(a1, a2)) =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      // FIXME: This is a workaround because ProjectBucket doesn’t currently
      //        propagate provenance. (#1573)
      Target(
        Ann[T](base.buckets, Free.roll(MFC(ProjectIndex(lval, rval)))),
        base.src).right
      // (Ann[T](buckets, HoleF[T]),
      //   PB.inj(BucketIndex(src, lval, rval)).embed).right

    case lp.InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Mapping =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      Target(
        Ann[T](base.buckets, Free.roll(MapFunc.translateBinaryMapping(IC)(func)(lval, rval))),
        base.src).right[PlannerError]

    case lp.InvokeUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3))
        if func.effect ≟ Mapping =>
      merge3Map(Func.Input3(a1, a2, a3))(MapFunc.translateTernaryMapping(IC)(func)).right[PlannerError]

    case lp.InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Reduction =>
      invokeReduction1(func, Func.Input1(a1)).right

    case lp.InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Reduction =>
      invokeReduction2(func, Func.Input2(a1, a2)).right

    case lp.InvokeUnapply(set.Distinct, Sized(a1)) =>
      // NB: If there’s no provenance, then there’s nothing to reduce. We’re
      //     already holding a single value.
      prov.swapProvenances(provenance.Value(a1.ann.values) :: a1.ann.provenance).tailOption.fold(a1) { tail =>
        prov.genBuckets(tail) match {
          case Some((newProvs, buckets)) =>
            Target(Ann(
              prov.rebase(Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))), newProvs),
              Free.roll(MFC(ProjectIndex(HoleF[T], IntLit[T, Hole](1))))),
              QC.inj(Reduce[T, T[F]](
                a1.value,
                buckets,
                Nil,
                Free.roll(MFC(ConcatArrays(
                  Free.roll(MFC(MakeArray(Free.point(ReduceIndex(none))))),
                  Free.roll(MFC(MakeArray(Free.roll(MFC(ProjectIndex(Free.point(ReduceIndex(none)), IntLit(0)))))))))))).embed)
          case None => a1
        }
      }.right

    case lp.InvokeUnapply(set.DistinctBy, Sized(a1, a2)) =>
      val AutoJoinResult(base, lval, rval) = autojoin(a1, a2)
      invokeReduction1(
        agg.First,
        Func.Input1(
          Target(
            Ann(
              prov.swapProvenances(provenance.Value(rval) :: base.buckets),
              lval),
            base.src))).right

    case lp.InvokeUnapply(set.Sample, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge.mergeT(a1.value, a2.value)

      Target(a1.ann, QC.inj(Subset(merged.src, merged.lval, Sample, Free.roll(FI.inject(QC.inj(reifyResult(a2.ann, merged.rval)))))).embed).right

    case lp.InvokeUnapply(set.Take, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge.mergeT(a1.value, a2.value)

      Target(a1.ann, QC.inj(Subset(merged.src, merged.lval, Take, Free.roll(FI.inject(QC.inj(reifyResult(a2.ann, merged.rval)))))).embed).right

    case lp.InvokeUnapply(set.Drop, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge.mergeT(a1.value, a2.value)

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
        Ann(buckets, Free.roll[MapFunc, Hole](MFC(TemporalTrunc(part, value)))),
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
      val SrcMerge(src, lfree, rfree) = merge.mergeT(a1.value, a2.value)
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
      val merged: SrcMerge[T[F], FreeQS] = merge.mergeT(a1.value, a2.value)

      Target(Ann(prov.joinProvenances(a1.ann.provenance, a2.ann.provenance), HoleF),
        TJ.inj(ThetaJoin(
          merged.src,
          merged.lval,
          merged.rval,
          Free.roll(MFC(Eq(Free.point(LeftSide), Free.point(RightSide)))),
          JoinType.Inner,
          LeftSideF)).embed).right

    case lp.InvokeUnapply(set.Except, Sized(a1, a2)) =>
      val merged: SrcMerge[T[F], FreeQS] = merge.mergeT(a1.value, a2.value)

      Target(Ann(a1.ann.provenance, HoleF),
        TJ.inj(ThetaJoin(
          merged.src,
          merged.lval,
          merged.rval,
          BoolLit(false),
          JoinType.LeftOuter,
          LeftSideF)).embed).right

    case lp.JoinSideName(name) =>
      Target(
        Ann(Nil, Free.roll[MapFunc, Hole](MFC(JoinSideName[T, FreeMap](name)))),
        QC.inj(Unreferenced[T, T[F]]()).embed).right

    case lp.Join(left, right, joinType, lp.JoinCondition(lName, rName, func)) =>
      val SrcMerge(src, lBranch, rBranch) = merge.mergeT(left.value, right.value)

      val Ann(leftBuckets, leftValue) = left.ann
      val Ann(rightBuckets, rightValue) = right.ann

      val reifiedCondition: F[T[F]] =
        QC.inj(reifyResult(func.ann, func.value)).embed
          .transCata[T[F]](rewrite.normalize).project

      val result: PlannerError \/ JoinFunc =
        QC.prj(reifiedCondition) match {
          case Some(Map(_, mf)) if mf.count ≟ 0 =>
            mf.as[JoinSide](LeftSide)
              .transCata[JoinFunc](replaceJoinSides[T](lName, rName))
              .right[PlannerError]
          case _ =>
            InternalError.fromMsg(s"non-mapping join condition found").left[JoinFunc]
        }

      result.map(cond =>
        Target(Ann(prov.joinProvenances(leftBuckets, rightBuckets), HoleF),
          TJ.inj(ThetaJoin(src,
            lBranch,
            rBranch,
            cond >>= {
              case LeftSide => leftValue >> LeftSideF
              case RightSide => rightValue >> RightSideF
            },
            joinType,
            magicJoinStructure(leftValue, rightValue))).embed))
  }
}
