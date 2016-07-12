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
import quasar.namegen._
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._
import quasar.Planner._
import quasar.Predef._
import quasar.std.StdLib._

import scala.Predef.implicitly

import matryoshka._, Recursive.ops._, TraverseT.ops._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _}, Scalaz._, Inject._, Leibniz._, IndexedStateT._
import shapeless.{Fin, nat, Sized}

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
class Transform[T[_[_]]: Recursive: Corecursive: EqualT: ShowT, F[_]: Traverse](
  implicit DE: Const[DeadEnd, ?] :<: F,
           SP: SourcedPathable[T, ?] :<: F,
           QC: QScriptCore[T, ?] :<: F,
           TJ: ThetaJoin[T, ?] :<: F,
           QB: QScriptBucket[T, ?] :<: F,
           // TODO: Remove this one once we have multi-sorted AST
           FI: F :<: QScriptInternal[T, ?],
           bucketable: Bucketable.Aux[T, F],
           mergeable:  Mergeable.Aux[T, F[Unit]],
           eq:         Delay[Equal, F])
    extends Helpers[T] {

  type Inner = T[F]
  type InnerPure = T[QScriptPure[T, ?]]

  type Fs[A] = List[F[A]]

  case class ZipperSides(
    lSide: FreeMap[T],
    rSide: FreeMap[T])

  case class ZipperTails(
    lTail: Fs[Unit],
    rTail: Fs[Unit])

  case class ZipperAcc(
    acc: Fs[Unit],
    sides: ZipperSides,
    tails: ZipperTails)

  type QSState[A] = StateT[PlannerError \/ ?, NameGen, A]

  def linearize[F[_]: Functor: Foldable]: Algebra[F, List[F[Unit]]] =
    fl => fl.void :: fl.fold

  def delinearizeInner[F[_]: Functor, A](implicit DE: Const[DeadEnd, ?] :<: F):
      Coalgebra[F, List[F[A]]] = {
    case Nil    => DE.inj(Const(Root))
    case h :: t => h.map(_ => t)
  }

  def delinearizeFreeQS[F[_]: Functor, A]:
      ElgotCoalgebra[Unit \/ ?, F, List[F[A]]] = {
    case Nil    => ().left
    case h :: t => h.map(_ => t).right
  }

  val consZipped: Algebra[ListF[F[Unit], ?], ZipperAcc] = {
    case NilF() => ZipperAcc(Nil, ZipperSides(UnitF[T], UnitF[T]), ZipperTails(Nil, Nil))
    case ConsF(head, ZipperAcc(acc, sides, tails)) => ZipperAcc(head :: acc, sides, tails)
  }

  // E, M, F, A => A => M[E[F[A]]] 
  val zipper: ElgotCoalgebraM[
      ZipperAcc \/ ?,
      State[NameGen, ?],
      ListF[F[Unit], ?],
      (ZipperSides, ZipperTails)] = {
    case (zs @ ZipperSides(lm, rm), zt @ ZipperTails(l :: ls, r :: rs)) => {
      val ma = implicitly[Mergeable.Aux[T, F[Unit]]]

      ma.mergeSrcs(lm, rm, l, r).fold[ZipperAcc \/ ListF[F[Unit], (ZipperSides, ZipperTails)]]({
          case SrcMerge(inn, lmf, rmf) =>
            ConsF(inn, (ZipperSides(lmf, rmf), ZipperTails(ls, rs))).right[ZipperAcc]
        }, ZipperAcc(Nil, zs, zt).left)
    }
    case (sides, tails) =>
      ZipperAcc(Nil, sides, tails).left.point[State[NameGen, ?]]
  }

  def merge(left: Inner, right: Inner): State[NameGen, SrcMerge[Inner, Free[F, Unit]]] = {
    val lLin: Fs[Unit] = left.cata(linearize).reverse
    val rLin: Fs[Unit] = right.cata(linearize).reverse

    elgotM((
      ZipperSides(UnitF[T], UnitF[T]),
      ZipperTails(lLin, rLin)))(
      consZipped(_: ListF[F[Unit], ZipperAcc]).point[State[NameGen, ?]], zipper) ∘ {
        case ZipperAcc(common, ZipperSides(lMap, rMap), ZipperTails(lTail, rTail)) =>
          val leftRev: FreeUnit[F] =
            foldIso(CoEnv.freeIso[Unit, F])
              .get(lTail.reverse.ana[T, CoEnv[Unit, F, ?]](delinearizeFreeQS[F, Unit] >>> (CoEnv(_))))

          val rightRev: FreeUnit[F] =
            foldIso(CoEnv.freeIso[Unit, F])
              .get(rTail.reverse.ana[T, CoEnv[Unit, F, ?]](delinearizeFreeQS[F, Unit] >>> (CoEnv(_))))

          SrcMerge[Inner, FreeUnit[F]](
            common.reverse.ana[T, F](delinearizeInner),
            rebase(leftRev, Free.roll(SP.inj(Map(().point[Free[F, ?]], lMap)))),
            rebase(rightRev, Free.roll(SP.inj(Map(().point[Free[F, ?]], rMap)))))
    }
  }

  def mergeTheta[A](
    inl: Inner,
    inr: Inner,
    out: SrcMerge[ThetaJoin[T, Inner], FreeMap[T]] => A): QSState[A] =
    merge(inl, inr).mapK(_.right[PlannerError]) >>= {
      case SrcMerge(merged, left, right) =>
        makeBasicTheta(merged, left, right).map(out)
    }

  def merge2Map(
    values: Func.Input[Inner, nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      QSState[SourcedPathable[T, Inner]] =
    mergeTheta[SourcedPathable[T, Inner]](
      values(0),
      values(1),
      { case SrcMerge(src, mfl, mfr) =>
        Map(TJ.inj(src).embed, Free.roll(func(mfl, mfr)))
      })

  def merge2Expansion(
    values: Func.Input[Inner, nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      QSState[SourcedPathable[T, Inner]] =
    mergeTheta[SourcedPathable[T, Inner]](
      values(0),
      values(1),
      { case SrcMerge(src, mfl, mfr) =>
        LeftShift(
          TJ.inj(src).embed,
          Free.roll(func(mfl, mfr)),
          Free.point[MapFunc[T, ?], JoinSide](RightSide)) // TODO Does this add a bucket for the range values? (values mirrored in buckets)
      })

  case class Merge3(
    src: ThetaJoin[T, Inner],
    first: FreeMap[T],
    second: FreeMap[T],
    fands: FreeMap[T],
    third: FreeMap[T])

  def merge3(a1: Inner, a2: Inner, a3: Inner): QSState[Merge3] = {
    for {
      tup0 <- merge(a1, a2).mapK(_.right[PlannerError])
      SrcMerge(merged0, first0, second0) = tup0
      tup1 <- merge(merged0, a3).mapK(_.right[PlannerError])
      SrcMerge(merged, fands0, third0) = tup1
      th1 <- makeBasicTheta(merged0, first0, second0)
      th2 <- makeBasicTheta(TJ.inj(th1.src).embed, fands0, third0)
    } yield {
      Merge3(th2.src, th1.left, th1.right, th2.left, th2.right)
    }
  }

  def merge3Map(
    values: Func.Input[Inner, nat._3])(
    func: (FreeMap[T], FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]])(
    implicit ma: Mergeable.Aux[T, F[Unit]]):
      QSState[SourcedPathable[T, Inner]] = {
      merge3(values(0), values(1), values(2)) map { merged =>
        Map(TJ.inj(merged.src).embed, Free.roll(func(
          rebase(merged.fands, merged.first),
          rebase(merged.fands, merged.second),
          merged.third)))
      }
  }

  def makeBasicTheta[A](src: A, left: Free[F, Unit], right: Free[F, Unit]):
      QSState[SrcMerge[ThetaJoin[T, A], FreeMap[T]]] =
    for {
      leftName <- freshName("leftTheta").lift[PlannerError \/ ?]
      rightName <- freshName("rightTheta").lift[PlannerError \/ ?]
    } yield {
      SrcMerge[ThetaJoin[T, A], FreeMap[T]](
        ThetaJoin(src, left.mapSuspension(FI), right.mapSuspension(FI), equiJF, Inner,
          Free.roll(ConcatMaps(
            Free.roll(MakeMap(
              StrLit(leftName),
              Free.point[MapFunc[T, ?], JoinSide](LeftSide))),
            Free.roll(MakeMap(
              StrLit(rightName),
              Free.point[MapFunc[T, ?], JoinSide](RightSide)))))),
        Free.roll(ProjectField(UnitF[T], StrLit(leftName))),
        Free.roll(ProjectField(UnitF[T], StrLit(rightName))))
    }

  // NB: More compilicated LeftShifts are generated as an optimization:
  // before: ThetaJoin(cs, Map((), mf), LeftShift((), struct, repair), comb)
  // after: LeftShift(cs, struct, comb.flatMap(LeftSide => mf.map(_ => LeftSide), RS => repair))
  def invokeExpansion1(func: UnaryFunc, values: Func.Input[Inner, nat._1]):
      F[Inner] =
    func match {
      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - 12
      //   id(p, x:bar) - 18
      //   id(p, x:foo) - 1
      //   id(p, x:bar) - 2
      // (one bucket)
      case structural.FlattenMap =>
        SP.inj(LeftShift(
          values(0),
          UnitF,
          Free.point(RightSide)))

      case structural.FlattenArray =>
        SP.inj(LeftShift(
          values(0),
          UnitF,
          Free.point(RightSide)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - foo
      //   id(p, x:bar) - bar
      //   id(p, y:foo) - foo
      //   id(p, y:bar) - bar
      // (one bucket)
      case structural.FlattenMapKeys =>
        SP.inj(LeftShift(
          values(0),
          Free.roll(DupMapKeys(UnitF)),
          Free.point(RightSide)))
      case structural.FlattenArrayIndices =>
        SP.inj(LeftShift(
          values(0),
          Free.roll(DupArrayIndices(UnitF)),
          Free.point(RightSide)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - 12
      //   id(p, x, bar) - 18
      //   id(p, y, foo) - 1
      //   id(p, y, bar) - 2
      // (two buckets)
      case structural.ShiftMap =>
        QB.inj(LeftShiftBucket(
          values(0),
          UnitF,
          Free.point(RightSide),
          Free.roll(DupMapKeys(UnitF)))) // affects bucketing metadata
      case structural.ShiftArray =>
        QB.inj(LeftShiftBucket(
          values(0),
          UnitF,
          Free.point(RightSide),
          Free.roll(DupArrayIndices(UnitF)))) // affects bucketing metadata

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - foo
      //   id(p, x, bar) - bar
      //   id(p, y, foo) - foo
      //   id(p, y, bar) - bar
      // (two buckets)
      case structural.ShiftMapKeys =>
        QB.inj(LeftShiftBucket(
          values(0),
          Free.roll(DupMapKeys(UnitF)),
          Free.point(RightSide),
          UnitF)) // affects bucketing metadata
      case structural.ShiftArrayIndices =>
        QB.inj(LeftShiftBucket(
          values(0),
          Free.roll(DupArrayIndices(UnitF)),
          Free.point(RightSide),
          UnitF)) // affects bucketing metadata
    }

  def invokeExpansion2(
    func: BinaryFunc,
    values: Func.Input[Inner, nat._2]):
      QSState[SourcedPathable[T, Inner]] =
    func match {
      case set.Range => merge2Expansion(values)(Range(_, _))
    }

  def invokeMapping1(func: UnaryFunc, values: Func.Input[Inner, nat._1]):
      SourcedPathable[T, Inner] =
    Map(values(0), Free.roll(MapFunc.translateUnaryMapping(func)(UnitF)))

  def invokeMapping2(
    func: BinaryFunc,
    values: Func.Input[Inner, nat._2]):
      QSState[SourcedPathable[T, Inner]] =
    merge2Map(values)(MapFunc.translateBinaryMapping(func))

  def invokeMapping3(
    func: TernaryFunc,
    values: Func.Input[Inner, nat._3]):
      QSState[SourcedPathable[T, Inner]] =
    merge3Map(values)(MapFunc.translateTernaryMapping(func))

  def findBucket(inner: Inner):
      QSState[(Inner, FreeMap[T], FreeMap[T])] = {
    val bucketDisj: QScriptBucket[T, Inner] \/ (Int, Inner) =
      inner.transAnaM(bucketable.digForBucket[F]).run(1)

    bucketDisj match {
      case -\/(GroupBy(src, _, bucket)) =>
        mergeTheta[(Inner, FreeMap[T], FreeMap[T])](
          src,
          inner,
          { case SrcMerge(src, mfl, mfr) =>
            (TJ.inj(src).embed, rebase(bucket, mfl), mfr)
          })
      case -\/(BucketField(src, _, name)) =>
        mergeTheta[(Inner, FreeMap[T], FreeMap[T])](
          src,
          inner,
          { case SrcMerge(src, mfl, mfr) =>
            (TJ.inj(src).embed, rebase(name, mfl), mfr)
          })
      case -\/(BucketIndex(src, _, index)) =>
        mergeTheta[(Inner, FreeMap[T], FreeMap[T])](
          src,
          inner,
          { case SrcMerge(src, mfl, mfr) =>
            (TJ.inj(src).embed, rebase(index, mfl), mfr)
          })
      case -\/(LeftShiftBucket(src, struct, _, bucket)) =>
        mergeTheta[(Inner, FreeMap[T], FreeMap[T])](
          src,
          inner,
          { case SrcMerge(src, mfl, mfr) =>
            (TJ.inj(src).embed, rebase(rebase(struct, bucket), mfl), mfr)
          })
      case -\/(SquashBucket(src)) =>
        stateT((inner, Free.roll(Nullary(CommonEJson.inj(ejson.Null[T[EJson]]()).embed)), UnitF)) // singleton provenance - one big bucket
      case \/-((qs, 0)) =>
        stateT((inner, Free.roll(Nullary(CommonEJson.inj(ejson.Null[T[EJson]]()).embed)), UnitF)) // singleton provenance - one big bucket
      case \/-((qs, _)) =>
        stateT((inner, UnitF, UnitF)) // everying in an individual bucket - throw away reduce
    }
  }

  def invokeReduction1(
      func: UnaryFunc,
      values: Func.Input[Inner, nat._1]):
    QSState[QScriptCore[T, Inner]] = {

    findBucket(values(0)) map {
      case (src, bucket, reduce) =>
        Reduce[T, Inner, nat._0](
          src,
          bucket,
          Sized[List](ReduceFunc.translateReduction[FreeMap[T]](func)(reduce)),
          Free.point(Fin[nat._0, nat._1]))
    }
  }

  // TODO: These should definitely be in Matryoshka.

  // apomorphism - short circuit by returning left
  def substitute[T[_[_]], F[_]](original: T[F], replacement: T[F])(implicit T: Equal[T[F]]):
      T[F] => T[F] \/ T[F] =
   tf => if (tf ≟ original) replacement.left else original.right

  def transApoT[T[_[_]]: FunctorT, F[_]: Functor](t: T[F])(f: T[F] => T[F] \/ T[F]):
      T[F] =
    f(t).fold(ι, FunctorT[T].map(_)(_.map(transApoT(_)(f))))

  def invokeThetaJoin(input: Func.Input[Inner, nat._3], tpe: JoinType): QSState[ThetaJoin[T, Inner]] =
    for {
      tup0 <- merge(input(0), input(1)).mapK(_.right[PlannerError])
      SrcMerge(src1, jbLeft, jbRight) = tup0
      tup1 <- merge(src1, input(2)).mapK(_.right[PlannerError])
    } yield {
      val SrcMerge(src2, bothSides, cond) = tup1

      val leftBr = rebase(bothSides, jbLeft)
      val rightBr = rebase(bothSides, jbRight)

      val onQS =
        transApoT[Free[?[_], JoinSide], F](transApoT[Free[?[_], JoinSide], F](cond.map[JoinSide](κ(RightSide)))(
          substitute[Free[?[_], JoinSide], F](jbLeft.map[JoinSide](κ(RightSide)), Free.point(LeftSide))))(
          substitute[Free[?[_], JoinSide], F](jbRight.map[JoinSide](κ(RightSide)), Free.point(RightSide)))

      val on: JoinFunc[T] = equiJF // TODO get from onQS to here somehow

      // TODO namegen
      ThetaJoin(
        src2,
        leftBr.mapSuspension(FI),
        rightBr.mapSuspension(FI),
        on,
        Inner,
        Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit("left"), Free.point(LeftSide))),
          Free.roll(MakeMap(StrLit("right"), Free.point(RightSide))))))
    }

  def pathToProj(path: pathy.Path[_, _, _]): FreeMap[T] =
    pathy.Path.peel(path).fold[FreeMap[T]](
      Free.point(())) {
      case (p, n) =>
        Free.roll(ProjectField(pathToProj(p), StrLit(n.fold(_.value, _.value))))
    }

  // TODO error handling
  def fromData[T[_[_]]: Corecursive](data: Data): String \/ T[EJson] = {
    data.hyloM[String \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[String \/ ?, EJson, Data, T[EJson]](
        _.toString.left[T[EJson]],
        _.embed.right[String]),
      Data.toEJson[EJson].apply(_).right)
  }

  def lpToQScript: LogicalPlan[Inner] => QSState[F[Inner]] = {
    case LogicalPlan.ReadF(path) =>
      stateT(SP.inj(Map(
        CorecursiveOps[T, F](DE.inj(Const[DeadEnd, Inner](Root))).embed,
        pathToProj(path))))

    case LogicalPlan.ConstantF(data) =>
      stateT(SP.inj(Map(
        DE.inj(Const[DeadEnd, Inner](Root)).embed,
        Free.roll[MapFunc[T, ?], Unit](Nullary[T, FreeMap[T]](fromData(data).fold(
          error => CommonEJson.inj(ejson.Str[T[EJson]](error)).embed,
          ι))))))

    case LogicalPlan.FreeF(name) =>
      stateT(SP.inj(Map(
        DE.inj(Const[DeadEnd, Inner](Empty)).embed,
        Free.roll(ProjectField(StrLit(name.toString), UnitF[T])))))

    case LogicalPlan.LetF(name, form, body) =>
      for {
        tmpName <- freshName("let").lift[PlannerError \/ ?]
        tup <- merge(form, body).mapK(_.right[PlannerError])
        SrcMerge(src, jb1, jb2) = tup
        theta <- makeBasicTheta(src, jb1, jb2)
      } yield {
        SP.inj(Map(
          SP.inj(Map(
            TJ.inj(theta.src).embed,
            Free.roll(ConcatMaps(
              Free.roll(MakeMap(StrLit(tmpName), UnitF[T])),
              Free.roll(MakeMap(StrLit(name.toString), theta.left)))))).embed,
          rebase(theta.right, Free.roll(ProjectField(UnitF[T], StrLit(tmpName))))))
      }

    case LogicalPlan.TypecheckF(expr, typ, cont, fallback) =>
      merge3Map(Func.Input3(expr, cont, fallback))(Guard(_, typ, _, _))
        .map(SP.inj)

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect ≟ Mapping =>
      stateT(SP.inj(invokeMapping1(func, Func.Input1(a1))))

    case LogicalPlan.InvokeFUnapply(structural.ObjectProject, Sized(a1, a2)) =>
      mergeTheta[F[Inner]](a1, a2, {
        case SrcMerge(src, mfl, mfr) =>
          QB.inj(BucketField(TJ.inj(src).embed, mfl, mfr))
      })

    case LogicalPlan.InvokeFUnapply(structural.ArrayProject, Sized(a1, a2)) =>
      mergeTheta[F[Inner]](a1, a2, {
        case SrcMerge(src, mfl, mfr) =>
          QB.inj(BucketIndex(TJ.inj(src).embed, mfl, mfr))
      })

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Mapping =>
      invokeMapping2(func, Func.Input2(a1, a2)).map(SP.inj)

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))
        if func.effect ≟ Mapping =>
      invokeMapping3(func, Func.Input3(a1, a2, a3)).map(SP.inj)

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Reduction =>
      invokeReduction1(func, Func.Input1(a1)) map(QC.inj)

    case LogicalPlan.InvokeFUnapply(set.Take, Sized(a1, a2)) =>
      merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
        case SrcMerge(src, jb1, jb2) =>
          QC.inj(Take(src, jb1.mapSuspension(FI), jb2.mapSuspension(FI)))
      }

    case LogicalPlan.InvokeFUnapply(set.Drop, Sized(a1, a2)) =>
      merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
        case SrcMerge(src, jb1, jb2) =>
          QC.inj(Drop(src, jb1.mapSuspension(FI), jb2.mapSuspension(FI)))
      }

    case LogicalPlan.InvokeFUnapply(set.OrderBy, Sized(a1, a2, a3)) => {
      // we assign extra variables because of:
      // https://issues.scala-lang.org/browse/SI-5589
      // https://issues.scala-lang.org/browse/SI-7515

      (for {
        bucket0 <- findBucket(a1)
        (bucketSrc, bucket, thing) = bucket0
        merged0 <- merge3(a2, a3, bucketSrc)
      } yield {
        val Merge3(src, keys, order, buckets, arrays) = merged0
        val rebasedArrays = rebase(thing, arrays)

        val keysList: List[FreeMap[T]] = rebase(rebasedArrays, keys) match {
          case ConcatArraysN(as) => as
          case mf => List(mf)
        }

        // TODO handle errors
        val orderList: PlannerError \/ List[SortDir] = {
          val orderStrs: PlannerError \/ List[String] = rebase(rebasedArrays, order) match {
            case ConcatArraysN(as) => as.traverse(StrLit.unapply(_)) \/> InternalError("unsupported ordering type") // disjunctionify
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
          orderList.map { keysList.zip(_) }

        (lists.map { pairs =>
          QC.inj(Sort(
            TJ.inj(src).embed,
            rebase(bucket, buckets),
            pairs))
        }).liftM[StateT[?[_], NameGen, ?]]
      }).join
    }

    case LogicalPlan.InvokeFUnapply(set.Filter, Sized(a1, a2)) =>
      mergeTheta[F[Inner]](a1, a2, {
        case SrcMerge(src, fm1, fm2) =>
          SP.inj(Map(
            QC.inj(Filter(TJ.inj(src).embed, fm2)).embed,
            fm1))
      })

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect ≟ Squashing =>
      stateT(func match {
        case identity.Squash => QB.inj(SquashBucket(a1))
      })

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect ≟ Expansion =>
      stateT(invokeExpansion1(func, Func.Input1(a1)))

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect ≟ Expansion =>
      invokeExpansion2(func, Func.Input2(a1, a2)) map {
        SP.inj(_)
      }

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect ≟ Transformation =>
      func match {
        case set.GroupBy =>
          mergeTheta[F[Inner]](a1, a2, {
            case SrcMerge(merged, source, bucket) =>
              QB.inj(GroupBy(TJ.inj(merged).embed, source, bucket))
          })
        case set.Union =>
          merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
            case SrcMerge(src, jb1, jb2) =>
              SP.inj(Union(src, jb1.mapSuspension(FI), jb2.mapSuspension(FI)))
          }
        case set.Intersect =>
          merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
            case SrcMerge(src, jb1, jb2) =>
              TJ.inj(ThetaJoin(src, jb1.mapSuspension(FI), jb2.mapSuspension(FI), equiJF, Inner, Free.point(LeftSide)))
          }
        case set.Except =>
          merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
            case SrcMerge(src, jb1, jb2) =>
              TJ.inj(ThetaJoin(
                src,
                jb1.mapSuspension(FI),
                jb2.mapSuspension(FI),
                Free.roll(Nullary(CommonEJson.inj(ejson.Bool[T[EJson]](false)).embed)),
                LeftOuter,
                Free.point(LeftSide)))
          }
      }

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))  if func.effect ≟ Transformation=>
      def invoke(tpe: JoinType): QSState[F[Inner]] =
        invokeThetaJoin(Func.Input3(a1, a2, a3), tpe).map(TJ.inj)

      func match {
        case set.InnerJoin      => invoke(Inner)
        case set.LeftOuterJoin  => invoke(LeftOuter)
        case set.RightOuterJoin => invoke(RightOuter)
        case set.FullOuterJoin  => invoke(FullOuter)
      }
  }
}

class Optimize[T[_[_]]: Recursive: Corecursive: EqualT] extends Helpers[T] {

  // TODO: These optimizations should give rise to various property tests:
  //       • elideNopMap ⇒ no `Map(???, UnitF)`
  //       • normalize ⇒ a whole bunch, based on MapFuncs
  //       • elideNopJoin ⇒ no `ThetaJoin(???, UnitF, UnitF, LeftSide === RightSide, ???, ???)`
  //       • coalesceMaps ⇒ no `Map(Map(???, ???), ???)`
  //       • coalesceMapJoin ⇒ no `Map(ThetaJoin(???, …), ???)`

  def elideNopMap[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F):
      SourcedPathable[T, T[F]] => F[T[F]] = {
    case Map(src, mf) if mf ≟ UnitF => src.project
    case x                          => SP.inj(x)
  }

  def elideNopJoin[F[_]](
    implicit Th: ThetaJoin[T, ?] :<: F, SP: SourcedPathable[T, ?] :<: F):
      ThetaJoin[T, T[F]] => F[T[F]] = {
    case ThetaJoin(src, l, r, on, _, combine)
        if l ≟ Free.point(()) && r ≟ Free.point(()) && on ≟ equiJF =>
      SP.inj(Map(src, combine.void))
    case x => Th.inj(x)
  }

  // TODO write extractor for inject
  //SourcedPathable[T, T[CoEnv[A,F, ?]]] => SourcedPathable[T, T[F]] = {
  //F[A] => A  ===> CoEnv[E, F, A] => A
  def coalesceMaps[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F):
      SourcedPathable[T, T[F]] => SourcedPathable[T, T[F]] = {
    case x @ Map(Embed(src), mf) => SP.prj(src) match {
      case Some(Map(srcInner, mfInner)) => Map(srcInner, rebase(mf, mfInner))
      case _ => x
    }
    case x => x
  }

  def coalesceMapJoin[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F, TJ: ThetaJoin[T, ?] :<: F):
      SourcedPathable[T, T[F]] => F[T[F]] = {
    case x @ Map(Embed(src), mf) =>
      TJ.prj(src).fold(
        SP.inj(x))(
        tj => TJ.inj(ThetaJoin.combine.modify(mf >> (_: JoinFunc[T]))(tj)))
    case x => SP.inj(x)
  }

  // TODO: Apply this to FreeQS structures.
  def applyAll[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F, TJ: ThetaJoin[T, ?] :<: F):
      F[T[F]] => F[T[F]] =
    liftFG(elideNopJoin[F]) ⋙
    liftFF(coalesceMaps[F]) ⋙
    liftFG(coalesceMapJoin[F]) ⋙
    Normalizable[F].normalize ⋙
    liftFG(elideNopMap[F])
}
