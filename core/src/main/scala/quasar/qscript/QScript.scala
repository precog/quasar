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

import matryoshka._, FunctorT.ops._, Recursive.ops._
import matryoshka.patterns._
import monocle.macros.Lenses
import pathy.Path._
import scalaz.{:+: => _, Divide => _, _}, Scalaz._, Inject._, Leibniz._, IndexedStateT._
import shapeless.{:: => _, Data => _, Coproduct => _, Const => _, _}

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

sealed abstract class DeadEnd

/** The top level of a filesystem. During compilation this represents `/`, but
  * in the structure a backend sees, it represents the mount point.
  */
final case object Root extends DeadEnd
final case object Empty extends DeadEnd

object DeadEnd {
  implicit def equal: Equal[DeadEnd] = Equal.equalRef
  implicit def show: Show[DeadEnd] = Show.showFromToString

  implicit def mergeable[T[_[_]]]: Mergeable.Aux[T, DeadEnd] =
    new Mergeable[DeadEnd] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: DeadEnd,
        p2: DeadEnd):
          Option[Merge[IT, DeadEnd]] =
        if (p1 ≟ p2)
          Some(AbsMerge[IT, DeadEnd, FreeMap](p1, UnitF, UnitF))
        else
          None
    }
}

/** A backend-resolved `Root`, which is now a path. */
@Lenses final case class Read[A](src: A, path: AbsFile[Sandboxed])

object Read {
  implicit def equal[T[_[_]]]: Delay[Equal, Read] =
    new Delay[Equal, Read] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (Read(a1, p1), Read(a2, p2)) => eq.equal(a1, a2) && p1 ≟ p2
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[Read] =
    new Traverse[Read] {
      def traverseImpl[G[_]: Applicative, A, B](fa: Read[A])(f: A => G[B]) =
        f(fa.src) ∘ (Read(_, fa.path))
    }
}

// backends can choose to rewrite joins using EquiJoin
// can rewrite a ThetaJoin as EquiJoin + Filter
@Lenses final case class EquiJoin[T[_[_]], A](
  src: A,
  lBranch: FreeQS[T],
  rBranch: FreeQS[T],
  lKey: FreeMap[T],
  rKey: FreeMap[T],
  f: JoinType,
  combine: JoinFunc[T])

object EquiJoin {
  implicit def equal[T[_[_]]](implicit eqTEj: Equal[T[EJson]]):
      Delay[Equal, EquiJoin[T, ?]] =
    new Delay[Equal, EquiJoin[T, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (EquiJoin(a1, l1, r1, lk1, rk1, f1, c1),
                EquiJoin(a2, l2, r2, lk2, rk2, f2, c2)) =>
            eq.equal(a1, a2) &&
            l1 ≟ l2 &&
            r1 ≟ r2 &&
            lk1 ≟ lk2 &&
            rk1 ≟ rk2 &&
            f1 ≟ f2 &&
            c1 ≟ c2
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[EquiJoin[T, ?]] =
    new Traverse[EquiJoin[T, ?]] {
      def traverseImpl[G[_]: Applicative, A, B](
        fa: EquiJoin[T, A])(
        f: A => G[B]) =
        f(fa.src) ∘
          (EquiJoin(_, fa.lBranch, fa.rBranch, fa.lKey, fa.rKey, fa.f, fa.combine))
    }
}

trait Helpers[T[_[_]]] {
  def basicJF: JoinFunc[T] =
    Free.roll(Eq(Free.point(LeftSide), Free.point(RightSide)))
}

class Transform[T[_[_]]: Recursive: Corecursive](
    implicit showInner: Show[T[QScriptInternal[T, ?]]],
             eqTEj: Equal[T[EJson]]) extends Helpers[T] {

  type Inner = T[QScriptInternal[T, ?]]
  type InnerPure = T[QScriptPure[T, ?]]

  type Pures[A] = List[QScriptInternal[T, A]]
  type DoublePures = (Pures[Unit], Pures[Unit])
  type TriplePures = (Pures[Unit], Pures[Unit], Pures[Unit])

  type DoubleFreeMap = (FreeMap[T], FreeMap[T])

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

  val consZipped: Algebra[ListF[QScriptInternal[T, Unit], ?], TriplePures] = {
    case NilF() => (Nil, Nil, Nil)
    case ConsF(head, (acc, l, r)) => (head :: acc, l, r)
  }

  val zipper: ElgotCoalgebra[TriplePures \/ ?, ListF[QScriptInternal[T, Unit], ?], (DoubleFreeMap, DoublePures)] = {
    case ((_, _), (Nil, Nil)) => (Nil, Nil, Nil).left
    case ((_, _), (Nil, r))   => (Nil, Nil, r).left
    case ((_, _), (l,   Nil)) => (Nil, l,   Nil).left
    case ((lm, rm), (l :: ls, r :: rs)) => {
      val ma = implicitly[Mergeable.Aux[T, QScriptInternal[T, Unit]]]

      ma.mergeSrcs(lm, rm, l, r).fold[TriplePures \/ ListF[QScriptInternal[T, Unit], (DoubleFreeMap, DoublePures)]](
        (Nil, l :: ls, r :: rs).left) {
        case AbsMerge(inn, lmf, rmf) => ConsF(inn, ((lmf, rmf), (ls, rs))).right[TriplePures]
      }
    }
  }

  def merge(left: Inner, right: Inner): MergeJoin[T, Inner] = {
    val lLin: Pures[Unit] = left.cata(linearize).reverse
    val rLin: Pures[Unit] = right.cata(linearize).reverse

    val (common, lTail, rTail) =
      ((UnitF[T], UnitF[T]), (lLin, rLin)).elgot(consZipped, zipper)

    AbsMerge[T, Inner, FreeQS](
      common.reverse.ana[T, QScriptInternal[T, ?]](delinearizeInner),
      foldIso(CoEnv.freeIso[Unit, QScriptInternal[T, ?]])
        .get(lTail.reverse.ana[T, CoEnv[Unit, QScriptInternal[T, ?], ?]](delinearizeFreeQS[QScriptInternal[T, ?], Unit] >>> (CoEnv(_)))),
      foldIso(CoEnv.freeIso[Unit, QScriptInternal[T, ?]])
        .get(rTail.reverse.ana[T, CoEnv[Unit, QScriptInternal[T, ?], ?]](delinearizeFreeQS[QScriptInternal[T, ?], Unit] >>> (CoEnv(_)))))
  }

  def merge2Map(
    values: Func.Input[Inner, nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      QSState[SourcedPathable[T, Inner]] = {
    val AbsMerge(merged, left, right) = merge(values(0), values(1))
    makeBasicTheta(merged, left, right) map {
      case AbsMerge(src, mfl, mfr) =>
        Map(ThetaJoinInternal[T].inj(src).embed, Free.roll(func(mfl, mfr)))
    }
  }

  def merge2Expansion(
    values: Func.Input[Inner, nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      QSState[SourcedPathable[T, Inner]] = {
    val AbsMerge(merged, left, right) = merge(values(0), values(1))
    makeBasicTheta(merged, left, right) map {
      case AbsMerge(src, mfl, mfr) =>
        LeftShift(
          ThetaJoinInternal[T].inj(src).embed,
          Free.roll(func(mfl, mfr)),
          Free.point(RightSide)) // TODO Does this add a bucket for the range values? (values mirrored in buckets)
    }
  }

  case class Merge3(
    src: ThetaJoin[T, Inner],
    first: FreeMap[T],
    second: FreeMap[T],
    fands: FreeMap[T],
    third: FreeMap[T])

  def merge3(a1: Inner, a2: Inner, a3: Inner): QSState[Merge3] = {
    val AbsMerge(merged0, first0, second0) = merge(a1, a2)
    val AbsMerge(merged, fands0, third0) = merge(merged0, a3)

    for {
      th1 <- makeBasicTheta(merged0, first0, second0)
      th2 <- makeBasicTheta(ThetaJoinInternal[T].inj(th1.src).embed, fands0, third0)
    } yield {
      Merge3(th2.src, th1.left, th1.right, th2.left, th2.right)
    }
  }

  def merge3Map(
    values: Func.Input[Inner, nat._3])(
    func: (FreeMap[T], FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]])(
    implicit ma: Mergeable.Aux[T, QScriptInternal[T, Unit]]):
      QSState[SourcedPathable[T, Inner]] = {
      merge3(values(0), values(1), values(2)) map { merged =>
        Map(ThetaJoinInternal[T].inj(merged.src).embed, Free.roll(func(
          rebase(merged.fands, merged.first),
          rebase(merged.fands, merged.second),
          merged.third)))
      }
  }

  def makeBasicTheta[A](src: A, left: FreeQS[T], right: FreeQS[T]):
      QSState[Merge[T, ThetaJoin[T, A]]] =
    for {
      leftName <- freshName("left").lift[PlannerError \/ ?]
      rightName <- freshName("right").lift[PlannerError \/ ?]
    } yield {
      AbsMerge[T, ThetaJoin[T, A], FreeMap](
        ThetaJoin(src, left, right, basicJF, Inner,
          Free.roll(ConcatObjects(
            Free.roll(MakeObject(
              StrLit(leftName),
              Free.point[MapFunc[T, ?], JoinSide](LeftSide))),
            Free.roll(MakeObject(
              StrLit(rightName),
              Free.point[MapFunc[T, ?], JoinSide](RightSide)))))),
        Free.roll(ProjectField(UnitF[T], StrLit(leftName))),
        Free.roll(ProjectField(UnitF[T], StrLit(rightName))))
    }

  // NB: More compilicated LeftShifts are generated as an optimization:
  // before: ThetaJoin(cs, Map((), mf), LeftShift((), struct, repair), comb)
  // after: LeftShift(cs, struct, comb.flatMap(LeftSide => mf.map(_ => LeftSide), RS => repair))
  def invokeExpansion1(
      func: UnaryFunc,
      values: Func.Input[Inner, nat._1]):
    QScriptInternal[T, Inner] =
    func match {
      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - 12
      //   id(p, x:bar) - 18
      //   id(p, x:foo) - 1
      //   id(p, x:bar) - 2
      // (one bucket)
      case structural.FlattenMap =>
        SourcedPathableInternal[T].inj(LeftShift(
          values(0),
          UnitF,
          Free.point(RightSide)))

      case structural.FlattenArray =>
        SourcedPathableInternal[T].inj(LeftShift(
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
        SourcedPathableInternal[T].inj(LeftShift(
          values(0),
          Free.roll(DupMapKeys(UnitF)),
          Free.point(RightSide)))
      case structural.FlattenArrayIndices =>
        SourcedPathableInternal[T].inj(LeftShift(
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
        QScriptBucketInternal[T].inj(LeftShiftBucket(
          values(0),
          UnitF,
          Free.point(RightSide),
          Free.roll(DupMapKeys(UnitF)))) // affects bucketing metadata
      case structural.ShiftArray =>
        QScriptBucketInternal[T].inj(LeftShiftBucket(
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
        QScriptBucketInternal[T].inj(LeftShiftBucket(
          values(0),
          Free.roll(DupMapKeys(UnitF)),
          Free.point(RightSide),
          UnitF)) // affects bucketing metadata
      case structural.ShiftArrayIndices =>
        QScriptBucketInternal[T].inj(LeftShiftBucket(
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
    val bucketable = implicitly[Bucketable.Aux[T, QScriptInternal[T, ?]]]

    val bucketDisj: QScriptBucket[T, Inner] \/ (Int, Inner) =
      inner.cataM(bucketable.digForBucket).run(1)

    bucketDisj match {
      case -\/(GroupBy(src, _, bucket)) =>
        val AbsMerge(merged, left, right) = merge(src, inner)
        makeBasicTheta(merged, left, right) map {
          case AbsMerge(src, mfl, mfr) =>
            (ThetaJoinInternal[T].inj(src).embed, rebase(bucket, mfl), mfr)
        }
      case -\/(BucketField(src, _, name)) =>
        val AbsMerge(merged, left, right) = merge(src, inner)
        makeBasicTheta(merged, left, right) map {
          case AbsMerge(src, mfl, mfr) =>
            (ThetaJoinInternal[T].inj(src).embed, rebase(name, mfl), mfr)
        }
      case -\/(BucketIndex(src, _, index)) =>
        val AbsMerge(merged, left, right) = merge(src, inner)
        makeBasicTheta(merged, left, right) map {
          case AbsMerge(src, mfl, mfr) =>
            (ThetaJoinInternal[T].inj(src).embed, rebase(index, mfl), mfr)
        }
      case -\/(LeftShiftBucket(src, struct, _, bucket)) =>
        val AbsMerge(merged, left, right) = merge(src, inner)
        makeBasicTheta(merged, left, right) map {
          case AbsMerge(src, mfl, mfr) =>
            (ThetaJoinInternal[T].inj(src).embed, rebase(rebase(struct, bucket), mfl), mfr)
        }
      // TODO in the cases below do we need to merge src?
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

  def invokeThetaJoin(input: Func.Input[Inner, nat._3], tpe: JoinType): ThetaJoin[T, Inner] = {
    val AbsMerge(src1, jbLeft, jbRight) = merge(input(0), input(1))
    val AbsMerge(src2, bothSides, cond) = merge(src1, input(2))

    val leftBr = rebase(bothSides, jbLeft)
    val rightBr = rebase(bothSides, jbRight)

    val onQS =
      transApoT[Free[?[_], JoinSide], QScriptInternal[T, ?]](transApoT[Free[?[_], JoinSide], QScriptInternal[T, ?]](cond.map[JoinSide](κ(RightSide)))(
        substitute[Free[?[_], JoinSide], QScriptInternal[T, ?]](jbLeft.map[JoinSide](κ(RightSide)), Free.point(LeftSide))))(
        substitute[Free[?[_], JoinSide], QScriptInternal[T, ?]](jbRight.map[JoinSide](κ(RightSide)), Free.point(RightSide)))

    val on: JoinFunc[T] = basicJF // TODO get from onQS to here somehow

    ThetaJoin(
      src2,
      leftBr,
      rightBr,
      on,
      Inner,
      Free.roll(ConcatObjects(
        Free.roll(MakeObject(StrLit("left"), Free.point(LeftSide))),
        Free.roll(MakeObject(StrLit("right"), Free.point(RightSide))))))
  }

  def pathToProj(path: pathy.Path[_, _, _]): FreeMap[T] =
    pathy.Path.peel(path).fold[FreeMap[T]](
      Free.point(())) {
      case (p, n) =>
        Free.roll(ProjectField(pathToProj(p),
          StrLit(n.fold(_.value, _.value))))
    }

  // TODO error handling
  def fromData[T[_[_]]: Corecursive](data: Data): String \/ T[EJson] = {
    data.hyloM[String \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[String \/ ?, EJson, Data, T[EJson]]({
        case data => data.toString.left[T[EJson]]
      },
        _.embed.right[String]),
      Data.toEJson[EJson].apply(_).right)
  }

  def lpToQScript: LogicalPlan[Inner] => QSState[QScriptInternal[T, Inner]] = {
    case LogicalPlan.ReadF(path) =>
      stateT(SourcedPathableInternal[T].inj(Map(
        CorecursiveOps[T, QScriptInternal[T, ?]](DeadEndInternal[T].inj(Const[DeadEnd, Inner](Root))).embed,
        pathToProj(path))))

    case LogicalPlan.ConstantF(data) =>
      stateT(SourcedPathableInternal[T].inj(Map(
        DeadEndInternal[T].inj(Const[DeadEnd, Inner](Root)).embed,
        Free.roll[MapFunc[T, ?], Unit](Nullary[T, FreeMap[T]](fromData(data).fold(
          error => CommonEJson.inj(ejson.Str[T[EJson]](error)).embed,
          x => x))))))

    case LogicalPlan.FreeF(name) =>
      stateT(SourcedPathableInternal[T].inj(Map(
        DeadEndInternal[T].inj(Const[DeadEnd, Inner](Empty)).embed,
        Free.roll(ProjectField(StrLit(name.toString), UnitF[T])))))

    case LogicalPlan.LetF(name, form, body) =>
      val AbsMerge(src, jb1, jb2) = merge(form, body)
      for {
        tmpName <- freshName("let").lift[PlannerError \/ ?]
        theta <- makeBasicTheta(src, jb1, jb2)
      } yield {
        SourcedPathableInternal[T].inj(Map(
          SourcedPathableInternal[T].inj(Map(
            ThetaJoinInternal[T].inj(theta.src).embed,
            Free.roll(ConcatObjects(
              Free.roll(MakeObject(StrLit(tmpName), UnitF[T])),
              Free.roll(MakeObject(StrLit(name.toString), theta.left)))))).embed,
          rebase(theta.right, Free.roll(ProjectField(UnitF[T], StrLit(tmpName))))))
      }

    case LogicalPlan.TypecheckF(expr, typ, cont, fallback) =>
      merge3Map(
        Func.Input3(expr, cont, fallback))(
        Guard(_, typ, _, _)) map {
          SourcedPathableInternal[T].inj(_)
        }

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Mapping =>
      stateT(SourcedPathableInternal[T].inj(invokeMapping1(func, Func.Input1(a1))))

    case LogicalPlan.InvokeFUnapply(structural.ObjectProject, Sized(a1, a2)) =>
      val AbsMerge(merged, left, right) = merge(a1, a2)
      makeBasicTheta(merged, left, right) map {
        case AbsMerge(src, mfl, mfr) =>
          QScriptBucketInternal[T].inj(BucketField(
            ThetaJoinInternal[T].inj(src).embed,
            mfl,
            mfr))
      }

    case LogicalPlan.InvokeFUnapply(structural.ArrayProject, Sized(a1, a2)) =>
      val AbsMerge(merged, left, right) = merge(a1, a2)
      makeBasicTheta(merged, left, right) map {
        case AbsMerge(src, mfl, mfr) =>
          QScriptBucketInternal[T].inj(BucketIndex(
            ThetaJoinInternal[T].inj(src).embed,
            mfl,
            mfr))
      }

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect == Mapping =>
      invokeMapping2(func, Func.Input2(a1, a2)) map {
        SourcedPathableInternal[T].inj(_)
      }

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3)) if func.effect == Mapping =>
      invokeMapping3(func, Func.Input3(a1, a2, a3)) map {
        SourcedPathableInternal[T].inj(_)
      }

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Reduction =>
      invokeReduction1(func, Func.Input1(a1)) map {
        QScriptCoreInternal[T].inj(_)
      }

    case LogicalPlan.InvokeFUnapply(set.Take, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)
      stateT(QScriptCoreInternal[T].inj(Take(src, jb1, jb2)))

    case LogicalPlan.InvokeFUnapply(set.Drop, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)
      stateT(QScriptCoreInternal[T].inj(Drop(src, jb1, jb2)))

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
          QScriptCoreInternal[T].inj(Sort(
            ThetaJoinInternal[T].inj(src).embed,
            rebase(bucket, buckets),
            pairs))
        }).liftM[StateT[?[_], NameGen, ?]]
      }).join
    }

    case LogicalPlan.InvokeFUnapply(set.Filter, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)

      makeBasicTheta(src, jb1, jb2) map {
        case AbsMerge(src, fm1, fm2) =>
          SourcedPathableInternal[T].inj(Map(
            QScriptCoreInternal[T].inj(Filter(ThetaJoinInternal[T].inj(src).embed, fm2)).embed,
            fm1))
      }

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Squashing =>
      stateT(func match {
        case identity.Squash => QScriptBucketInternal[T].inj(SquashBucket(a1))
      })

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Expansion =>
      stateT(invokeExpansion1(func, Func.Input1(a1)))

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect == Expansion =>
      invokeExpansion2(func, Func.Input2(a1, a2)) map {
        SourcedPathableInternal[T].inj(_)
      }

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect == Transformation =>
      func match {
        case set.GroupBy =>
          val AbsMerge(merged0, source0, bucket0) = merge(a1, a2)
          makeBasicTheta(merged0, source0, bucket0) map {
            case AbsMerge(merged, source, bucket) =>
              QScriptBucketInternal[T].inj(GroupBy(ThetaJoinInternal[T].inj(merged).embed, source, bucket))
          }
        case set.Union =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          stateT(SourcedPathableInternal[T].inj(Union(src, jb1, jb2)))
        case set.Intersect =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          stateT(ThetaJoinInternal[T].inj(ThetaJoin(src, jb1, jb2, basicJF, Inner, Free.point(LeftSide))))
        case set.Except =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          stateT(ThetaJoinInternal[T].inj(ThetaJoin(
            src,
            jb1,
            jb2,
            Free.roll(Nullary(CommonEJson.inj(ejson.Bool[T[EJson]](false)).embed)),
            LeftOuter,
            Free.point(LeftSide))))
      }

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))  if func.effect == Transformation=>
      def invoke(tpe: JoinType): QScriptInternal[T, Inner] =
        ThetaJoinInternal[T].inj(invokeThetaJoin(Func.Input3(a1, a2, a3), tpe))

      stateT(func match {
        case set.InnerJoin      => invoke(Inner)
        case set.LeftOuterJoin  => invoke(LeftOuter)
        case set.RightOuterJoin => invoke(RightOuter)
        case set.FullOuterJoin  => invoke(FullOuter)
      })
  }
}

class Optimize[T[_[_]]: Recursive: Corecursive](
    implicit eqTEj: Equal[T[EJson]]) extends Helpers[T] {

  def elideNopMap[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F):
      SourcedPathable[T, T[F]] => F[T[F]] = {
    case Map(src, mf) if mf ≟ UnitF => src.project
    case x                          => SP.inj(x)
  }

  // def normalizeMapFunc: SourcedPathable[T, ?] ~> SourcedPathable[T, ?] =
  //   new (SourcedPathable[T, ?] ~> SourcedPathable[T, ?]) {
  //     def apply[A](sp: SourcedPathable[T, A]) = sp match {
  //       case Map(src, f) => Map(src, f.transCata(repeatedly(MapFunc.normalize)))
  //       case x           => x
  //     }
  //   }

  def elideNopJoin[F[_]](
    implicit Th: ThetaJoin[T, ?] :<: F,
    SP: SourcedPathable[T, ?] :<: F):
      ThetaJoin[T, T[F]] => F[T[F]] = {
    case ThetaJoin(src, l, r, on, _, combine)
        if l ≟ Free.point(()) && r ≟ Free.point(()) && on ≟ basicJF =>
      SP.inj(Map(src, combine.void))
    case x => Th.inj(x)
  }

  // TODO write extractor for inject
  def coalesceMaps[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F):
      SourcedPathable[T, T[F]] => SourcedPathable[T, T[F]] = {
    case x @ Map(Embed(src), mf) => SP.prj(src) match {
      case Some(Map(srcInner, mfInner)) => Map(srcInner, rebase(mf, mfInner))
      case _ => x
    }
    case x => x
  }
}
