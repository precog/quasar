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

package quasar.qsu

import slamdata.Predef._
import quasar.{RenderTree, RenderTreeT, RenderedTree}
import quasar.common.{JoinType, SortDir}
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.contrib.matryoshka._
import quasar.fp.ski.{ι, κ}
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript._
import quasar.qscript.RecFreeS._
import quasar.qscript.provenance.JoinKeys

import matryoshka.{Hole => _, birecursiveIso => _, _} // {delayEqual, equalTEqual, delayShow, showTShow, BirecursiveT, Delay, Embed, EqualT, ShowT}
import matryoshka.data._
import matryoshka.patterns.{CoEnv, EnvT}
import monocle.{Iso, PTraversal, Prism}
import pathy.Path

import scalaz.{Applicative, Bitraverse, Cofree, Enum, Equal, Forall, Free, Functor, Id, Order, Scalaz, Show, Traverse, \/, \/-, NonEmptyList => NEL}
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.show._
import scalaz.syntax.std.option._

sealed trait QScriptUniform[T[_[_]], A] extends Product with Serializable

object QScriptUniform {

  implicit def traverse[T[_[_]]]: Traverse[QScriptUniform[T, ?]] = new Traverse[QScriptUniform[T, ?]] {
    // we need both apply and traverse syntax, which conflict
    import Scalaz._

    def traverseImpl[G[_]: Applicative, A, B](qsu: QScriptUniform[T, A])(f: A => G[B])
        : G[QScriptUniform[T, B]] = qsu match {
      case AutoJoin2(left, right, combiner) =>
        (f(left) |@| f(right))(AutoJoin2(_, _, combiner))

      case AutoJoin3(left, center, right, combiner) =>
        (f(left) |@| f(center) |@| f(right))(AutoJoin3(_, _, _, combiner))

      case QSAutoJoin(left, right, keys, combiner) =>
        (f(left) |@| f(right))(QSAutoJoin(_, _, keys, combiner))

      case GroupBy(left, right) =>
        (f(left) |@| f(right))(GroupBy(_, _))

      case DimEdit(source, dtrans) =>
        f(source).map(DimEdit(_, dtrans))

      case LPJoin(left, right, condition, joinType, leftRef, rightRef) =>
        (f(left) |@| f(right) |@| f(condition))(LPJoin(_, _, _, joinType, leftRef, rightRef))

      case ThetaJoin(left, right, condition, joinType, combiner) =>
        (f(left) |@| f(right))(ThetaJoin(_, _, condition, joinType, combiner))

      case Unary(source, mf) =>
        f(source).map(Unary(_, mf))

      case Map(source, fm) =>
        f(source).map(Map(_, fm))

      case Read(path) => (Read(path): QScriptUniform[T, B]).point[G]

      case Transpose(source, retain, rotations) =>
        f(source).map(Transpose(_, retain, rotations))

      case LeftShift(source, struct, idStatus, onUndefined, repair, rot) =>
        f(source).map(LeftShift(_, struct, idStatus, onUndefined, repair, rot))

      case MultiLeftShift(source, shifts, onUndefined, repair) =>
        f(source).map(MultiLeftShift(_, shifts, onUndefined, repair))

      case LPReduce(source, reduce) =>
        f(source).map(LPReduce(_, reduce))

      case QSReduce(source, buckets, reducers, repair) =>
        f(source).map(QSReduce(_, buckets, reducers, repair))

      case Distinct(source) =>
        f(source).map(Distinct(_))

      case LPSort(source, order) =>
        val T = Bitraverse[(?, ?)].leftTraverse[SortDir]

        val source2G = f(source)
        val orders2G = order.traverse(p => T.traverse(p)(f))

        (source2G |@| orders2G)(LPSort(_, _))

      case QSSort(source, buckets, order) =>
        f(source).map(QSSort(_, buckets, order))

      case Union(left, right) =>
        (f(left) |@| f(right))(Union(_, _))

      case Subset(from, op, count) =>
        (f(from) |@| f(count))(Subset(_, op, _))

      case LPFilter(source, predicate) =>
        (f(source) |@| f(predicate))(LPFilter(_, _))

      case QSFilter(source, predicate) =>
        f(source).map(QSFilter(_, predicate))

      case JoinSideRef(id) => (JoinSideRef(id): QScriptUniform[T, B]).point[G]

      case Unreferenced() => (Unreferenced(): QScriptUniform[T, B]).point[G]
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def show[T[_[_]]: ShowT]: Delay[Show, QScriptUniform[T, ?]] =
    new Delay[Show, QScriptUniform[T, ?]] {
      def apply[A](a: Show[A]) = {
        implicit val showA = a
        Show.shows {
          case AutoJoin2(left, right, combiner) =>
            s"AutoJoin2(${left.shows}, ${right.shows}, ${combiner.shows})"

          case AutoJoin3(left, center, right, combiner) =>
            s"AutoJoin3(${left.shows}, ${center.shows}, ${right.shows}, ${combiner.shows})"

          case QSAutoJoin(left, right, keys, combiner) =>
            s"QSAutoJoin(${left.shows}, ${right.shows}, ${keys.shows}, ${combiner.shows})"

          case GroupBy(left, right) =>
            s"GroupBy(${left.shows}, ${right.shows})"

          case DimEdit(source, dtrans) =>
            s"DimEdit(${source.shows}, ${dtrans.shows})"

          case LPJoin(left, right, condition, joinType, leftRef, rightRef) =>
            s"LPJoin(${left.shows}, ${right.shows}, ${condition.shows}, ${joinType.shows}, ${leftRef.shows}, ${rightRef.shows})"

          case ThetaJoin(left, right, condition, joinType, combiner) =>
            s"ThetaJoin(${left.shows}, ${right.shows}, ${condition.shows}, ${joinType.shows}, ${combiner.shows})"

          case Unary(source, mf) =>
            s"Unary(${source.shows}, ${mf.shows})"

          case Map(source, fm) =>
            s"Map(${source.shows}, ${fm.shows})"

          case Read(path) =>
            s"Read(${Path.posixCodec.printPath(path)})"

          case Transpose(source, retain, rotations) =>
            s"Transpose(${source.shows}, ${retain.shows}, ${rotations.shows})"

          case LeftShift(source, struct, idStatus, onUndefined, repair, rot) =>
            s"LeftShift(${source.shows}, ${struct.linearize.shows}, ${idStatus.shows}, ${onUndefined.shows}, ${repair.shows}, ${rot.shows})"

          case MultiLeftShift(source, shifts, onUndefined, repair) =>
            s"MultiLeftShift(${source.shows}, ${shifts.shows}, ${onUndefined.shows}, ${repair.shows})"

          case LPReduce(source, reduce) =>
            s"LPReduce(${source.shows}, ${reduce.shows})"

          case QSReduce(source, buckets, reducers, repair) =>
            s"QSReduce(${source.shows}, ${buckets.shows}, ${reducers.shows}, ${repair.shows})"

          case Distinct(source) =>
            s"Distinct(${source.shows})"

          case LPSort(source, order) =>
            s"LPSort(${source.shows}, ${order.shows})"

          case QSSort(source, buckets, order) =>
            s"QSSort(${source.shows}, ${buckets.shows}, ${order.shows})"

          case Union(left, right) =>
            s"Union(${left.shows}, ${right.shows})"

          case Subset(from, op, count) =>
            s"Subset(${from.shows}, ${op.shows}, ${count.shows})"

          case LPFilter(source, predicate) =>
            s"LPFilter(${source.shows}, ${predicate.shows})"

          case QSFilter(source, predicate) =>
            s"QSFilter(${source.shows}, ${predicate.shows})"

          case JoinSideRef(id) =>
            s"JoinSideRef(${id.shows})"

          case Unreferenced() =>
            "⊥"
        }
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]
      : Delay[RenderTree, QScriptUniform[T, ?]] = ???

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  implicit def equal[T[_[_]]: BirecursiveT: EqualT]
      : Delay[Equal, QScriptUniform[T, ?]] = ???

  final case class AutoJoin2[T[_[_]], A](
      left: A,
      right: A,
      combiner: FreeMapA[T, JoinSide]) extends QScriptUniform[T, A]

  final case class AutoJoin3[T[_[_]], A](
      left: A,
      center: A,
      right: A,
      combiner: FreeMapA[T, JoinSide3]) extends QScriptUniform[T, A]

  final case class QSAutoJoin[T[_[_]], A](
    left: A,
    right: A,
    keys: JoinKeys[QIdAccess[T]],
    combiner: JoinFunc[T]) extends QScriptUniform[T, A]

  final case class GroupBy[T[_[_]], A](
      left: A,
      right: A) extends QScriptUniform[T, A]

  final case class DimEdit[T[_[_]], A](
      source: A,
      trans: DTrans[T]) extends QScriptUniform[T, A]

  sealed trait DTrans[T[_[_]]] extends Product with Serializable

  object DTrans {
    final case class Squash[T[_[_]]]() extends DTrans[T]
    final case class Group[T[_[_]]](getKey: FreeMap[T]) extends DTrans[T]

    implicit def show[T[_[_]]: ShowT]: Show[DTrans[T]] =
      Show.shows[DTrans[T]] {
        case Squash() => "Squash"
        case Group(k) => s"Group(${k.shows})"
      }
  }

  // LPish
  final case class LPJoin[T[_[_]], A](
      left: A,
      right: A,
      condition: A,
      joinType: JoinType,
      leftRef: Symbol,
      rightRef: Symbol) extends QScriptUniform[T, A]

  // QScriptish
  final case class ThetaJoin[T[_[_]], A](
      left: A,
      right: A,
      condition: JoinFunc[T],
      joinType: JoinType,
      combiner: JoinFunc[T]) extends QScriptUniform[T, A]

  /**
   * This is a non-free (as in monad) variant of Map.  We need it
   * in ReadLP so that graph compaction is defined, which is required
   * because compaction utilizes an `SMap[QSU[Symbol], Symbol]`, which
   * isn't valid when the `QSU`s inside the keys are libre.
   */
  final case class Unary[T[_[_]], A](
      source: A,
      mf: MapFunc[T, Hole]) extends QScriptUniform[T, A]

  final case class Map[T[_[_]], A](
      source: A,
      fm: RecFreeMap[T]) extends QScriptUniform[T, A]

  final case class Read[T[_[_]], A](path: AFile) extends QScriptUniform[T, A]

  // LPish
  final case class Transpose[T[_[_]], A](
      source: A,
      retain: Retain,
      rotations: Rotation) extends QScriptUniform[T, A]

  sealed trait Retain extends Product with Serializable {
    def fold[A](ids: => A, vals: => A): A = this match {
      case Retain.Identities => ids
      case Retain.Values => vals
    }
  }

  object Retain {
    case object Identities extends Retain
    case object Values extends Retain

    implicit val enum: Enum[Retain] =
      new Enum[Retain] {
        def succ(r: Retain) =
          r match {
            case Identities => Values
            case Values => Identities
          }

        def pred(r: Retain) =
          r match {
            case Identities => Values
            case Values => Identities
          }

        override val min = Some(Identities)
        override val max = Some(Values)

        def order(x: Retain, y: Retain) =
          Order[Int].order(toInt(x), toInt(y))

        val toInt: Retain => Int = {
          case Identities => 0
          case Values     => 1
        }
      }

    implicit val show: Show[Retain] =
      Show.showFromToString
  }

  sealed trait Rotation extends Product with Serializable

  object Rotation {
    case object FlattenArray extends Rotation
    case object ShiftArray extends Rotation
    case object FlattenMap extends Rotation
    case object ShiftMap extends Rotation

    implicit val enum: Enum[Rotation] =
      new Enum[Rotation] {
        def succ(r: Rotation) =
          r match {
            case FlattenArray => ShiftArray
            case ShiftArray => FlattenMap
            case FlattenMap => ShiftMap
            case ShiftMap => FlattenArray
          }

        def pred(r: Rotation) =
          r match {
            case FlattenArray => ShiftMap
            case ShiftArray => FlattenArray
            case FlattenMap => ShiftArray
            case ShiftMap => FlattenMap
          }

        override val min = Some(FlattenArray)
        override val max = Some(ShiftMap)

        def order(x: Rotation, y: Rotation) =
          Order[Int].order(toInt(x), toInt(y))

        val toInt: Rotation => Int = {
          case FlattenArray => 0
          case ShiftArray   => 1
          case FlattenMap   => 2
          case ShiftMap     => 3
        }
      }

    implicit val show: Show[Rotation] =
      Show.showFromToString
  }

  sealed trait ShiftTarget[T[_[_]]] extends Product with Serializable

  object ShiftTarget {
    final case class LeftTarget[T[_[_]]]() extends ShiftTarget[T]
    final case class RightTarget[T[_[_]]]() extends ShiftTarget[T]
    final case class AccessLeftTarget[T[_[_]]](access: QAccess[T, Hole]) extends ShiftTarget[T]

    implicit def equalShiftTarget[T[_[_]]: BirecursiveT: EqualT]: Equal[ShiftTarget[T]] = Equal.equal {
      case (AccessLeftTarget(access1), AccessLeftTarget(access2)) => access1 ≟ access2
      case (LeftTarget(), LeftTarget()) => true
      case (RightTarget(), RightTarget()) => true
      case _ => false
    }

    implicit def showShiftTarget[T[_[_]]: ShowT]: Show[ShiftTarget[T]] = Show.shows {
      case LeftTarget() => "LeftTarget"
      case RightTarget() => "RightTarget"
      case AccessLeftTarget(access) => s"AccessLeftTarget(${access.shows})"
    }

    implicit def renderShiftTarget[T[_[_]]: RecursiveT: RenderTreeT: ShowT]: RenderTree[ShiftTarget[T]] = RenderTree.make {
      case LeftTarget() =>
        RenderedTree("ShiftTarget" :: Nil, "LeftTarget".some, Nil)
      case RightTarget() =>
        RenderedTree("ShiftTarget" :: Nil, "RightTarget".some, Nil)
      case AccessLeftTarget(access) =>
        RenderedTree("ShiftTarget" :: Nil, "AccessLeftTarget".some, RenderTree[QAccess[T, Hole]].render(access) :: Nil)
    }
  }

  // QScriptish
  final case class LeftShift[T[_[_]], A](
      source: A,
      struct: RecFreeMap[T],
      idStatus: IdStatus,
      onUndefined: OnUndefined,
      repair: FreeMapA[T, ShiftTarget[T]],
      rot: Rotation) extends QScriptUniform[T, A]

  // shifting multiple structs on the same source;
  // horizontal composition of LeftShifts
  final case class MultiLeftShift[T[_[_]], A](
      source: A,
      shifts: List[(FreeMap[T], IdStatus, Rotation)],
      onUndefined: OnUndefined,
      repair: FreeMapA[T, QAccess[T, Hole] \/ Int]) extends QScriptUniform[T, A]

  // LPish
  final case class LPReduce[T[_[_]], A](
      source: A,
      reduce: ReduceFunc[Unit]) extends QScriptUniform[T, A]

  // QScriptish
  final case class QSReduce[T[_[_]], A](
      source: A,
      buckets: List[FreeMapA[T, QAccess[T, Hole]]],
      reducers: List[ReduceFunc[FreeMap[T]]],
      repair: FreeMapA[T, ReduceIndex]) extends QScriptUniform[T, A]

  final case class Distinct[T[_[_]], A](source: A) extends QScriptUniform[T, A]

  // LPish
  final case class LPSort[T[_[_]], A](
      source: A,
      order: NEL[(A, SortDir)]) extends QScriptUniform[T, A]

  // QScriptish
  final case class QSSort[T[_[_]], A](
      source: A,
      buckets: List[FreeMapA[T, QAccess[T, Hole]]],
      order: NEL[(FreeMap[T], SortDir)]) extends QScriptUniform[T, A]

  final case class Union[T[_[_]], A](left: A, right: A) extends QScriptUniform[T, A]

  final case class Subset[T[_[_]], A](
      from: A,
      op: SelectionOp,
      count: A) extends QScriptUniform[T, A]

  // LPish
  final case class LPFilter[T[_[_]], A](
      source: A,
      predicate: A) extends QScriptUniform[T, A]

  // QScriptish
  final case class QSFilter[T[_[_]], A](
      source: A,
      predicate: RecFreeMap[T]) extends QScriptUniform[T, A]

  final case class Unreferenced[T[_[_]], A]() extends QScriptUniform[T, A]

  final case class JoinSideRef[T[_[_]], A](id: Symbol) extends QScriptUniform[T, A]

  final class Optics[T[_[_]]] private () extends QSUTTypes[T] {
    def autojoin2[A]: Prism[QScriptUniform[A], (A, A, FreeMapA[JoinSide])] =
      Prism.partial[QScriptUniform[A], (A, A, FreeMapA[JoinSide])] {
        case AutoJoin2(left, right, func) => (left, right, func)
      } { case (left, right, func) => AutoJoin2(left, right, func) }

    def autojoin3[A]: Prism[QScriptUniform[A], (A, A, A, FreeMapA[JoinSide3])] =
      Prism.partial[QScriptUniform[A], (A, A, A, FreeMapA[JoinSide3])] {
        case AutoJoin3(left, center, right, func) => (left, center, right, func)
      } { case (left, center, right, func) => AutoJoin3(left, center, right, func) }

    def dimEdit[A]: Prism[QScriptUniform[A], (A, DTrans[T])] =
      Prism.partial[QScriptUniform[A], (A, DTrans[T])] {
        case DimEdit(a, dt) => (a, dt)
      } { case (a, dt) => DimEdit(a, dt) }

    def distinct[A]: Prism[QScriptUniform[A], A] =
      Prism.partial[QScriptUniform[A], A] {
        case Distinct(a) => a
      } (Distinct(_))

    def groupBy[A]: Prism[QScriptUniform[A], (A, A)] =
      Prism.partial[QScriptUniform[A], (A, A)] {
        case GroupBy(l, r) => (l, r)
      } { case (l, r) => GroupBy(l, r) }

    def joinSideRef[A]: Prism[QScriptUniform[A], Symbol] =
      Prism.partial[QScriptUniform[A], Symbol] {
        case JoinSideRef(s) => s
      } (JoinSideRef(_))

    def leftShift[A]: Prism[QScriptUniform[A], (A, RecFreeMap, IdStatus, OnUndefined, FreeMapA[ShiftTarget[T]], Rotation)] =
      Prism.partial[QScriptUniform[A], (A, RecFreeMap, IdStatus, OnUndefined, FreeMapA[ShiftTarget[T]], Rotation)] {
        case LeftShift(s, fm, ids, ou, jf, rot) => (s, fm, ids, ou, jf, rot)
      } { case (s, fm, ids, ou, jf, rot) => LeftShift(s, fm, ids, ou, jf, rot) }

    def multiLeftShift[A]: Prism[QScriptUniform[A], (A, List[(FreeMap, IdStatus, Rotation)], OnUndefined, FreeMapA[QAccess[Hole] \/ Int])] =
      Prism.partial[QScriptUniform[A], (A, List[(FreeMap, IdStatus, Rotation)], OnUndefined, FreeMapA[QAccess[Hole] \/ Int])] {
        case MultiLeftShift(s, ss, ou, map) => (s, ss, ou, map)
      } { case (s, ss, ou, map) => MultiLeftShift(s, ss, ou, map) }

    def lpFilter[A]: Prism[QScriptUniform[A], (A, A)] =
      Prism.partial[QScriptUniform[A], (A, A)] {
        case LPFilter(s, p) => (s, p)
      } { case (s, p) => LPFilter(s, p) }

    def lpJoin[A]: Prism[QScriptUniform[A], (A, A, A, JoinType, Symbol, Symbol)] =
      Prism.partial[QScriptUniform[A], (A, A, A, JoinType, Symbol, Symbol)] {
        case LPJoin(l, r, c, t, lr, rr) => (l, r, c, t, lr, rr)
      } { case (l, r, c, t, lr, rr) => LPJoin(l, r, c, t, lr, rr) }

    def lpReduce[A]: Prism[QScriptUniform[A], (A, ReduceFunc[Unit])] =
      Prism.partial[QScriptUniform[A], (A, ReduceFunc[Unit])] {
        case LPReduce(a, rf) => (a, rf)
      } { case (a, rf) => LPReduce(a, rf) }

    def lpSort[A]: Prism[QScriptUniform[A], (A, NEL[(A, SortDir)])] =
      Prism.partial[QScriptUniform[A], (A, NEL[(A, SortDir)])] {
        case LPSort(a, keys) => (a, keys)
      } { case (a, keys) => LPSort(a, keys) }

    def unary[A]: Prism[QScriptUniform[A], (A, MapFunc[Hole])] =
      Prism.partial[QScriptUniform[A], (A, MapFunc[Hole])] {
        case Unary(a, mf) => (a, mf)
      } { case (a, mf) => Unary(a, mf) }

    def map[A]: Prism[QScriptUniform[A], (A, RecFreeMap)] =
      Prism.partial[QScriptUniform[A], (A, RecFreeMap)] {
        case Map(a, fm) => (a, fm)
      } { case (a, fm) => Map(a, fm) }

    def qsAutoJoin[A]: Prism[QScriptUniform[A], (A, A, JoinKeys[QIdAccess], JoinFunc)] =
      Prism.partial[QScriptUniform[A], (A, A, JoinKeys[QIdAccess], JoinFunc)] {
        case QSAutoJoin(l, r, ks, c) => (l, r, ks, c)
      } { case (l, r, ks, c) => QSAutoJoin(l, r, ks, c) }

    def qsFilter[A]: Prism[QScriptUniform[A], (A, RecFreeMap)] =
      Prism.partial[QScriptUniform[A], (A, RecFreeMap)] {
        case QSFilter(a, p) => (a, p)
      } { case (a, p) => QSFilter(a, p) }

    def qsReduce[A]: Prism[QScriptUniform[A], (A, List[FreeAccess[Hole]], List[ReduceFunc[FreeMap]], FreeMapA[ReduceIndex])] =
      Prism.partial[QScriptUniform[A], (A, List[FreeAccess[Hole]], List[ReduceFunc[FreeMap]], FreeMapA[ReduceIndex])] {
        case QSReduce(a, bs, rfs, rep) => (a, bs, rfs, rep)
      } { case (a, bs, rfs, rep) => QSReduce(a, bs, rfs, rep) }

    def qsSort[A]: Prism[QScriptUniform[A], (A, List[FreeAccess[Hole]], NEL[(FreeMap, SortDir)])] =
      Prism.partial[QScriptUniform[A], (A, List[FreeAccess[Hole]], NEL[(FreeMap, SortDir)])] {
        case QSSort(a, buckets, keys) => (a, buckets, keys)
      } { case (a, buckets, keys) => QSSort(a, buckets, keys) }

    def read[A]: Prism[QScriptUniform[A], AFile] =
      Prism.partial[QScriptUniform[A], AFile] {
        case Read(f) => f
      } (Read(_))

    def subset[A]: Prism[QScriptUniform[A], (A, SelectionOp, A)] =
      Prism.partial[QScriptUniform[A], (A, SelectionOp, A)] {
        case Subset(f, op, c) => (f, op, c)
      } { case (f, op, c) => Subset(f, op, c) }

    def thetaJoin[A]: Prism[QScriptUniform[A], (A, A, JoinFunc, JoinType, JoinFunc)] =
      Prism.partial[QScriptUniform[A], (A, A, JoinFunc, JoinType, JoinFunc)] {
        case ThetaJoin(l, r, c, t, b) => (l, r, c, t, b)
      } { case (l, r, c, t, b) => ThetaJoin(l, r, c, t, b) }

    def transpose[A]: Prism[QScriptUniform[A], (A, Retain, Rotation)] =
      Prism.partial[QScriptUniform[A], (A, Retain, Rotation)] {
        case Transpose(a, ret, rot) => (a, ret, rot)
      } { case (a, ret, rot) => Transpose(a, ret, rot) }

    def union[A]: Prism[QScriptUniform[A], (A, A)] =
      Prism.partial[QScriptUniform[A], (A, A)] {
        case Union(l, r) => (l, r)
      } { case (l, r) => Union(l, r) }

    def unreferenced[A]: Prism[QScriptUniform[A], Unit] =
      Prism.partial[QScriptUniform[A], Unit] {
        case Unreferenced() => ()
      } (κ(Unreferenced()))

    def holes[A, B]: PTraversal[QScriptUniform[A], QScriptUniform[B], A, B] =
      PTraversal.fromTraverse[QScriptUniform, A, B]
  }

  object Optics {
    def apply[T[_[_]]]: Optics[T] = new Optics[T]
  }

  sealed abstract class Dsl[T[_[_]]: BirecursiveT, F[_]: Functor, A] extends QSUTTypes[T] {
    import Scalaz._

    val iso: Iso[A, F[QScriptUniform[A]]]
    def lifting[S, A]: Prism[S, A] => Prism[F[S], F[A]]
    val recFunc = construction.RecFunc[T]

    type Bin[A] = (A, A) => Binary[T, A]
    type Tri[A] = (A, A, A) => Ternary[T, A]

    private val O = Optics[T]

    def mfc[A] = PrismNT.injectCopK[MapFuncCore, MapFunc].asPrism[A]

    private def composeLifting[G[_]](optic: Prism[QScriptUniform[A], G[A]]) =
      iso composePrism lifting[QScriptUniform[A], G[A]](optic)

    def _autojoin2: Prism[A, F[(A, A, FreeMapA[JoinSide])]] = {
      type G[A] = (A, A, FreeMapA[JoinSide])
      composeLifting[G](O.autojoin2[A])
    }

    def _autojoin3: Prism[A, F[(A, A, A, FreeMapA[JoinSide3])]] = {
      type G[A] = (A, A, A, FreeMapA[JoinSide3])
      composeLifting[G](O.autojoin3[A])
    }

    def autojoin2(input: F[(A, A, Forall.CPS[Bin])]): A =
      _autojoin2(input.map {
        case (left, right, combiner) =>
          (left, right,
           Free.liftF(mfc(Forall[Bin](combiner)[JoinSide](LeftSide, RightSide))))
      })

    def autojoin3(input: F[(A, A, A, Forall.CPS[Tri])]): A =
      _autojoin3(input.map {
        case (left, center, right, combiner) =>
          (left, center, right,
           Free.liftF(mfc(Forall[Tri](combiner)[JoinSide3](LeftSide3, Center, RightSide3))))
      })

    def dimEdit: Prism[A, F[(A, DTrans[T])]] =
      composeLifting[(?, DTrans[T])](O.dimEdit[A])

    def distinct: Prism[A, F[A]] =
      composeLifting[Id](O.distinct[A])

    def groupBy: Prism[A, F[(A, A)]] = {
      type G[A] = (A, A)
      composeLifting[G](O.groupBy[A])
    }

    def joinSideRef: Prism[A, F[Symbol]] = {
      type G[A] = Symbol
      composeLifting[G](O.joinSideRef[A])
    }

    def leftShift: Prism[A, F[(A, RecFreeMap, IdStatus, OnUndefined, FreeMapA[ShiftTarget[T]], Rotation)]] = {
      composeLifting[(?, RecFreeMap, IdStatus, OnUndefined, FreeMapA[ShiftTarget[T]], Rotation)](O.leftShift[A])
    }

    def multiLeftShift: Prism[A, F[(A, List[(FreeMap, IdStatus, Rotation)], OnUndefined, FreeMapA[QAccess[Hole] \/ Int])]] = {
      composeLifting[(?, List[(FreeMap, IdStatus, Rotation)], OnUndefined, FreeMapA[QAccess[Hole] \/ Int])](O.multiLeftShift[A])
    }

    def lpFilter: Prism[A, F[(A, A)]] = {
      type G[A] = (A, A)
      composeLifting[G](O.lpFilter[A])
    }

    def lpJoin: Prism[A, F[(A, A, A, JoinType, Symbol, Symbol)]] = {
      type G[A] = (A, A, A, JoinType, Symbol, Symbol)
      composeLifting[G](O.lpJoin[A])
    }

    def lpReduce: Prism[A, F[(A, ReduceFunc[Unit])]] =
      composeLifting[(?, ReduceFunc[Unit])](O.lpReduce[A])

    def lpSort: Prism[A, F[(A, NEL[(A, SortDir)])]] = {
      type G[A] = (A, NEL[(A, SortDir)])
      composeLifting[G](O.lpSort[A])
    }

    def unary: Prism[A, F[(A, MapFunc[Hole])]] =
      composeLifting[(?, MapFunc[Hole])](O.unary[A])

    def map: Prism[A, F[(A, RecFreeMap)]] =
      composeLifting[(?, RecFreeMap)](O.map[A])

    def map1(pair: F[(A, MapFuncCore[Hole])]): A =
      map(pair.map {
        case(src, f) => (src, RecFreeS.roll(mfc(f.as(recFunc.Hole))))
      })

    def qsAutoJoin: Prism[A, F[(A, A, JoinKeys[QIdAccess], JoinFunc)]] = {
      type G[A] = (A, A, JoinKeys[QIdAccess], JoinFunc)
      composeLifting[G](O.qsAutoJoin[A])
    }

    def qsFilter: Prism[A, F[(A, RecFreeMap)]] =
      composeLifting[(?, RecFreeMap)](O.qsFilter[A])

    def qsReduce: Prism[A, F[(A, List[FreeAccess[Hole]], List[ReduceFunc[FreeMap]], FreeMapA[ReduceIndex])]] =
      composeLifting[(?, List[FreeAccess[Hole]], List[ReduceFunc[FreeMap]], FreeMapA[ReduceIndex])](O.qsReduce[A])

    def qsSort: Prism[A, F[(A, List[FreeAccess[Hole]], NEL[(FreeMap, SortDir)])]] =
      composeLifting[(?, List[FreeAccess[Hole]], NEL[(FreeMap, SortDir)])](O.qsSort[A])

    def read: Prism[A, F[AFile]] = {
      type G[_] = AFile
      composeLifting[G](O.read[A])
    }

    def subset: Prism[A, F[(A, SelectionOp, A)]] = {
      type G[A] = (A, SelectionOp, A)
      composeLifting[G](O.subset[A])
    }

    def thetaJoin: Prism[A, F[(A, A, JoinFunc, JoinType, JoinFunc)]] = {
      type G[A] = (A, A, JoinFunc, JoinType, JoinFunc)
      composeLifting[G](O.thetaJoin[A])
    }

    def transpose: Prism[A, F[(A, Retain, Rotation)]] =
      composeLifting[(?, Retain, Rotation)](O.transpose[A])

    def union: Prism[A, F[(A, A)]] = {
      type G[A] = (A, A)
      composeLifting[G](O.union[A])
    }

    def unreferenced: Prism[A, F[Unit]] = {
      type G[_] = Unit
      composeLifting[G](O.unreferenced[A])
    }
  }

  sealed abstract class DslT[T[_[_]]: BirecursiveT] private () extends Dsl[T, Id.Id, T[QScriptUniform[T, ?]]] {

    type QSU[A] = QScriptUniform[A]

    private val J = Fixed[T[EJson]]

    // read
    def tread(file: AFile): T[QSU] =
      transpose(read(file), Retain.Values, Rotation.ShiftMap)

    def tread1(name: String): T[QSU] =
      tread(Path.rootDir </> Path.file(name))

    // undefined
    val undefined: Prism[T[QSU], Unit] =
      Prism[T[QSU], Unit](map.getOption(_) collect {
        case (Unreferenced(), Embed(CoEnv(\/-(Suspend(MFC(MapFuncsCore.Undefined())))))) => ()
      })(_ => map(unreferenced(), recFunc.Undefined[Hole]))

    // constants
    val constant: Prism[T[QSU], T[EJson]] =
      Prism[T[QSU], T[EJson]](map.getOption(_) collect {
        case (Unreferenced(), Embed(CoEnv(\/-(Suspend(MFC(MapFuncsCore.Constant(ejs))))))) => ejs
      })(ejs => map(unreferenced(), recFunc.Constant[Hole](ejs)))

    val carr: Prism[T[QSU], List[T[EJson]]] =
      constant composePrism J.arr

    val cbool: Prism[T[QSU], Boolean] =
      constant composePrism J.bool

    val cbyte: Prism[T[QSU], Byte] =
      constant composePrism J.byte

    val cchar: Prism[T[QSU], Char] =
      constant composePrism J.char

    val cdec: Prism[T[QSU], BigDecimal] =
      constant composePrism J.dec

    val cint: Prism[T[QSU], BigInt] =
      constant composePrism J.int

    val cmap: Prism[T[QSU], List[(T[EJson], T[EJson])]] =
      constant composePrism J.map

    val cmeta: Prism[T[QSU], (T[EJson], T[EJson])] =
      constant composePrism J.meta

    val cnull: Prism[T[QSU], Unit] =
      constant composePrism J.nul

    val cstr: Prism[T[QSU], String] =
      constant composePrism J.str
  }

  object DslT {
    def apply[T[_[_]]: BirecursiveT]: DslT[T] =
      new DslT {
        val iso: Iso[T[QSU], QSU[T[QSU]]] = birecursiveIso[T[QSU], QSU]
        def lifting[S, A]: Prism[S, A] => Prism[S, A] = ι
      }
  }

  object AnnotatedDsl {
    import Scalaz._

    def apply[T[_[_]]: BirecursiveT, A]
        : Dsl[T, (A, ?), Cofree[QScriptUniform[T, ?], A]] = {

      type QSU[B] = QScriptUniform[T, B]
      type CoQSU = Cofree[QSU, A]

      new Dsl[T, (A, ?), CoQSU] {

        val iso: Iso[CoQSU, (A, QSU[CoQSU])] =
          birecursiveIso[CoQSU, EnvT[A, QSU, ?]]
            .composeIso(envTIso[A, QSU, CoQSU])

        def lifting[S, B]: Prism[S, B] => Prism[(A, S), (A, B)] =
          _.second[A]
      }
    }
  }
}
