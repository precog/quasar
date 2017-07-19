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

package quasar.qscript.provenance

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.fp._
import quasar.qscript._
import quasar.qscript.MapFuncsCore._

import scala.Predef.$conforms

import matryoshka._
import matryoshka.data._
import monocle.macros.Lenses
import scalaz._, Scalaz._

// TODO: Convert to fixed-point
sealed abstract class Provenance[T[_[_]]]
@Lenses final case class Nada[T[_[_]]]() extends Provenance[T]
@Lenses final case class Value[T[_[_]]](expr: FreeMap[T]) extends Provenance[T]
@Lenses final case class Proj[T[_[_]]](field: T[EJson]) extends Provenance[T]
@Lenses final case class Both[T[_[_]]](l: Provenance[T], r: Provenance[T])
    extends Provenance[T]
@Lenses final case class OneOf[T[_[_]]](l: Provenance[T], r: Provenance[T])
    extends Provenance[T]
@Lenses final case class Then[T[_[_]]](l: Provenance[T], r: Provenance[T])
    extends Provenance[T]

object Provenance {
  // TODO: This might not be the proper notion of equality – this just tells us
  //       which things align properly for autojoins.
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def equal[T[_[_]]: EqualT](implicit J: Equal[T[EJson]]): Equal[Provenance[T]] =
    Equal.equal {
      case (Nada(),        Nada())        => true
      case (Value(_),      Value(_))      => true
      case (Value(_),      Proj(_))       => true
      case (Proj(_),       Value(_))      => true
      case (Proj(d1),      Proj(d2))      => d1 ≟ d2
      case (Both(l1, r1),  Both(l2, r2))  => l1 ≟ l2 && r1 ≟ r2
      case (OneOf(l1, r1), OneOf(l2, r2)) =>
        l1 ≟ l2 && r1 ≟ r2 || l1 ≟ r2 && r1 ≟ l2
      case (Then(l1, r1),  Then(l2, r2))  => l1 ≟ l2 && r1 ≟ r2
      case (_,             _)             => false
    }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def show[T[_[_]]: ShowT]: Show[Provenance[T]] = Show.show {
    case Nada() => Cord("Nada")
    case Value(expr) => Cord("Value(") ++ expr.show ++ Cord(")")
    case Proj(field) => Cord("Proj(") ++ field.show ++ Cord(")")
    case Both(l, r) => Cord("Both(") ++ l.show ++ Cord(", ") ++ r.show ++ Cord(")")
    case OneOf(l, r) => Cord("OneOf(") ++ l.show ++ Cord(", ") ++ r.show ++ Cord(")")
    case Then(l, r) => Cord("Then(") ++ l.show ++ Cord(", ") ++ r.show ++ Cord(")")
  }
}

class ProvenanceT[T[_[_]]: CorecursiveT: EqualT](implicit J: Equal[T[EJson]]) extends TTypes[T] {
  type Provenance = quasar.qscript.provenance.Provenance[T]

  def genComparisons(lps: List[Provenance], rps: List[Provenance]): JoinFunc =
    lps.reverse.zip(rps.reverse).takeWhile { case (l, r) => l ≟ r }.reverse.map((genComparison(_, _)).tupled(_).toList).join match {
      case Nil    => BoolLit(true)
      case h :: t => t.foldLeft(h)((a, e) => Free.roll(MFC(And(a, e))))
    }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def genComparison(lp: Provenance, rp: Provenance): Option[JoinFunc] =
    (lp, rp) match {
      case (Value(v1), Value(v2)) => Free.roll(MFC(MapFuncsCore.Eq[T, JoinFunc](v1.as(LeftSide), v2.as(RightSide)))).some
      case (Value(v1), Proj(d2)) => Free.roll(MFC(MapFuncsCore.Eq[T, JoinFunc](v1.as(LeftSide), Free.roll(MFC(Constant(d2)))))).some
      case (Proj(d1), Value(v2)) => Free.roll(MFC(MapFuncsCore.Eq[T, JoinFunc](Free.roll(MFC(Constant(d1))), v2.as(RightSide)))).some
      case (Both(l1, r1),  Both(l2, r2)) =>
        genComparison(l1, l2).fold(
          genComparison(r1, r2))(
          lc => genComparison(r1, r2).fold(lc)(rc => Free.roll(MFC(And[T, JoinFunc](lc, rc)))).some)
      case (OneOf(l1, r1),  OneOf(l2, r2)) =>
        genComparison(l1, l2).fold(
          genComparison(r1, r2))(
          lc => genComparison(r1, r2).fold(lc)(rc => Free.roll(MFC(And[T, JoinFunc](lc, rc)))).some)
      case (Then(l1, r1),  Then(l2, r2)) =>
        genComparison(l1, l2).fold(
          genComparison(r1, r2))(
          lc => genComparison(r1, r2).fold(lc)(rc => Free.roll(MFC(And[T, JoinFunc](lc, rc)))).some)
      case (_, _) => None
    }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def rebase0(newBase: FreeMap): Provenance => Option[Provenance] = {
    case Value(expr) => Value(expr >> newBase).some
    case Both(l, r)  => (rebase0(newBase)(l), rebase0(newBase)(r)) match {
      case (None,     None)     => None
      case (None,     Some(r0)) => Both(l, r0).some
      case (Some(l0), None)     => Both(l0, r).some
      case (Some(l0), Some(r0)) => Both(l0, r0).some
    }
    case OneOf(l, r) => (rebase0(newBase)(l), rebase0(newBase)(r)) match {
      case (None,     None)     => None
      case (None,     Some(r0)) => OneOf(l, r0).some
      case (Some(l0), None)     => OneOf(l0, r).some
      case (Some(l0), Some(r0)) => OneOf(l0, r0).some
    }
    case Then(l, r)  => (rebase0(newBase)(l), rebase0(newBase)(r)) match {
      case (None,     None)     => None
      case (None,     Some(r0)) => Then(l, r0).some
      case (Some(l0), None)     => Then(l0, r).some
      case (Some(l0), Some(r0)) => Then(l0, r0).some
    }
    case _           => None
  }

  def rebase(newBase: FreeMap, ps: List[Provenance]): List[Provenance] =
    ps.map(orOriginal(rebase0(newBase)))

  /** Reifies the part of the provenance that must exist in the plan.
    */
  def genBuckets(ps: List[Provenance]): Option[(List[Provenance], FreeMap)] =
    ps.traverse(genBucket).eval(0).unzip.traverse(_.join match {
      case Nil      => None
      case h :: t   =>
        t.foldLeft(
          Free.roll(MFC(MakeArray[T, FreeMap](h))))(
          (a, e) => Free.roll(MFC(ConcatArrays(a, Free.roll(MFC(MakeArray(e))))))).some
    })

  val genBucket: Provenance => State[Int, (Provenance, List[FreeMap])] = {
    case Nada()      => (Nada[T](): Provenance, Nil: List[FreeMap]).point[State[Int, ?]]
    case Value(expr) =>
      State(i => (i + 1, (Value(Free.roll(MFC(ProjectIndex(HoleF, IntLit(i))))), List(expr))))
    case Proj(d)     => (Proj(d): Provenance, Nil: List[FreeMap]).point[State[Int, ?]]
    case Both(l, r)  => (genBucket(l) ⊛ genBucket(r)) {
      case ((lp, lf), (rp, rf)) => (Both(lp, rp), lf ++ rf)
    }
    case OneOf(l, r)  => (genBucket(l) ⊛ genBucket(r)) {
      case ((lp, lf), (rp, rf)) => (OneOf(lp, rp), lf ++ rf)
    }
    case Then(l, r)  => (genBucket(l) ⊛ genBucket(r)) {
      case ((lp, lf), (rp, rf)) => (Then(lp, rp), lf ++ rf)
    }
  }

  def joinProvenances(leftBuckets: List[Provenance], rightBuckets: List[Provenance]):
      List[Provenance] =
    leftBuckets.reverse.alignWith(rightBuckets.reverse) {
      case \&/.Both(l, r) => if (l ≟ r) l else Both(l, r)
      case \&/.This(l)    => Both(l, Nada())
      case \&/.That(r)    => Both(Nada(), r)
    }.reverse

  def unionProvenances(leftBuckets: List[Provenance], rightBuckets: List[Provenance]):
      List[Provenance] =
    leftBuckets.reverse.alignWith(rightBuckets.reverse) {
      case \&/.Both(l, r) => OneOf(l, r)
      case \&/.This(l)    => OneOf(l, Nada())
      case \&/.That(r)    => OneOf(Nada(), r)
    }.reverse

  def nestProvenances(buckets: List[Provenance]): List[Provenance] =
    buckets match {
      case a :: b :: tail => Then(a, b) :: tail
      case _              => buckets
    }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def squashProvenances(buckets: List[Provenance]): List[Provenance] =
    buckets match {
      case a :: b :: tail => squashProvenances(Then(a, b) :: tail)
      case _              => buckets
    }

  def swapProvenances(buckets: List[Provenance]): List[Provenance] =
    buckets match {
      case a :: b :: tail => b :: a :: tail
      case _              => buckets
    }
}
