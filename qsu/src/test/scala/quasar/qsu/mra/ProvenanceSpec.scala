/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu.mra

import slamdata.Predef.List

import quasar.Qspec
import quasar.qscript.OnUndefined

import cats.{Eq, Id, Order, Show}
import cats.data.NonEmptyList
import cats.instances.list._
import cats.kernel.laws.discipline.{BoundedSemilatticeTests, CommutativeMonoidTests}
import cats.syntax.eq._

import org.scalacheck.Arbitrary

import org.specs2.mutable.SpecificationLike
import org.specs2.scalacheck.Parameters

import org.typelevel.discipline.specs2.mutable.Discipline

import scalaz.@@
import scalaz.Tags.{Conjunction, Disjunction}

abstract class ProvenanceSpec[
    S: Arbitrary: Order,
    V: Arbitrary: Order,
    T: Arbitrary: Eq]
    extends Qspec
    with SpecificationLike
    with Discipline {

  // All values of the same type must be distinct.
  def ts: (T, T)
  def ss: (S, S, S)
  def vs: (V, V, V)

  val prov: Provenance[S, V, T]

  implicit def PArbitrary: Arbitrary[prov.P]
  implicit def PEq: Eq[prov.P]
  implicit def PShow: Show[prov.P]

  implicit def params: Parameters

  type P = prov.P

  import prov.applyComponentsN
  import prov.instances._
  import prov.syntax._

  implicit def ConjArb: Arbitrary[P @@ Conjunction] =
    Conjunction.subst(PArbitrary)

  implicit def ConjEq: Eq[P @@ Conjunction] =
    Conjunction.subst(PEq)

  implicit def DisjArb: Arbitrary[P @@ Disjunction] =
    Disjunction.subst(PArbitrary)

  implicit def DisjEq: Eq[P @@ Disjunction] =
    Disjunction.subst(PEq)

  def empty = prov.empty

  def t1 = ts._1
  def t2 = ts._2

  def s1 = ss._1
  def s2 = ss._2
  def s3 = ss._3

  def v1 = vs._1
  def v2 = vs._2
  def v3 = vs._3

  "prereqs" >> {
    "ts distinct" >> {
      t1 =!= t2
    }

    "ss distinct" >> {
      (s1 =!= s2) && (s1 =!= s3) && (s2 =!= s3)
    }

    "vs distinct" >> {
      (v1 =!= v2) && (v1 =!= v3) && (v2 =!= v3)
    }
  }

  "autojoin" >> {
    "extend :: T ⋈ extend :: T" >> prop { (vx: V, vy: V) =>
      val exp = JoinKeys.one(JoinKey.dynamic[S, V](vx, vy))

      (empty.inflateExtend(vx, t1) ⋈ empty.inflateExtend(vy, t1)).keys eqv exp
    }

    "extend :: T ⋈ extend :: U" >> prop { (vx: V, vy: V) =>
      (empty.inflateExtend(vx, t1) ⋈ empty.inflateExtend(vy, t2)).keys.isEmpty
    }

    "extend :: T ⋈ conjoin :: T" >> prop { (vx: V, vy: V, s: S) =>
      val exp = JoinKeys.one(JoinKey.dynamic[S, V](vx, vy))
      val b = empty.projectStatic(s, t2)

      (b.inflateExtend(vx, t1) ⋈ b.inflateConjoin(vy, t1)).keys eqv exp
    }

    "extend :: T ⋈ conjoin :: U" >> prop { (vx: V, vy: V, s: S) =>
      val b = empty.projectStatic(s, t2)
      (b.inflateExtend(vx, t1) ⋈ b.inflateConjoin(vy, t2)).keys.isEmpty
    }

    "conjoin :: T ⋈ conjoin :: T" >> prop { (vx: V, vy: V, s: S) =>
      val exp = JoinKeys.one(JoinKey.dynamic[S, V](vx, vy))
      val b = empty.projectStatic(s, t2)

      (b.inflateConjoin(vx, t1) ⋈ b.inflateConjoin(vy, t1)).keys eqv exp
    }

    "conjoin :: T ⋈ conjoin :: U" >> prop { (vx: V, vy: V, s: S) =>
      val b = empty.projectStatic(s, t1)
      (b.inflateConjoin(vx, t1) ⋈ b.inflateConjoin(vy, t2)).keys.isEmpty
    }

    "project :: T ⋈ extend :: T" >> prop { (s: S, v: V) =>
      val exp = JoinKeys.one(JoinKey.staticL(s, v))
      (empty.projectStatic(s, t1) ⋈ empty.inflateExtend(v, t1)).keys eqv exp
    }

    "project :: T ⋈ extend :: U" >> prop { (s: S, v: V) =>
      (empty.projectStatic(s, t1) ⋈ empty.inflateExtend(v, t2)).keys.isEmpty
    }

    "project :: T ⋈ conjoin :: T" >> prop { (sx: S, sy: S, v: V) =>
      val exp = JoinKeys.one(JoinKey.staticL(sy, v))
      val b = empty.projectStatic(sx, t1)

      (b.projectStatic(sy, t2) ⋈ b.inflateConjoin(v, t2)).keys eqv exp
    }

    "project :: T ⋈ conjoin :: U" >> prop { (sx: S, sy: S, v: V) =>
      val exp = JoinKeys.one(JoinKey.staticL(sy, v))
      val b = empty.projectStatic(sx, t1)

      (b.projectStatic(sy, t2) ⋈ b.inflateConjoin(v, t1)).keys.isEmpty
    }

    "commutativity" >> prop { (p: P, q: P) =>
      (p ⋈ q) eqv AutoJoin.keys[S, V].modify(_.mapKeys(_.flip))(q ⋈ p)
    }

    "mismatched project id halts joining" >> {
      val p = empty.projectStatic(s1, t1).inflateExtend(v1, t2)
      val q = empty.projectStatic(s2, t1).inflateExtend(v1, t2)

      (p ⋈ q).keys.isEmpty
    }

    "mismatched project type halts joining" >> {
      val p = empty.projectStatic(s1, t1).inflateExtend(v1, t2)
      val q = empty.projectStatic(s1, t2).inflateExtend(v1, t2)

      (p ⋈ q).keys.isEmpty
    }

    "conjunction of keys when joining conjunctions" >> {
      val p = empty.projectStatic(s1, t1).inflateExtend(v1, t1).inflateExtend(v2, t1)
      val q = empty.projectStatic(s1, t1).projectStatic(s2, t1).inflateExtend(v3, t1)

      val exp =
        AutoJoin[S, V](
          JoinKeys.conj(JoinKey.staticR(v1, s2), JoinKey.dynamic(v2, v3)),
          OnUndefined.Omit)

      (p ⋈ q) eqv exp
    }

    "join keys for all members of conjunction" >> {
      val p = empty.inflateExtend(v1, t1).projectStatic(s1, t1)
      val q = empty.projectStatic(s2, t1)

      val r = empty.inflateExtend(v2, t1).inflateExtend(v3, t1)

      val exp =
        AutoJoin(
          JoinKeys.conj[S, V](
            JoinKey.dynamic(v1, v2),
            JoinKey.staticL(s2, v2),
            JoinKey.staticL(s1, v3)),
          OnUndefined.Omit)

      ((p ∧ q) ⋈ r) eqv exp
    }

    "disjunction of conjunction of keys when join involves union" >> {
      val p = empty.projectStatic(s1, t1).inflateExtend(v3, t1)
      val q = empty.inflateExtend(v1, t1).inflateConjoin(v2, t1)

      val r = empty.inflateExtend(v1, t1).inflateExtend(v3, t1)

      val pkeys = JoinKeys.conj[S, V](JoinKey.staticL(s1, v1), JoinKey.dynamic(v3, v3))
      val qkeys = JoinKeys.conj[S, V](JoinKey.dynamic(v1, v1), JoinKey.dynamic(v2, v3))

      val exp = AutoJoin(pkeys ∨ qkeys, OnUndefined.Omit)

      ((p ∨ q) ⋈ r) eqv exp
    }

    "halt join for one member of conjunction while continuing with other" >> {
      val p = empty.inflateExtend(v1, t1).inflateExtend(v2, t1)
      val q = empty.inflateExtend(v1, t2).inflateExtend(v3, t1)

      val r = empty.projectStatic(s1, t1).inflateExtend(v3, t1)

      val keys = JoinKeys.conj[S, V](
        JoinKey.dynamic(v2, v3),
        JoinKey.staticR(v1, s1))

      val exp = AutoJoin(keys, OnUndefined.Omit)

      ((p ∧ q) ⋈ r) eqv exp
    }

    "emit on undefined when only joining with part of a union" >> {
      val p = empty.inflateExtend(v1, t1).inflateExtend(v2, t1)
      val q = empty.inflateExtend(v1, t2).inflateExtend(v3, t2)

      val keys = JoinKeys.conj[S, V](
        JoinKey.dynamic(v1, v1),
        JoinKey.dynamic(v2, v2))

      val exp = AutoJoin(keys, OnUndefined.Emit)

      ((p ∨ q) ⋈ p) eqv exp
    }
  }

  "and" >> {
    "consistent with applyComponentsN" >> prop { (p: P, q: P) =>
      val res = applyComponentsN[Id, NonEmptyList](_.reduceLeft(_ ∧ _))(NonEmptyList.of(p, q))

      res eqv (p ∧ q)
    }
  }

  "or" >> {
    "creates independent components" >> prop { (v: V, x: S, y: S, t: T) =>
      val p = empty.inflateExtend(v, t).projectStatic(x, t)
      val q = empty.inflateExtend(v, t).projectStatic(y, t)
      val ps = (p ∨ q).foldMapComponents(List(_))

      ps.exists(_ eqv p) && ps.exists(_ eqv q)
    }
  }

  "inflate submerge" >> {
    "id on empty" >> prop { (v: V, t: T) =>
      empty.inflateSubmerge(v, t) eqv empty
    }

    "equivalent to flipped inflate extend" >> prop { (p: P, vx: V, vy: V, t: T) =>
      p.inflateExtend(vx, t).inflateSubmerge(vy, t) eqv p.inflateExtend(vy, t).inflateExtend(vx, t)
    }

    "submerges under highest conjoined region" >> prop { (p: P, vx: V, vy: V, s: S, t: T) =>
      val x = p.inflateExtend(vx, t).projectStatic(s, t).inflateSubmerge(vy, t)
      val y = p.inflateExtend(vy, t).inflateExtend(vx, t).projectStatic(s, t)

      x eqv y
    }
  }

  "inject static" >> {
    "distributes over conjunction" >> prop { (p: P, q: P, s: S, t: T) => (p =!= empty && q =!= empty) ==> {
      (p ∧ q).injectStatic(s, t) eqv (p.injectStatic(s, t) ∧ q.injectStatic(s, t))
    }}

    "distributes over disjunction" >> prop { (p: P, q: P, s: S, t: T) => (p =!= empty && q =!= empty) ==> {
      (p ∨ q).injectStatic(s, t) eqv (p.injectStatic(s, t) ∨ q.injectStatic(s, t))
    }}
  }

  "project static" >> {
    "distributes over disjunction" >> prop { (p: P, q: P, s: S, t: T) => (p =!= empty && q =!= empty) ==> {
      (p ∨ q).projectStatic(s, t) eqv (p.projectStatic(s, t) ∨ q.projectStatic(s, t))
    }}

    "eliminates matching inject and all other contiguous injects" >> prop { (v: V, t: T, x: S, y: S, z: S) =>
        (x =!= y && x =!= z && y =!= z) ==> {
      val p = empty.inflateExtend(v, t)
      val a = p.injectStatic(x, t).injectStatic(y, t)
      val b = p.injectStatic(x, t).injectStatic(z, t)
      val c = p.injectStatic(y, t).injectStatic(x, t)

      val q = a ∧ b ∧ c
      val r = p ∧ p.injectStatic(y, t)

      q.projectStatic(x, t) eqv r
    }}

    "eliminates all contiguous injects when none match" >> prop { (v: V, t: T, x: S, y: S, z: S) =>
        (x =!= y && x =!= z && y =!= z) ==> {
      val p = empty.inflateExtend(v, t)
      val a = p.injectStatic(x, t).injectStatic(y, t)
      val b = p.injectStatic(x, t).injectStatic(z, t)
      val c = p.injectStatic(y, t).injectStatic(y, t)

      val q = a ∧ b ∧ c

      q.projectStatic(x, t) eqv p
    }}

    "eliminates inject of same id and type" >> prop { (p: P, s: S) =>
      p.injectStatic(s, t1).projectStatic(s, t1) eqv p
    }

    "does not extend when any injects exist" >> prop { (p: P, s: S, v: V, t: T) =>
      val q = empty.projectStatic(s1, t)
      val x = p.inflateExtend(v, t)
      val y = q.injectStatic(s, t)

      (x ∧ y).projectStatic(s, t) eqv (x ∧ q)
    }

    "affects unions independently" >> prop { (v: V, x: S, y: S, t: T) =>
      val p = empty.inflateExtend(v, t)
      val a = p.injectStatic(x, t)
      val b = p.projectStatic(y, t)

      val exp = (p ∨ b.projectStatic(x, t))

      (a ∨ b).projectStatic(x, t) eqv exp
    }
  }

  "reduce" >> {
    "inverse of inflate extend" >> prop { (p: P, v: V, t: T) =>
      p.inflateExtend(v, t).reduce eqv p
    }

    "inflate conjoin extends reducible region" >> prop { (p: P, vx: V, vy: V, t: T) =>
      p.inflateExtend(vx, t).inflateConjoin(vy, t).reduce eqv p
    }

    "project extends reducible region" >> prop { (p: P, v: V, s: S, t: T) =>
      p.inflateExtend(v, t).projectStatic(s, t).reduce eqv p
    }

    "inject extends reducible region" >> prop { (p: P, v: V, s: S, t: T) =>
      p.inflateExtend(v, t).injectStatic(s, t).reduce eqv p
    }

    "distributes over disjunction" >> prop { (p: P, q: P) =>
      (p ∨ q).reduce eqv (p.reduce ∨ q.reduce)
    }
  }

  "squash" >> {
    "conjoins all dimensions" >> prop { p: P =>
      p.squash.reduce eqv empty
    }
  }

  checkAll("CommutativeMonoid[P @@ Conjunction]", CommutativeMonoidTests[P @@ Conjunction].commutativeMonoid)
  checkAll("BoundedSemilattice[P @@ Disjunction]", BoundedSemilatticeTests[P @@ Disjunction].boundedSemilattice)
}
