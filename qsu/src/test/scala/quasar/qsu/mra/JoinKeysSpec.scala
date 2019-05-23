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

package quasar.qsu.mra

import slamdata.Predef.{Char, Int}

import quasar.Qspec

import scala.Predef.implicitly

import cats.Eq
import cats.instances.char._
import cats.instances.int._
import cats.kernel.laws.discipline.{BoundedSemilatticeTests, CommutativeMonoidTests, EqTests}

import org.scalacheck.Arbitrary

import org.specs2.mutable.SpecificationLike
import org.specs2.scalacheck.Parameters

import org.typelevel.discipline.specs2.mutable.Discipline

import scalaz.@@
import scalaz.Tags.{Conjunction, Disjunction}

object JoinKeysSpec extends Qspec
    with SpecificationLike
    with JoinKeysGenerator
    with Discipline {

  import JoinKeys._

  implicit val params = Parameters(maxSize = 10)

  implicit def ConjArb: Arbitrary[JoinKeys[Char, Int] @@ Conjunction] =
    Conjunction.subst(implicitly[Arbitrary[JoinKeys[Char, Int]]])

  implicit def ConjEq: Eq[JoinKeys[Char, Int] @@ Conjunction] =
    Conjunction.subst(Eq[JoinKeys[Char, Int]])

  implicit def DisjArb: Arbitrary[JoinKeys[Char, Int] @@ Disjunction] =
    Disjunction.subst(implicitly[Arbitrary[JoinKeys[Char, Int]]])

  implicit def DisjEq: Eq[JoinKeys[Char, Int] @@ Disjunction] =
    Disjunction.subst(Eq[JoinKeys[Char, Int]])

  checkAll("Eq[JoinKeys[Char, Int]]", EqTests[JoinKeys[Char, Int]].eqv)
  checkAll("CommutativeMonoid[JoinKeys[Char, Int] @@ Conjunction]", CommutativeMonoidTests[JoinKeys[Char, Int] @@ Conjunction].commutativeMonoid)
  checkAll("BoundedSemilattice[JoinKeys[Char, Int] @@ Disjunction]", BoundedSemilatticeTests[JoinKeys[Char, Int] @@ Disjunction].boundedSemilattice)
}
