/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef.Int

import quasar.Qspec

import scala.Predef.implicitly

import cats.Eq
import cats.instances.int._
import cats.instances.option._
import cats.kernel.laws.discipline.{BoundedSemilatticeTests, CommutativeMonoidTests, OrderTests}

import org.scalacheck.Arbitrary

import org.specs2.mutable.SpecificationLike
import org.specs2.scalacheck.Parameters

import org.typelevel.discipline.specs2.mutable.Discipline

import scalaz.@@
import scalaz.Tags.{Conjunction, Disjunction}

object UopSpec extends Qspec
    with SpecificationLike
    with UopGenerator
    with Discipline {

  import Uop._

  implicit val params = Parameters(maxSize = 10)

  implicit def IntConjArb: Arbitrary[Uop[Int] @@ Conjunction] =
    Conjunction.subst(implicitly[Arbitrary[Uop[Int]]])

  implicit def IntConjEq: Eq[Uop[Int] @@ Conjunction] =
    Conjunction.subst(Eq[Uop[Int]])

  implicit def DisjArb: Arbitrary[Uop[Int] @@ Disjunction] =
    Disjunction.subst(implicitly[Arbitrary[Uop[Int]]])

  implicit def DisjEq: Eq[Uop[Int] @@ Disjunction] =
    Disjunction.subst(Eq[Uop[Int]])

  checkAll("Order[Uop[Int]]", OrderTests[Uop[Int]].order)
  checkAll("CommutativeMonoid[Uop[Int] @@ Conjunction]", CommutativeMonoidTests[Uop[Int] @@ Conjunction].commutativeMonoid)
  checkAll("BoundedSemilattice[Uop[Int] @@ Disjunction]", BoundedSemilatticeTests[Uop[Int] @@ Disjunction].boundedSemilattice)
}
