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

import slamdata.Predef.Set

import quasar.pkg.tests._

import cats.Order

import org.scalacheck.Cogen

trait UopGenerator {
  implicit def arbitraryUop[A: Arbitrary: Order]: Arbitrary[Uop[A]] =
    Arbitrary(for {
      sz <- Gen.frequency((32, 1), (16, 2), (8, 3), (4, 4), (2, 5), (1, 0))
      as <- Gen.listOfN(sz, arbitrary[A])
    } yield Uop.of(as: _*))

  implicit def cogenUop[A: Cogen: Order]: Cogen[Uop[A]] = {
    implicit val ording = Order[A].toOrdering
    Cogen[Set[A]].contramap(_.toSortedSet)
  }
}

object UopGenerator extends UopGenerator
