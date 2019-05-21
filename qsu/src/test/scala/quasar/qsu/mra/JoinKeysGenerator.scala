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

import slamdata.Predef.Set

import quasar.pkg.tests._

import cats.Order
import cats.data.NonEmptyList
import cats.instances.list._
import cats.syntax.foldable._

import org.scalacheck.Cogen

import scalaz.Tags.Disjunction
import scalaz.syntax.tag._

trait JoinKeysGenerator {
  import CatsNonEmptyListGenerator._
  import JoinKeyGenerator._

  implicit def arbitraryJoinKeys[S: Arbitrary: Order, V: Arbitrary: Order]: Arbitrary[JoinKeys[S, V]] = {
    import JoinKeys._
    Arbitrary(for {
      sz <- Gen.frequency((32, 1), (16, 2), (8, 3), (4, 4), (2, 5), (1, 0))
      cs <- Gen.listOfN(sz, arbitrary[NonEmptyList[JoinKey[S, V]]])
    } yield cs.foldMap(c => Disjunction(JoinKeys.conj(c.head, c.tail: _*))).unwrap)
  }

  implicit def cogenJoinKeys[S: Cogen: Order, V: Cogen: Order]: Cogen[JoinKeys[S, V]] = {
    implicit val ording = Order[NonEmptyList[JoinKey[S, V]]].toOrdering
    Cogen[Set[NonEmptyList[JoinKey[S, V]]]].contramap(_.toSortedSet.map(_.toNonEmptyList))
  }
}

object JoinKeysGenerator extends JoinKeyGenerator
