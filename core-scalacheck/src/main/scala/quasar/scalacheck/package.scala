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

package quasar

import quasar.Predef._

import org.scalacheck.{Gen, Arbitrary}
import scalaz.{Apply, NonEmptyList}
import scalaz.scalacheck.ScalaCheckBinding._

package object scalacheck {
  def nonEmptyListSmallerThan[A: Arbitrary](n: Int): Arbitrary[NonEmptyList[A]] = {
    val listGen = Gen.containerOfN[List,A](n, implicitly[Arbitrary[A]].arbitrary)
    Apply[Arbitrary].apply2[A, List[A], NonEmptyList[A]](implicitly[Arbitrary[A]], Arbitrary(listGen))(NonEmptyList.nel)
  }

  def listSmallerThan[A: Arbitrary](n: Int): Arbitrary[List[A]] =
    Arbitrary(Gen.containerOfN[List,A](n,implicitly[Arbitrary[A]].arbitrary))
}
