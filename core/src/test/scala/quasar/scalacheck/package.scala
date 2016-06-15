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

import org.scalacheck.{Gen, Arbitrary, Shrink}
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

package object scalacheck {
  def nonEmptyListSmallerThan[A: Arbitrary](n: Int): Arbitrary[NonEmptyList[A]] = {
    val listGen = Gen.containerOfN[List,A](n, Arbitrary.arbitrary[A])
    Apply[Arbitrary].apply2[A, List[A], NonEmptyList[A]](Arbitrary(Arbitrary.arbitrary[A]), Arbitrary(listGen))((a, rest) =>
      NonEmptyList.nel(a, IList.fromList(rest)))
  }

  def listSmallerThan[A: Arbitrary](n: Int): Arbitrary[List[A]] =
    Arbitrary(Gen.containerOfN[List,A](n, Arbitrary.arbitrary[A]))


  implicit def shrinkIList[A](implicit s: Shrink[List[A]]): Shrink[IList[A]] =
    Shrink(as => s.shrink(as.toList).map(IList.fromFoldable(_)))

  implicit def shrinkISet[A: Order](implicit s: Shrink[Set[A]]): Shrink[ISet[A]] =
    Shrink(as => s.shrink(as.toSet).map(ISet.fromFoldable(_)))
}
