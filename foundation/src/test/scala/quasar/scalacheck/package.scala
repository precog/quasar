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

package quasar

import scala.Predef.$conforms
import slamdata.Predef._

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

  /** Resize a generator, applying a scale factor so that the resulting
    * values still respond to the incoming size value (which is controlled by
    * the `maxSize` parameter), but typically scaled down to produce more
    * modestly-sized values. */
  def scaleSize[A](gen: Gen[A], f: Int => Int): Gen[A] =
    Gen.sized(size => Gen.resize(f(size), gen))

  /** Scaling function raising the incoming size to some power, typically less
    * than 1. If your generator constructs values with dimension 2 (e.g.
    * `List[List[Int]]`), then 0.5 is a good choice. */
  def scalePow(exp: Double): Int => Int =
    size => scala.math.pow(size.toDouble, exp).toInt

  /** Scaling function which just adjusts the size so as to use at most
    * `desiredLimit`, assuming the default `maxSize` is in effect. */
  def scaleLinear(desiredLimit: Int): Int => Int = {
    val externalLimit = 200
    val factor = externalLimit/desiredLimit
    _/factor
  }
}
