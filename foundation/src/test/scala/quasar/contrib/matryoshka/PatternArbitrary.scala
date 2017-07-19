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

package quasar.contrib.matryoshka

import slamdata.Predef._

import _root_.matryoshka.Delay
import org.scalacheck.{Arbitrary, Gen}
import scalaz._, Scalaz._

/** A helper trait for defining `Delay[Arbitrary, F]` instances that handles
  * halting corecursive generation by ensuring leaf nodes are produced
  * eventually.
  */
trait PatternArbitrary[F[_]] extends Delay[Arbitrary, F] {
  /** Weighted generators for the leaf terms of `F[_]` (those without a recursive parameter). */
  def leafGenerators[A]: NonEmptyList[(Int, Gen[F[A]])]

  /** Weighted generators for the branching terms of `F[_]` (those having recursive parameters). */
  def branchGenerators[A: Arbitrary]: NonEmptyList[(Int, Gen[F[A]])]

  /** Returns a uniformly weighted list of generators. */
  protected def uniformly[A](a: Gen[F[A]], as: Gen[F[A]]*): NonEmptyList[(Int, Gen[F[A]])] =
    NonEmptyList(a, as : _*) strengthL 1

  def apply[A](arb: Arbitrary[A]) = {
    val leaves   = leafGenerators[A].toList
    val genLeaf  = Gen.frequency(leaves : _*)
    val genAll   = Gen.frequency((leaves ::: branchGenerators(arb).toList) : _*)
    Arbitrary(Gen.sized(size => if (size <= 1) genLeaf else genAll))
  }
}
