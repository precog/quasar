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

package quasar.fp.numeric

import quasar.pkg.tests._

import org.scalacheck.Gen
import scalaz.Equal
import scalaz.std.list._
import spire.algebra.Field

trait SampleStatsArbitrary {
  implicit def arbitrarySampleStats[A: Arbitrary: Equal: Field]: Arbitrary[SampleStats[A]] =
    Arbitrary(for {
      n  <- Gen.choose(0, 20)
      ds <- Gen.listOfN(n, Gen.choose(-1000000.0, 1000000.0))
    } yield SampleStats.fromFoldable(ds.map(Field[A].fromDouble)))
}

object SampleStatsArbitrary extends SampleStatsArbitrary
