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

package quasar.sst

import slamdata.Predef.{Char => SChar, _}
import quasar.fp.numeric.{SampleStats, SampleStatsArbitrary}
import quasar.pkg.tests._

import scalaz._
import scalaz.std.math.bigInt._
import scalaz.std.math.bigDecimal._
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.order._
import scalaz.scalacheck.ScalaCheckBinding._
import spire.algebra.Field

trait TypeStatArbitrary {
  import TypeStat._
  import SampleStatsArbitrary._

  // NB: This instance isn't found otherwise.
  private implicit val charOrder: Order[SChar] = scalaz.std.anyVal.char

  implicit def typeStatArbitrary[A: Arbitrary: Order: Field]: Arbitrary[TypeStat[A]] =
    Gen.oneOf(
      genBool[A],
      genColl[A],
      arbitrary[A] map (count[A](_)),
      (arbitrary[SampleStats[A]] |@| genRange[SChar])((s, r) => char(s, r._1, r._2)),
      (arbitrary[SampleStats[A]] |@| genRange[BigInt])((s, r) => int[A](s, r._1, r._2)),
      (arbitrary[SampleStats[A]] |@| genRange[BigDecimal])((s, r) => dec[A](s, r._1, r._2)))

  ////

  private def genBool[A: Arbitrary]: Gen[TypeStat[A]] =
    (arbitrary[A] |@| arbitrary[A])(bool[A](_, _))

  private def genRange[A: Arbitrary: Order]: Gen[(A, A)] =
    (arbitrary[A] |@| arbitrary[A])((b1, b2) => (b1 min b2, b1 max b2))

  private def genColl[A: Arbitrary: Order]: Gen[TypeStat[A]] =
    (arbitrary[A] |@| arbitrary[Option[A]] |@| arbitrary[Option[A]]) { (a, o1, o2) =>
      val (mn, mx) =
        if (o1.isEmpty || o2.isEmpty) (o1, o2)
        else (o1 min o2, o1 max o2)

      coll[A](a, mn, mx)
    }
}

object TypeStatArbitrary extends TypeStatArbitrary
