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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.contrib.scalacheck.gen

import java.time.LocalDate

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

/** Defines the domains of values for which the MongoDb connector is expected
  * to behave properly. May be mixed in when implementing `StdLibTestRunner`.
*/
trait MongoDbDomain {
  // NB: in the pipeline, the entire 64-bit range works, but in map-reduce,
  // only about 53 bits of integer resolution are available.
  // TODO: use `Long` in the Expr test, and this domain for JS.
  val intDomain = arbitrary[Int].filter(_ ≠ Int.MinValue).map(BigInt(_))
  val decDomain = arbitrary[Double].map(BigDecimal(_))

  // NB: restricted to printable ASCII only because most functions are not
  // well-defined for the rest (e.g. $toLower, $toUpper, $substr)
  val stringDomain = gen.printableAsciiString

  val dateDomain: Gen[LocalDate] =
    Gen.choose(
      LocalDate.of(1, 1, 1).toEpochDay,
      LocalDate.of(9999, 12, 31).toEpochDay
    ) ∘ (LocalDate.ofEpochDay(_))
}
