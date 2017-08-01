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

package quasar.connector

import slamdata.Predef._

import org.scalacheck.Arbitrary.{arbitrary => arb}
import org.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazArbitrary._

trait EnvironmentErrorArbitrary {
  implicit val arbEnvironmentError: Arbitrary[EnvironmentError] =
    Arbitrary(Gen.oneOf(
      arb[Throwable] ∘ (EnvironmentError.ConnectionFailed(_)),
      arb[String] ∘ (EnvironmentError.InvalidCredentials(_)),
      (arb[String] ⊛ arb[String])(EnvironmentError.UnsupportedVersion(_, _))))
}

object EnvironmentErrorArbitrary extends EnvironmentErrorArbitrary
