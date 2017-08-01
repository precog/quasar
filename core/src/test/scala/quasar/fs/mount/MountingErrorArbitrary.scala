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

package quasar.fs.mount

import quasar.connector.{EnvironmentError, EnvironmentErrorArbitrary}
import quasar.fs.{PathError, PathErrorArbitrary}
import slamdata.Predef._

import org.scalacheck.Arbitrary.{arbitrary => arb}
import org.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazArbitrary._

trait MountingErrorArbitrary
    extends EnvironmentErrorArbitrary
    with    MountConfigArbitrary
    with    MountTypeArbitrary
    with    PathErrorArbitrary {
  implicit val arbMountingError: Arbitrary[MountingError] =
    Arbitrary(Gen.oneOf(
      arb[PathError] ∘ (MountingError.PError(_)),
      arb[EnvironmentError] ∘ (MountingError.EError(_)),
      (arb[MountConfig] ⊛ arb[NonEmptyList[String]])(MountingError.InvalidConfig(_, _)),
      (arb[MountType] ⊛ arb[String])(MountingError.InvalidMount(_, _))))
}

object MountingErrorArbitrary extends MountingErrorArbitrary
