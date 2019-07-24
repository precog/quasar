/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar.pkg.tests._

import cats.Order
import cats.data.NonEmptyList

import org.scalacheck.Cogen

trait IdentitiesGenerator {
  import CatsNonEmptyListGenerator._

  implicit def arbitraryIdentities[A: Arbitrary: Order]: Arbitrary[Identities[A]] =
    Arbitrary(arbitrary[NonEmptyList[NonEmptyList[NonEmptyList[A]]]].map(Identities.collapsed(_)))

  implicit def cogenIdentities[A: Cogen]: Cogen[Identities[A]] =
    Cogen[NonEmptyList[NonEmptyList[NonEmptyList[A]]]].contramap(_.expanded)
}

object IdentitiesGenerator extends IdentitiesGenerator
