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

import org.scalacheck.Cogen

import ProvImpl.Vecs

trait VecsGenerator {
  import IdentitiesGenerator._

  implicit def arbitraryVecs[A: Arbitrary: Order]: Arbitrary[Vecs[A]] =
    Arbitrary(arbitrary[Identities[A]].map(Vecs.active(_)))

  implicit def cogenVecs[A: Order: Cogen]: Cogen[Vecs[A]] =
    Cogen[Identities[A]].contramap(_.united)
}

object VecsGenerator extends VecsGenerator
