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

package quasar.qsu.mra

import quasar.pkg.tests._

import org.scalacheck.Cogen

trait JoinKeyGenerator {
  implicit def arbitraryJoinKey[S: Arbitrary, V: Arbitrary]: Arbitrary[JoinKey[S, V]] =
    Arbitrary(Gen.oneOf(
      arbitrary[(V, V)].map(JoinKey.dynamic[S, V](_)),
      arbitrary[(S, V)].map(JoinKey.staticL(_)),
      arbitrary[(V, S)].map(JoinKey.staticR(_))))

  implicit def cogenJoinKey[S: Cogen, V: Cogen]: Cogen[JoinKey[S, V]] =
    Cogen((k, jk) => jk match {
      case JoinKey.Dynamic(l, r) => Cogen[(V, V)].perturb(k, (l, r))
      case JoinKey.StaticL(s, v) => Cogen[(S, V)].perturb(k, (s, v))
      case JoinKey.StaticR(v, s) => Cogen[(V, S)].perturb(k, (v, s))
    })
}

object JoinKeyGenerator extends JoinKeyGenerator
