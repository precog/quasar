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

package quasar.ejson

import slamdata.Predef._

import quasar.fp._
import quasar.pkg.tests._

import scala.Predef.$conforms

import scalaz.Equal
import scalaz.std.list._

trait EqMapArbitrary {
  implicit def arbitraryEqMap[K: Arbitrary: Equal, V: Arbitrary]: Arbitrary[EqMap[K, V]] =
    Arbitrary(arbitrary[List[(K, V)]] map (EqMap.fromFoldable(_)))
}

object EqMapArbitrary extends EqMapArbitrary
