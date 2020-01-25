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

package quasar.common

import slamdata.Predef._

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary

trait CPathGenerator {
  implicit val arbitraryCPathNode: Arbitrary[CPathNode] =
    Arbitrary(Gen.frequency(
      20 -> arbitrary[Int].map(CPathIndex(_)),
      20 -> arbitrary[String].map(CPathField(_)),
      20 -> arbitrary[String].map(CPathMeta(_)),
      1 -> Gen.const(CPathArray)))

  implicit val arbitraryCPath: Arbitrary[CPath] =
    Arbitrary(Gen.sized { sz =>
      Gen.listOfN(sz % 10, arbitrary[CPathNode]).map(CPath(_))
    })
}

object CPathGenerator extends CPathGenerator
