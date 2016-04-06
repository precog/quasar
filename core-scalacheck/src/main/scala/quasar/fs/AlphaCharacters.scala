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

package quasar.fs

import quasar.Predef._

import org.scalacheck.{Gen, Arbitrary}
import scalaz.Show

final case class AlphaCharacters(value: String)

object AlphaCharacters {
  implicit val arb: Arbitrary[AlphaCharacters] =
    Arbitrary(Gen.nonEmptyListOf(Gen.alphaChar).map(chars => AlphaCharacters(chars.mkString)))
  implicit val show: Show[AlphaCharacters] = Show.shows(_.value)
}

/** Useful for debugging by producing easier to read paths but that still tend to trigger corner cases */
final case class AlphaAndSpecialCharacters(value: String)

object AlphaAndSpecialCharacters {
  implicit val arb: Arbitrary[AlphaAndSpecialCharacters] =
    Arbitrary(Gen.nonEmptyListOf(Gen.oneOf(Gen.alphaChar, Gen.const('/'), Gen.const('.'))).map(chars => AlphaAndSpecialCharacters(chars.mkString)))
  implicit val show: Show[AlphaAndSpecialCharacters] = Show.shows(_.value)
}
