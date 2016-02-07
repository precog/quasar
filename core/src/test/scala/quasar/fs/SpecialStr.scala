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

package quasar
package fs

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}
import scalaz.Show
import scalaz.syntax.functor._
import scalaz.std.list._

/** A random string that favors special characters with more frequency. */
final case class SpecialStr(str: String) extends scala.AnyVal

object SpecialStr {
  implicit val specialStrArbitrary: Arbitrary[SpecialStr] =
    Arbitrary {
      val specialFreqs = "$./\\_~ *+-".toList map (Gen.const) strengthL 10

      Gen.nonEmptyListOf(Gen.frequency(
        (100, Arbitrary.arbitrary[Char]) :: specialFreqs : _*
      )) map (cs => SpecialStr(cs.mkString))
    }

  implicit val specialStrShow: Show[SpecialStr] =
    Show.shows(_.str)
}
