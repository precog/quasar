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

package quasar.tpe

import slamdata.Predef.Option
import quasar.pkg.tests._

// NB: Something in here is needed by scalacheck's Arbitrary defs
//     for scala collections.
import scala.Predef._

import matryoshka.Delay
import scalaz._
import scalaz.scalacheck.ScalazArbitrary._

trait TypeFArbitrary {
  import TypeF._, SimpleTypeArbitrary._

  implicit def arbitraryTypeF[J: Arbitrary: Order]: Delay[Arbitrary, TypeF[J, ?]] =
    new Delay[Arbitrary, TypeF[J, ?]] {
      def apply[α](arb: Arbitrary[α]) = {
        implicit val arbA: Arbitrary[α] = arb
        val x = arbitrary[IList[α]]
        Arbitrary(Gen.oneOf(
          Gen.const(                                     bottom[J, α]() ),
          Gen.const(                                        top[J, α]() ),
          arbitrary[SimpleType]                   ^^ (   simple[J, α](_)),
          arbitrary[J]                            ^^ (    const[J, α](_)),
          arbitrary[IList[α] \/ α]                ^^ (      arr[J, α](_)),
          arbitrary[(IMap[J, α], Option[(α, α)])] ^^ (      map[J, α](_)),
          arbitrary[(α, α)]                       ^^ (coproduct[J, α](_))
        ))
      }
    }
}

object TypeFArbitrary extends TypeFArbitrary
