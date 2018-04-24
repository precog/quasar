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

package quasar

import slamdata.Predef.String

import argonaut.CodecJson
import org.scalacheck.{Arbitrary, Gen}
import org.specs2.scalaz._
import scalaz.scalacheck.ScalazProperties._

class CIStringSpec extends Spec with CIStringArbitrary {
  import CIStringSpec.AlphaStr

  checkAll(order.laws[CIString])

  "JSON codec laws" >> prop { ci: CIString =>
    CodecJson.codecLaw(CodecJson.derived[CIString])(ci)
  }

  "equal must ignore case for ascii characters" >> prop { as: AlphaStr =>
    CIString(as.value) equals CIString(as.value.toUpperCase)
  }
}

object CIStringSpec {
  final case class AlphaStr(value: String)

  object AlphaStr {
    implicit val arbitrary: Arbitrary[AlphaStr] =
      Arbitrary(Gen.alphaStr map (AlphaStr(_)))
  }
}
