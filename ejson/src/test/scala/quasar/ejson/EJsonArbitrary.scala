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

package quasar.ejson

import quasar.fp._
import quasar.pkg.tests._

import matryoshka.Delay

trait EJsonArbitrary {
  implicit val arbitraryCommon: Delay[Arbitrary, Common] =
    new Delay[Arbitrary, Common] {
      def apply[α](arb: Arbitrary[α]) = Arbitrary(
        Gen.oneOf(
          arb.list      ^^ Arr[α],
          const(           Null[α]()),
          genBool       ^^ Bool[α],
          genString     ^^ Str[α],
          genBigDecimal ^^ Dec[α]
        )
      )
    }

  implicit val arbitraryObj: Delay[Arbitrary, Obj] =
    new Delay[Arbitrary, Obj] {
      def apply[α](arb: Arbitrary[α]) =
        (genString, arb.gen).zip.list ^^ (l => Obj(l.toListMap))
    }

  implicit val arbitraryExtension: Delay[Arbitrary, Extension] =
    new Delay[Arbitrary, Extension] {
      def apply[α](arb: Arbitrary[α]) = Arbitrary(
        Gen.oneOf(
          (arb.gen, arb.gen).zip      ^^ (Meta[α] _).tupled,
          (arb.gen, arb.gen).zip.list ^^ Map[α],
          genByte                     ^^ Byte[α],
          genChar                     ^^ Char[α],
          genBigInt                   ^^ Int[α]
        )
      )
    }
}

object EJsonArbitrary extends EJsonArbitrary
