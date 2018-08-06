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

package quasar.ejson

import slamdata.Predef._

import quasar.contrib.matryoshka.PatternArbitrary
import quasar.fp._
import quasar.pkg.tests._

import scala.Predef.$conforms

import matryoshka.Delay

trait EJsonArbitrary {
  implicit val arbitraryCommon: Delay[Arbitrary, Common] =
    new PatternArbitrary[Common] {
      def leafGenerators[A] =
        uniformly(
          const(Null[A]()),
          genBool       ^^ Bool[A],
          genString     ^^ Str[A],
          genBigDecimal ^^ Dec[A])

      def branchGenerators[A: Arbitrary] =
        uniformly(arbitrary[List[A]] ^^ Arr[A])
    }

  implicit val arbitraryObj: Delay[Arbitrary, Obj] =
    new Delay[Arbitrary, Obj] {
      def apply[α](arb: Arbitrary[α]) =
        (genString, arb.gen).zip.list ^^ (l => Obj(l.toListMap))
    }

  implicit val arbitraryExtension: Delay[Arbitrary, Extension] =
    new PatternArbitrary[Extension] {
      def leafGenerators[A] =
        uniformly(
          genChar   ^^ Char[A],
          genBigInt ^^ Int[A])

      def branchGenerators[A: Arbitrary] =
        uniformly(
          arbitrary[(A, A)] ^^ (Meta[A] _).tupled,
          arbitrary[List[(A, A)]] ^^ Map[A])
    }

  implicit val arbitraryTypeTag: Arbitrary[TypeTag] =
    Arbitrary(Gen.alphaNumStr map (TypeTag(_)))
}

object EJsonArbitrary extends EJsonArbitrary
