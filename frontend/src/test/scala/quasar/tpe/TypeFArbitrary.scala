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
import quasar.contrib.matryoshka.PatternArbitrary
import quasar.pkg.tests._

import scala.Predef.$conforms

import matryoshka.Delay
import scalaz._
import scalaz.scalacheck.ScalazArbitrary._

trait TypeFArbitrary {
  import TypeF._, SimpleTypeArbitrary._

  implicit def arbitraryTypeF[J: Arbitrary: Order]: Delay[Arbitrary, TypeF[J, ?]] =
    new PatternArbitrary[TypeF[J, ?]] {
      def leafGenerators[A] =
        uniformly(
          Gen.const(                bottom[J, A]() ),
          Gen.const(                   top[J, A]() ),
          arbitrary[SimpleType] ^^ (simple[J, A](_)),
          arbitrary[J]          ^^ ( const[J, A](_)))

      def branchGenerators[A: Arbitrary] =
        uniformly(
          arbitrary[IList[A] \/ A]                ^^ (      arr[J, A](_)),
          arbitrary[(IMap[J, A], Option[(A, A)])] ^^ (      map[J, A](_)),
          arbitrary[(A, A)]                       ^^ (coproduct[J, A](_)))
    }
}

object TypeFArbitrary extends TypeFArbitrary
