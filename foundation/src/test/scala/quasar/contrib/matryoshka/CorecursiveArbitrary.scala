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

package quasar.contrib.matryoshka

import _root_.matryoshka.{Corecursive, Delay}
import _root_.matryoshka.data._
import _root_.matryoshka.implicits._
import org.scalacheck.{Arbitrary, Gen}
import scalaz.Functor

// TODO{matryoshka}: This exists as matryoshka depends on a shapshot version of
//                   scalacheck at the moment.
trait CorecursiveArbitrary {
  def corecursiveArbitrary[T, F[_]: Functor]
    (implicit T: Corecursive.Aux[T, F], fArb: Delay[Arbitrary, F])
      : Arbitrary[T] =
    Arbitrary(Gen.sized(size =>
      fArb(Arbitrary(
        if (size <= 0)
          Gen.fail[T]
        else
          Gen.resize(size - 1, corecursiveArbitrary[T, F].arbitrary))).arbitrary map (_.embed)))

  implicit def fixArbitrary[F[_]: Functor](implicit fArb: Delay[Arbitrary, F]): Arbitrary[Fix[F]] =
    corecursiveArbitrary[Fix[F], F]

  implicit def muArbitrary[F[_]: Functor](implicit fArb: Delay[Arbitrary, F]): Arbitrary[Mu[F]] =
    corecursiveArbitrary[Mu[F], F]

  implicit def nuArbitrary[F[_]: Functor](implicit fArb: Delay[Arbitrary, F]): Arbitrary[Nu[F]] =
    corecursiveArbitrary[Nu[F], F]
}

object CorecursiveArbitrary extends CorecursiveArbitrary
