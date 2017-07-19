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

import _root_.matryoshka.Delay
import org.scalacheck.{Arbitrary, Gen}
import scalaz.Coproduct
import scalaz.syntax.either._

object arbitrary extends CorecursiveArbitrary {
  implicit def delayArbitrary[F[_], A](
    implicit
    A: Arbitrary[A],
    F: Delay[Arbitrary, F]
  ): Arbitrary[F[A]] =
    F(A)

  implicit def coproductDelayArbitrary[F[_], G[_]](
    implicit
    FA: Delay[Arbitrary, F],
    FW: UnionWidth[F],
    GA: Delay[Arbitrary, G],
    GW: UnionWidth[G]
  ): Delay[Arbitrary, Coproduct[F, G, ?]] =
    new Delay[Arbitrary, Coproduct[F, G, ?]] {
      def apply[A](arb: Arbitrary[A]): Arbitrary[Coproduct[F, G, A]] =
        Arbitrary(Gen.frequency(
          (FW.width, FA(arb).arbitrary map (_.left)),
          (GW.width, GA(arb).arbitrary map (_.right))
        ) map (Coproduct(_)))
    }
}
