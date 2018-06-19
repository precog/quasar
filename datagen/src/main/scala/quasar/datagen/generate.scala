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

package quasar.datagen

import slamdata.Predef.Option
import quasar.ejson.EJson
import quasar.sst.{PopulationSST, SST}

import cats.effect.Sync
import fs2.Stream
import matryoshka.{Corecursive, Recursive}
import scalaz.{\/, Equal, Order}
import scalaz.Scalaz._
import spire.algebra.{Field, IsReal, NRoot}
import spire.math.ConvertableFrom
import spire.random.{Gaussian, Generator}

object generate {
  object ejson {
    def apply[F[_]] = new PartiallyApplied[F]
    final class PartiallyApplied[F[_]] {
      def apply[J: Order, A: ConvertableFrom: Equal: Field: Gaussian: IsReal: NRoot](
          maxCollLen: A,
          sst: PopulationSST[J, A] \/ SST[J, A])(
          implicit
          F: Sync[F],
          JC: Corecursive.Aux[J, EJson],
          JR: Recursive.Aux[J, EJson])
          : Option[Stream[F, J]] =
        sst
          .fold(
            dist.population[J, A](maxCollLen, _),
            dist.sample[J, A](maxCollLen, _))
          .map(d => Stream.repeatEval(F.delay(d(Generator.rng))))
    }
  }
}
