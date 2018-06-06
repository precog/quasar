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

package quasar.fp

import slamdata.Predef._

import quasar.contrib.matryoshka.UnionWidth

import matryoshka.Delay
import org.scalacheck._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::
import quasar.contrib.iota.mkInject

sealed trait ArbitraryKMaterializer[LL <: TListK] {
  def materialize(offset: Int): Delay[Arbitrary, CopK[LL, ?]]
}

object ArbitraryKMaterializer {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def base[F[_]](
    implicit
    FA: Delay[Arbitrary, F]
  ): ArbitraryKMaterializer[F ::: TNilK] = new ArbitraryKMaterializer[F ::: TNilK] {
    override def materialize(offset: Int): Delay[Arbitrary, CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      new Delay[Arbitrary, CopK[F ::: TNilK, ?]] {
        override def apply[A](arb: Arbitrary[A]): Arbitrary[CopK[F ::: TNilK, A]] = {
          Arbitrary(Gen.frequency(
            (1, FA(arb).arbitrary.map(I(_)))
          ))
        }
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
    implicit
    FA: Delay[Arbitrary, F],
    LW: UnionWidth[CopK[LL, ?]],
    LL: ArbitraryKMaterializer[LL]
  ): ArbitraryKMaterializer[F ::: LL] = new ArbitraryKMaterializer[F ::: LL] {
    override def materialize(offset: Int): Delay[Arbitrary, CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      new Delay[Arbitrary, CopK[F ::: LL, ?]] {
        override def apply[A](arb: Arbitrary[A]): Arbitrary[CopK[F ::: LL, A]] = {
          Arbitrary(Gen.frequency(
            (1, FA(arb).arbitrary.map(I(_))),
            (LW.width, LL.materialize(offset + 1)(arb).arbitrary.asInstanceOf[Gen[CopK[F ::: LL, A]]])
          ))
        }
      }
    }
  }
}
