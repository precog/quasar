/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef.{Array, Int => SInt, SuppressWarnings}

import quasar.contrib.iota._

import iotaz.TListK.:::
import iotaz.{CopK, TListK, TNilK}
import matryoshka.{Algebra, Corecursive, Recursive}

sealed trait EncodeEJsonKMaterializer[LL <: TListK] {
  def materialize(offset: SInt): EncodeEJsonK[CopK[LL, ?]]
}

object EncodeEJsonKMaterializer {
  implicit def base[F[_]](implicit F: EncodeEJsonK[F]): EncodeEJsonKMaterializer[F ::: TNilK] =
    new EncodeEJsonKMaterializer[F ::: TNilK] {
      def materialize(offset: SInt): EncodeEJsonK[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)

        new EncodeEJsonK[CopK[F ::: TNilK, ?]] {
          def encodeK[J](
              implicit
              JC: Corecursive.Aux[J, EJson],
              JR: Recursive.Aux[J, EJson])
              : Algebra[CopK[F ::: TNilK, ?], J] = {

            case I(fa) => F.encodeK[J].apply(fa)
          }
        }
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
      implicit
      F: EncodeEJsonK[F],
      LL: EncodeEJsonKMaterializer[LL])
      : EncodeEJsonKMaterializer[F ::: LL] =
    new EncodeEJsonKMaterializer[F ::: LL] {
      def materialize(offset: SInt): EncodeEJsonK[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)

        new EncodeEJsonK[CopK[F ::: LL, ?]] {
          def encodeK[J](
              implicit
              JC: Corecursive.Aux[J, EJson],
              JR: Recursive.Aux[J, EJson])
              : Algebra[CopK[F ::: LL, ?], J] = {

            case I(fa) =>
              F.encodeK[J].apply(fa)

            case other =>
              LL.materialize(offset + 1)
                .encodeK[J].apply(other.asInstanceOf[CopK[LL, J]])
          }
        }
      }
    }
}
