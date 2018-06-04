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

package quasar.contrib.iota

import slamdata.Predef._
import iotaz.TListK.:::
import iotaz.{CopK, TListK, TNilK}
import scalaz.Functor

sealed trait FunctorMaterializer[LL <: TListK] {
  def materialize(offset: Int): Functor[CopK[LL, ?]]
}

object FunctorMaterializer {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def base[F[_]](
    implicit
    F: Functor[F]
  ): FunctorMaterializer[F ::: TNilK] = new FunctorMaterializer[F ::: TNilK] {
    override def materialize(offset: Int): Functor[CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      new Functor[CopK[F ::: TNilK, ?]] {
        override def map[A, B](cfa: CopK[F ::: TNilK, A])(f: A => B): CopK[F ::: TNilK, B] = {
          cfa match {
            case I(fa) => I(F.map(fa)(f))
          }
        }
      }
    }
  }


  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
    implicit
    F: Functor[F],
    LL: FunctorMaterializer[LL]
  ): FunctorMaterializer[F ::: LL] = new FunctorMaterializer[F ::: LL] {
    override def materialize(offset: Int): Functor[CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      new Functor[CopK[F ::: LL, ?]] {
        override def map[A, B](cfa: CopK[F ::: LL, A])(f: A => B): CopK[F ::: LL, B] = {
          cfa match {
            case I(fa) => I(F.map(fa)(f))
            case other => LL.materialize(offset + 1).map(other.asInstanceOf[CopK[LL, A]])(f).asInstanceOf[CopK[F ::: LL, B]]
          }
        }
      }
    }
  }

}