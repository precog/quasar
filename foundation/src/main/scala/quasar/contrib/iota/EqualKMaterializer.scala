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
import matryoshka.Delay
import scalaz._

sealed trait EqualKMaterializer[LL <: TListK] {
  def materialize(offset: Int): Delay[Equal, CopK[LL, ?]]
}

object EqualKMaterializer {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def base[F[_]](
    implicit
    F: Delay[Equal, F]
  ): EqualKMaterializer[F ::: TNilK] = new EqualKMaterializer[F ::: TNilK] {
    override def materialize(offset: Int): Delay[Equal, CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      Delay.fromNT(new (Equal ~> λ[a => Equal[CopK[F ::: TNilK, a]]]) {
        override def apply[A](eq: Equal[A]): Equal[CopK[F ::: TNilK, A]] = {
          Equal equal {
            case (I(left), I(right)) => F(eq).equal(left, right)
            case _ => false
          }
        }
      })
    }
  }


  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
    implicit
    F: Delay[Equal, F],
    LL: EqualKMaterializer[LL]
  ): EqualKMaterializer[F ::: LL] = new EqualKMaterializer[F ::: LL] {
    override def materialize(offset: Int): Delay[Equal, CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      Delay.fromNT(new (Equal ~> λ[a => Equal[CopK[F ::: LL, a]]]) {
        override def apply[A](eq: Equal[A]): Equal[CopK[F ::: LL, A]] = {
          Equal equal {
            case (I(left), I(right)) => F(eq).equal(left, right)
            case (left, right) => LL.materialize(offset + 1)(eq).equal(left.asInstanceOf[CopK[LL, A]], right.asInstanceOf[CopK[LL, A]])
          }
        }
      })
    }
  }

}