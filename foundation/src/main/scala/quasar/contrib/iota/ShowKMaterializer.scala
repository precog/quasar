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

sealed trait ShowKMaterializer[LL <: TListK] {
  def materialize(offset: Int): Delay[Show, CopK[LL, ?]]
}

object ShowKMaterializer {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def base[F[_]](
    implicit
    F: Delay[Show, F]
  ): ShowKMaterializer[F ::: TNilK] = new ShowKMaterializer[F ::: TNilK] {
    override def materialize(offset: Int): Delay[Show, CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      Delay.fromNT(new (Show ~> λ[a => Show[CopK[F ::: TNilK, a]]]) {
        override def apply[A](sh: Show[A]): Show[CopK[F ::: TNilK, A]] = {
          Show show {
            case I(fa) => F(sh).show(fa)
          }
        }
      })
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
    implicit
    F: Delay[Show, F],
    LL: ShowKMaterializer[LL]
  ): ShowKMaterializer[F ::: LL] = new ShowKMaterializer[F ::: LL] {
    override def materialize(offset: Int): Delay[Show, CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      Delay.fromNT(new (Show ~> λ[a => Show[CopK[F ::: LL, a]]]) {
        override def apply[A](sh: Show[A]): Show[CopK[F ::: LL, A]] = {
          Show show {
            case I(fa) => F(sh).show(fa)
            case other => LL.materialize(offset + 1)(sh).show(other.asInstanceOf[CopK[LL, A]])
          }
        }
      })
    }
  }

}