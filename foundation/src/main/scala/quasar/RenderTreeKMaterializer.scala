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

package quasar

import slamdata.Predef._
import iotaz.TListK.:::
import iotaz.{CopK, TListK, TNilK}
import matryoshka.Delay
import scalaz._
import quasar.contrib.iota.mkInject

sealed trait RenderTreeKMaterializer[LL <: TListK] {
  def materialize(offset: Int): Delay[RenderTree, CopK[LL, ?]]
}

object RenderTreeKMaterializer {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def base[F[_]](
    implicit
    F: Delay[RenderTree, F]
  ): RenderTreeKMaterializer[F ::: TNilK] = new RenderTreeKMaterializer[F ::: TNilK] {
    override def materialize(offset: Int): Delay[RenderTree, CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      Delay.fromNT(new (RenderTree ~> λ[a => RenderTree[CopK[F ::: TNilK, a]]]) {
        override def apply[A](rt: RenderTree[A]): RenderTree[CopK[F ::: TNilK, A]] = {
          RenderTree make {
            case I(fa) => F(rt).render(fa)
          }
        }
      })
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
    implicit
    F: Delay[RenderTree, F],
    LL: RenderTreeKMaterializer[LL]
  ): RenderTreeKMaterializer[F ::: LL] = new RenderTreeKMaterializer[F ::: LL] {
    override def materialize(offset: Int): Delay[RenderTree, CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      Delay.fromNT(new (RenderTree ~> λ[a => RenderTree[CopK[F ::: LL, a]]]) {
        override def apply[A](rt: RenderTree[A]): RenderTree[CopK[F ::: LL, A]] = {
          RenderTree make {
            case I(fa) => F(rt).render(fa)
            case other => LL.materialize(offset + 1)(rt).render(other.asInstanceOf[CopK[LL, A]])
          }
        }
      })
    }
  }

}