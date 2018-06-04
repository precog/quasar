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
import scalaz.{Applicative, Traverse}

sealed trait TraverseMaterializer[LL <: TListK] {
  def materialize(offset: Int): Traverse[CopK[LL, ?]]
}

object TraverseMaterializer {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def base[F[_]](
    implicit F: Traverse[F]
  ): TraverseMaterializer[F ::: TNilK] = new TraverseMaterializer[F ::: TNilK] {
    override def materialize(offset: Int): Traverse[CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)

      new Traverse[CopK[F ::: TNilK, ?]] {
        override def traverseImpl[G[_], A, B](cfa: CopK[F ::: TNilK, A])(f: A => G[B])(implicit A: Applicative[G]): G[CopK[F ::: TNilK, B]] = {
          cfa match {
            case I(fa) => A.map(F.traverse(fa)(f))(I(_))
          }
        }
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
    implicit
    F: Traverse[F],
    LL: TraverseMaterializer[LL]
  ): TraverseMaterializer[F ::: LL] = new TraverseMaterializer[F ::: LL] {
    override def materialize(offset: Int): Traverse[CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)

      new Traverse[CopK[F ::: LL, ?]] {
        override def traverseImpl[G[_], A, B](cfa: CopK[F ::: LL, A])(f: A => G[B])(implicit A: Applicative[G]): G[CopK[F ::: LL, B]] = {
          cfa match {
            case I(fa) => A.map(F.traverse(fa)(f))(I(_))
            case other => LL.materialize(offset + 1).traverseImpl(other.asInstanceOf[CopK[LL, A]])(f).asInstanceOf[G[iotaz.CopK[F ::: LL,B]]]
          }
        }
      }
    }
  }

}