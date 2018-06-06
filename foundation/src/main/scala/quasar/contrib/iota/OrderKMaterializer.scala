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
import iotaz.{ CopK, TNilK, TListK }
import matryoshka.Delay
import scalaz._

sealed trait OrderKMaterializer[LL <: TListK] {
  def materialize(offset: Int): Delay[Order, CopK[LL, ?]]
}

object OrderKMaterializer {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def base[F[_]](
    implicit
    F: Delay[Order, F]
  ): OrderKMaterializer[F ::: TNilK] = new OrderKMaterializer[F ::: TNilK] {
    override def materialize(offset: Int): Delay[Order, CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      Delay.fromNT(new (Order ~> λ[a => Order[CopK[F ::: TNilK, a]]]) {
        override def apply[A](ord: Order[A]): Order[CopK[F ::: TNilK, A]] = {
          Order.order[CopK[F ::: TNilK, A]] {
            case (I(left), I(right)) => F(ord).order(left, right)
            case (I(left),   right)  => Ordering.LT
            case (  left,  I(right)) => Ordering.GT
          }
        }
      })
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
    implicit
    F: Delay[Order, F],
    LL: OrderKMaterializer[LL]
  ): OrderKMaterializer[F ::: LL] = new OrderKMaterializer[F ::: LL] {
    override def materialize(offset: Int): Delay[Order, CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      Delay.fromNT(new (Order ~> λ[a => Order[CopK[F ::: LL, a]]]) {
        override def apply[A](ord: Order[A]): Order[CopK[F ::: LL, A]] = {
          Order.order[CopK[F ::: LL, A]] {
            case (I(left), I(right)) => F(ord).order(left, right)
            case (I(left),   right)  => Ordering.LT
            case (  left,  I(right)) => Ordering.GT
            case (left, right) => LL.materialize(offset + 1)(ord).order(left.asInstanceOf[CopK[LL, A]], right.asInstanceOf[CopK[LL, A]])
          }
        }
      })
    }
  }

}