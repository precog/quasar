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

package quasar.ejson

import quasar.contrib.matryoshka._

import matryoshka._
import matryoshka.implicits._
import scalaz.Order

object implicits {
  // NB: This is defined here as we need to elide metadata from args before
  //     comparing them.
  implicit def ejsonOrder[T](
    implicit
    TC: Corecursive.Aux[T, EJson],
    TR: Recursive.Aux[T, EJson]
  ): Order[T] =
    Order.order { (x, y) =>
      implicit val ordExt = Extension.order
      OrderR.order[T, EJson](
        x.transCata[T](EJson.elideMetadata[T]),
        y.transCata[T](EJson.elideMetadata[T]))
    }

  implicit final class EncodeEJsonOps[A](val self: A) extends scala.AnyVal {
    def asEJson[J](implicit A: EncodeEJson[A], J: Corecursive.Aux[J, EJson]): J =
      A.encode[J](self)
  }
}
