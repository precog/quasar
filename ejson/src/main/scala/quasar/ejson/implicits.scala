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

package quasar.ejson

import slamdata.Predef._
import quasar.contrib.matryoshka.{project => projectg, _}
import quasar.contrib.iota.{copkTraverse, copkOrder}

import matryoshka._
import matryoshka.implicits._
import scalaz.{==>>, Equal, Order}
import scalaz.std.list._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._

object implicits {
  // NB: This is defined here as we need to elide metadata from args before
  //     comparing them.
  implicit def ejsonOrder[T](
    implicit
    TC: Corecursive.Aux[T, EJson],
    TR: Recursive.Aux[T, EJson]
  ): Order[T] =
    Order.order { (x, y) =>
      implicit val ordExt = Extension.structuralOrder
      OrderR.order[T, EJson](
        x.transCata[T](EJson.elideMetadata[T]),
        y.transCata[T](EJson.elideMetadata[T]))
    }

  implicit final class EJsonOps[J](val j: J) extends scala.AnyVal {
    def array(implicit JR: Recursive.Aux[J, EJson]): Option[List[J]] =
      projectg[J, EJson].composePrism(optics.arr).headOption(j)

    def assoc(implicit JR: Recursive.Aux[J, EJson]): Option[List[(J, J)]] =
      projectg[J, EJson].composePrism(optics.map).headOption(j)

    def decodeAs[A](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson], A: DecodeEJson[A]): Decoded[A] =
      A.decode[J](j)

    def decodeKeyS(k: String)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[J] =
      Decoded.attempt(j, keyS(k) \/> s"Map[$k]")

    def decodedKeyS[A: DecodeEJson](k: String)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[A] =
      decodeKeyS(k) flatMap (_.decodeAs[A])

    def key(k: J)(implicit JR: Recursive.Aux[J, EJson], J: Equal[J]): Option[J] =
      assoc flatMap (_ findLeft (_._1 ≟ k)) map (_._2)

    def keyS(k: String)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Option[J] =
      key(EJson.str(k))

    def map(implicit JR: Recursive.Aux[J, EJson], J: Order[J]): Option[J ==>> J] =
      projectg[J, EJson].composePrism(optics.imap).headOption(j)
  }

  implicit final class EncodeEJsonOps[A](val self: A) extends scala.AnyVal {
    def asEJson[J](
      implicit
      A : EncodeEJson[A],
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson]
    ): J =
      A.encode[J](self)
  }

  implicit final class EncodeEJsonKOps[F[_], J](val self: F[J]) extends scala.AnyVal {
    def asEJsonK(
      implicit
      F : EncodeEJsonK[F],
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson]
    ): J =
      F.encodeK[J].apply(self)
  }
}
