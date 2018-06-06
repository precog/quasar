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

import slamdata.Predef.{Int => SInt, Char => SChar, Byte => SByte, _}
import quasar.contrib.argonaut._
import quasar.contrib.iota.copkTraverse

import argonaut.EncodeJson
import matryoshka._
import matryoshka.implicits._
import scalaz._
import simulacrum.typeclass

/** Typeclass for types that can be encoded as EJson. */
@typeclass
trait EncodeEJson[A] {
  def encode[J](a: A)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J

  def contramap[B](f: B => A): EncodeEJson[B] = {
    val orig = this
    new EncodeEJson[B] {
      def encode[J](b: B)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J =
        orig.encode[J](f(b))
    }
  }
}

object EncodeEJson extends EncodeEJsonInstances {
  def encodeEJsonR[T, F[_]: Functor](
    implicit T: Recursive.Aux[T, F], F: EncodeEJsonK[F]
  ): EncodeEJson[T] =
    new EncodeEJson[T] {
      def encode[J](t: T)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J =
        t.cata[J](F.encodeK[J])
    }
}

sealed abstract class EncodeEJsonInstances extends EncodeEJsonInstances0 {
  implicit val bigIntEncodeEJson: EncodeEJson[BigInt] =
    new EncodeEJson[BigInt] {
      def encode[J](i: BigInt)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J =
        Fixed[J].int(i)
    }

  implicit val intEncodeEJson: EncodeEJson[SInt] =
    bigIntEncodeEJson.contramap(BigInt(_))

  implicit val longEncodeEJson: EncodeEJson[Long] =
    bigIntEncodeEJson.contramap(BigInt(_))

  implicit val shortEncodeEJson: EncodeEJson[Short] =
    intEncodeEJson.contramap(_.toInt)

  implicit val byteEncodeEJson: EncodeEJson[SByte] =
    new EncodeEJson[SByte] {
      def encode[J](b: SByte)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J =
        Fixed[J].byte(b)
    }

  implicit val charEncodeEJson: EncodeEJson[SChar] =
    new EncodeEJson[SChar] {
      def encode[J](c: SChar)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J =
        Fixed[J].char(c)
    }

  implicit def optionEncodeEJson[A](implicit A: EncodeEJson[A]): EncodeEJson[Option[A]] =
    new EncodeEJson[Option[A]] {
      def encode[J](oa: Option[A])(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J =
        oa.fold(Fixed[J].nul())(A.encode[J](_))
    }

  implicit def encodeJsonT[T[_[_]]: RecursiveT, F[_]: Functor: EncodeEJsonK]: EncodeEJson[T[F]] =
    EncodeEJson.encodeEJsonR[T[F], F]
}

sealed abstract class EncodeEJsonInstances0 {
  implicit def encodeJsonEJson[A: EncodeJson]: EncodeEJson[A] =
    new EncodeEJson[A] {
      def encode[J](a: A)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J =
        EncodeJson.of[A].encode(a).transCata[J](EJson.fromJson(Fixed[J].str(_)))
    }
}
