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

import slamdata.Predef.{Int => SInt, Char => SChar, _}
import quasar.RenderedTree
import quasar.contrib.argonaut._
import quasar.ejson.implicits._
import quasar.contrib.iota.copkTraverse

import argonaut.{DecodeJson, Json => AJson}
import matryoshka._
import matryoshka.implicits._
import scalaz._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.equal._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.either._
import scalaz.syntax.std.option._
import simulacrum.typeclass

/** Typeclass for types that can be decoded from EJson. */
@typeclass
trait DecodeEJson[A] {
  def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[A]

  def map[B](f: A => B): DecodeEJson[B] =
    mapDecoded(_ map f)

  def mapDecoded[B](f: Decoded[A] => Decoded[B]): DecodeEJson[B] =
    flatMapDecoded(f andThen DecodeEJson.always)

  def flatMap[B](f: A => DecodeEJson[B]): DecodeEJson[B] =
    flatMapDecoded(_.fold(DecodeEJson.failed, f))

  def flatMapDecoded[B](f: Decoded[A] => DecodeEJson[B]): DecodeEJson[B] = {
    val orig = this
    new DecodeEJson[B] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[B] =
        f(orig.decode(j)).decode(j)
    }
  }

  def reinterpret[B](msg: String, f: A => Option[B]): DecodeEJson[B] = {
    val orig = this
    new DecodeEJson[B] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[B] =
        orig.decode(j).setMessage(msg).flatMap(a => Decoded.attempt(j, f(a) \/> msg))
    }
  }
}

object DecodeEJson extends DecodeEJsonInstances {
  def always[A](r: Decoded[A]): DecodeEJson[A] =
    new DecodeEJson[A] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[A] =
        r
    }

  def constant[A](a: A): DecodeEJson[A] =
    always(Decoded.success(a))

  def decodeEJsonC[T, F[_]: Traverse](implicit T: Corecursive.Aux[T, F], F: DecodeEJsonK[F]): DecodeEJson[T] =
    new DecodeEJson[T] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[T] =
        j.anaM[T](F.decodeK[J])
    }

  def failed[A](input: RenderedTree, msg: String): DecodeEJson[A] =
    always(Decoded.failure(input, msg))

  object failedOn {
    def apply[A] = new PartiallyApplied[A]
    final class PartiallyApplied[A] {
      def apply[J](input: J, msg: String)(implicit J: Recursive.Aux[J, EJson]): DecodeEJson[A] =
        always(Decoded.failureFor[A](input, msg))
    }
  }
}

sealed abstract class DecodeEJsonInstances extends DecodeEJsonInstances0 {
  implicit val monad: Monad[DecodeEJson] =
    new Monad[DecodeEJson] {
      override def map[A, B](fa: DecodeEJson[A])(f: A => B) =
        fa map f

      def bind[A, B](fa: DecodeEJson[A])(f: A => DecodeEJson[B]) =
        fa flatMap f

      def point[A](a: => A) =
        DecodeEJson.constant(a)
    }

  implicit val bigIntDecodeEJson: DecodeEJson[BigInt] =
    new DecodeEJson[BigInt] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[BigInt] =
        Decoded.attempt(j, Fixed[J].int.getOption(j) \/> "BigInt")
    }

  implicit val intDecodeEJson: DecodeEJson[SInt] =
    bigIntDecodeEJson.reinterpret("Int", bi => bi.isValidInt option bi.intValue)

  implicit val longDecodeEJson: DecodeEJson[Long] =
    bigIntDecodeEJson.reinterpret("Long", bi => bi.isValidLong option bi.longValue)

  implicit val shortDecodeEJson: DecodeEJson[Short] =
    bigIntDecodeEJson.reinterpret("Short", bi => bi.isValidShort option bi.shortValue)

  implicit val charDecodeEJson: DecodeEJson[SChar] =
    new DecodeEJson[SChar] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[SChar] = {
        val fromChar = Fixed[J].char.getOption(j)
        def fromStr = Fixed[J].str.getOption(j).filter(_.length ≟ 1).map(_(0))
        Decoded.attempt(j, (fromChar orElse fromStr) \/> "Char")
      }
    }

  implicit def optionDecodeEJson[A](implicit A: DecodeEJson[A]): DecodeEJson[Option[A]] =
    new DecodeEJson[Option[A]] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[Option[A]] =
        A.decode[J](j).map(some(_))
          .orElse(Decoded.attempt(j, Fixed[J].nul.getOption(j).as(none[A]) \/> "Option[A]"))
    }

  implicit def listDecodeEJson[A](implicit A: DecodeEJson[A]): DecodeEJson[List[A]] =
    new DecodeEJson[List[A]] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[List[A]] =
        Decoded.attempt(j, j.array \/> "[A]List[A]") flatMap (_.traverse(_.decodeAs[A]))
    }

  implicit def decodeEJsonT[T[_[_]]: CorecursiveT, F[_]: Traverse: DecodeEJsonK]: DecodeEJson[T[F]] =
    DecodeEJson.decodeEJsonC[T[F], F]
}

sealed abstract class DecodeEJsonInstances0 {
  implicit def decodeJsonEJson[A: DecodeJson]: DecodeEJson[A] =
    new DecodeEJson[A] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Decoded[A] = {
        val ojson = j.transAnaM[Option, AJson, Json](EJson.toJson(Fixed[J].str.getOption(_)))
        Decoded.attempt(j, ojson \/> "JSON") flatMap { js =>
          Decoded.attempt(j, DecodeJson.of[A].decodeJson(js).toEither.disjunction leftMap (_._1))
        }
      }
    }
}
