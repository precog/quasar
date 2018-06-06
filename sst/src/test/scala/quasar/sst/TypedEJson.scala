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

package quasar.sst

import quasar.contrib.matryoshka.arbitrary._
import quasar.ejson.{
  DecodeEJson,
  EJsonL,
  EJson,
  EncodeEJson,
  Common,
  Extension,
  CommonEJson,
  ExtEJson,
  EJsonArbitrary,
  Meta,
  Type => EType,
  SizedType => ESizedType,
  Null => ENull
}
import quasar.ejson.implicits._
import quasar.fp._, Helpers._
import quasar.contrib.iota.copkShow
import quasar.pkg.tests._

import matryoshka._
import matryoshka.implicits._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz._, Scalaz._
import iotaz.CopK
import iotaz.TListK.:::

/** EJson that contains `_ejson.type` metadata. */
final case class TypedEJson[T[_[_]]](ejson: T[EJson])

object TypedEJson extends TypedEJsonInstances {
  import quasar.contrib.iota.copkTraverse

  type TEJson[A] = CopK[TypeMetadata ::: EJsonL, A]

  def absorbMetadata[J](implicit J: Birecursive.Aux[J, EJson]): Transform[J, TEJson, EJson] = {
    case TM(TypeMetadata.Type(tag, j))            => ExtEJson(Meta(j, EType(tag)))
    case TM(TypeMetadata.SizedType(tag, size, j)) => ExtEJson(Meta(j, ESizedType(tag, size)))
    case TM(TypeMetadata.Absent(j))               => J.project(j)
    case TM(TypeMetadata.Null())                  => CommonEJson(ENull())
    case CJ(cj)                                   => CommonEJson(cj)
    case EJ(ej)                                   => ExtEJson(ej)
  }

  ////

  private val TM = CopK.Inject[TypeMetadata, TEJson]
  private val CJ = CopK.Inject[Common, TEJson]
  private val EJ = CopK.Inject[Extension, TEJson]
}

sealed abstract class TypedEJsonInstances extends TypedEJsonInstances0 {
  import EJsonArbitrary._
  import quasar.contrib.iota.copkTraverse

  implicit def arbitrary[T[_[_]]: BirecursiveT]: Arbitrary[TypedEJson[T]] =
    corecursiveArbitrary[T[TypedEJson.TEJson], TypedEJson.TEJson] map { v =>
      TypedEJson(v.transCata[T[EJson]](TypedEJson.absorbMetadata[T[EJson]]))
    }

  implicit def decodeEJson[T[_[_]]: BirecursiveT]: DecodeEJson[TypedEJson[T]] =
    DecodeEJson.decodeEJsonC[TypedEJson[T], EJson]

  implicit def encodeEJson[T[_[_]]: BirecursiveT]: EncodeEJson[TypedEJson[T]] =
    EncodeEJson.encodeEJsonR[TypedEJson[T], EJson]

  implicit def order[T[_[_]]: BirecursiveT]: Order[TypedEJson[T]] =
    Order[T[EJson]].contramap(_.ejson)

  implicit def show[T[_[_]]: ShowT]: Show[TypedEJson[T]] =
    Show[T[EJson]].contramap(_.ejson)
}

sealed abstract class TypedEJsonInstances0 {
  implicit def corecursive[T[_[_]]: CorecursiveT]: Corecursive.Aux[TypedEJson[T], EJson] =
    new Corecursive[TypedEJson[T]] {
      type Base[B] = EJson[B]

      def embed(bt: EJson[TypedEJson[T]])(implicit BF: Functor[EJson]) =
        TypedEJson(bt.map(_.ejson).embed)
    }

  implicit def recursive[T[_[_]]: RecursiveT]: Recursive.Aux[TypedEJson[T], EJson] =
    new Recursive[TypedEJson[T]] {
      type Base[B] = EJson[B]

      def project(bt: TypedEJson[T])(implicit BF: Functor[EJson]) =
        bt.ejson.project map (TypedEJson(_))
    }
}
