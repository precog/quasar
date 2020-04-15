/*
 * Copyright 2020 Precog Data
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

import quasar.api.Column
import quasar.contrib.scalaz.MonadError_

import java.time.OffsetDateTime

import cats.Id
import cats.data.Const

import skolems.∃

package object connector {
  type ByteStore[F[_]] = Store[F, String, Array[Byte]]

  type Offset = Column[∃[ActualKey]]

  type MonadResourceErr[F[_]] = MonadError_[F, ResourceError]

  def MonadResourceErr[F[_]](implicit ev: MonadResourceErr[F])
      : MonadResourceErr[F] = ev

  type ActualKey[A] = Key[Id, A]

  object ActualKey {
    def double(k: Double): ActualKey[Double] =
      Key.DoubleKey[Id](k)

    def long(k: Long): ActualKey[Long] =
      Key.LongKey[Id](k)

    def bigDecimal[T](k: BigDecimal): ActualKey[BigDecimal] =
      Key.BigDecimalKey[Id](k)

    def string(k: String): ActualKey[String] =
      Key.StringKey[Id](k)

    def dateTime(k: OffsetDateTime): ActualKey[OffsetDateTime] =
      Key.DateTimeKey[Id](k)
  }

  type TypedKey[T, A] = Key[Const[T, ?], A]

  object TypedKey {
    def double[T](t: T): TypedKey[T, Double] =
      Key.DoubleKey(Const(t))

    def long[T](t: T): TypedKey[T, Long] =
      Key.LongKey(Const(t))

    def bigDecimal[T](t: T): TypedKey[T, BigDecimal] =
      Key.BigDecimalKey(Const(t))

    def string[T](t: T): TypedKey[T, String] =
      Key.StringKey(Const(t))

    def dateTime[T](t: T): TypedKey[T, OffsetDateTime] =
      Key.DateTimeKey(Const(t))
  }
}
