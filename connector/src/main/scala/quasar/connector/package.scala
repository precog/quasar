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

import skolems.Exists

import spire.math.Real

package object connector {
  type ByteStore[F[_]] = Store[F, String, Array[Byte]]

  type MonadResourceErr[F[_]] = MonadError_[F, ResourceError]

  def MonadResourceErr[F[_]](implicit ev: MonadResourceErr[F])
      : MonadResourceErr[F] = ev

  type OffsetValue = Exists[OffsetKey[Id, ?]]
  type Offset = Column[OffsetValue]

  type ActualKey[A] = OffsetKey[Id, A]

  object ActualKey {
    def real[T](k: Real): ActualKey[Real] =
      OffsetKey.RealKey[Id](k)

    def string(k: String): ActualKey[String] =
      OffsetKey.StringKey[Id](k)

    def dateTime(k: OffsetDateTime): ActualKey[OffsetDateTime] =
      OffsetKey.DateTimeKey[Id](k)
  }

  type TypedKey[T, A] = OffsetKey[Const[T, ?], A]

  object TypedKey {
    def real[T](t: T): TypedKey[T, Real] =
      OffsetKey.RealKey(Const(t))

    def string[T](t: T): TypedKey[T, String] =
      OffsetKey.StringKey(Const(t))

    def dateTime[T](t: T): TypedKey[T, OffsetDateTime] =
      OffsetKey.DateTimeKey(Const(t))
  }
}
