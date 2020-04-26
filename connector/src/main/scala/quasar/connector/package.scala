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

import slamdata.Predef.{Eq => _, _}

import quasar.api.Column
import quasar.contrib.scalaz.MonadError_

import java.time.OffsetDateTime

import cats.{Eq, Id, Show}
import cats.data.Const
import cats.implicits._

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

    implicit def actualKeyEq[A: Eq] = new Eq[ActualKey[A]] {
      def eqv(a1: ActualKey[A], a2: ActualKey[A]): Boolean =
        (a1, a2) match {
          case (OffsetKey.RealKey(k1), OffsetKey.RealKey(k2)) => k1 === k2
          case (OffsetKey.StringKey(k1), OffsetKey.StringKey(k2)) => k1 === k2
          case (OffsetKey.DateTimeKey(k1), OffsetKey.DateTimeKey(k2)) => k1 === k2
          case (_, _) => false
        }
    }

    implicit def actualKeyShow[A: Show]: Show[ActualKey[A]] =
      Show.fromToString[ActualKey[A]]
  }

  type FormalKey[T, A] = OffsetKey[Const[T, ?], A]

  object FormalKey {
    def real[T](t: T): FormalKey[T, Real] =
      OffsetKey.RealKey(Const(t))

    def string[T](t: T): FormalKey[T, String] =
      OffsetKey.StringKey(Const(t))

    def dateTime[T](t: T): FormalKey[T, OffsetDateTime] =
      OffsetKey.DateTimeKey(Const(t))
  }
}
