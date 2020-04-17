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

package quasar.connector

import slamdata.Predef._

import java.time.OffsetDateTime

import cats.evidence._

import spire.math.Real

sealed trait OffsetKey[F[_], A] extends Product with Serializable {
  type Repr
  val value: F[A]
  val reify: A === Repr
}

object OffsetKey {
  final case class RealKey[F[_]](value: F[Real]) extends OffsetKey[F, Real] {
    type Repr = Real
    val reify = Is.refl
  }

  final case class StringKey[F[_]](value: F[String]) extends OffsetKey[F, String] {
    type Repr = String
    val reify = Is.refl
  }

  final case class DateTimeKey[F[_]](value: F[OffsetDateTime]) extends OffsetKey[F, OffsetDateTime] {
    type Repr = OffsetDateTime
    val reify = Is.refl
  }
}
