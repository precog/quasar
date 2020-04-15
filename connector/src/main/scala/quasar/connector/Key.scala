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

sealed trait Key[F[_], A] extends Product with Serializable {
  type Repr
  val value: F[A]
  val reify: A === Repr
}

object Key {
  final case class DoubleKey[F[_]](value: F[Double]) extends Key[F, Double] {
    type Repr = Double
    val reify = Is.refl
  }

  final case class LongKey[F[_]](value: F[Long]) extends Key[F, Long] {
    type Repr = Long
    val reify = Is.refl
  }

  final case class BigDecimalKey[F[_]](value: F[BigDecimal]) extends Key[F, BigDecimal] {
    type Repr = BigDecimal
    val reify = Is.refl
  }

  final case class StringKey[F[_]](value: F[String]) extends Key[F, String] {
    type Repr = String
    val reify = Is.refl
  }

  final case class DateTimeKey[F[_]](value: F[OffsetDateTime]) extends Key[F, OffsetDateTime] {
    type Repr = OffsetDateTime
    val reify = Is.refl
  }
}
