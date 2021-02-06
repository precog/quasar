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

package quasar.api.push

import slamdata.Predef.{Eq => _, _}

import java.time.OffsetDateTime

import cats.{Eq, Id, Show}
import cats.data.Const
import cats.evidence._
import cats.implicits._

import spire.math.Real

sealed trait OffsetKey[F[_], A] extends Product with Serializable {
  type Repr
  val value: F[A]
  val reify: A === Repr
}
sealed trait InternalKey[F[_], A] extends OffsetKey[F, A]

object InternalKey {
  import OffsetKey._
  type Actual[A] = InternalKey[Id, A]
  type Formal[T, A] = InternalKey[Const[T, ?], A]

  object Actual {
    def real(k: Real): Actual[Real] =
      RealKey[Id](k)

    def string(k: String): Actual[String] =
      StringKey[Id](k)

    def dateTime(k: OffsetDateTime): Actual[OffsetDateTime] =
      DateTimeKey[Id](k)
  }

  object Formal {
    def real[T](t: T): Formal[T, Real] =
      RealKey(Const[T, Real](t))

    def string[T](t: T): Formal[T, String] =
      StringKey(Const[T, String](t))

    def dateTime[T](t: T): Formal[T, OffsetDateTime] =
      DateTimeKey(Const[T, OffsetDateTime](t))
  }

  def fromOffset[F[_], A](inp: OffsetKey[F, A]): Option[InternalKey[F, A]] = inp match {
    case i: InternalKey[F, A] => Some(i)
    case _ => None
  }
}
object OffsetKey {
  type Actual[A] = OffsetKey[Id, A]
  type Formal[T, A] = OffsetKey[Const[T, ?], A]

  final case class RealKey[F[_]](value: F[Real]) extends InternalKey[F, Real] {
    type Repr = Real
    val reify = Is.refl
  }

  final case class StringKey[F[_]](value: F[String]) extends InternalKey[F, String] {
    type Repr = String
    val reify = Is.refl
  }

  final case class DateTimeKey[F[_]](value: F[OffsetDateTime]) extends InternalKey[F, OffsetDateTime] {
    type Repr = OffsetDateTime
    val reify = Is.refl
  }

  final case class ExternalKey[F[_]](value: F[ExternalOffsetKey]) extends OffsetKey[F, ExternalOffsetKey] {
    type Repr = ExternalOffsetKey
    val reify = Is.refl
  }

  object Actual {
    def real(k: Real): Actual[Real] =
      RealKey[Id](k)

    def string(k: String): Actual[String] =
      StringKey[Id](k)

    def dateTime(k: OffsetDateTime): Actual[OffsetDateTime] =
      DateTimeKey[Id](k)

    def external(k: ExternalOffsetKey): Actual[ExternalOffsetKey] =
      ExternalKey[Id](k)
  }

  object Formal {
    def real[T](t: T): Formal[T, Real] =
      RealKey(Const[T, Real](t))

    def string[T](t: T): Formal[T, String] =
      StringKey(Const[T, String](t))

    def dateTime[T](t: T): Formal[T, OffsetDateTime] =
      DateTimeKey(Const[T, OffsetDateTime](t))

    def external[T](t: T): Formal[T, ExternalOffsetKey] =
      ExternalKey(Const[T, ExternalOffsetKey](t))
  }

  implicit def offsetKeyActualShow[A]: Show[Actual[A]] =
    Show show {
      case RealKey(k) => s"RealKey($k)"
      case StringKey(k) => s"StringKey($k)"
      case DateTimeKey(k) => s"DateTimeKey($k)"
      case ExternalKey(k) => s"ExternalKey(${k.show})"
    }

  implicit def offsetKeyFormalShow[T: Show, A]: Show[Formal[T, A]] =
    Show show {
      case k: RealKey[Const[T, ?]] => s"RealKey(${k.value.getConst.show})"
      case k: StringKey[Const[T, ?]] => s"StringKey(${k.value.getConst.show})"
      case k: DateTimeKey[Const[T, ?]] => s"DateTimeKey(${k.value.getConst.show})"
      case k: ExternalKey[Const[T, ?]] => s"ExternalKey(${k.value.getConst.show})"
    }

  implicit def offsetKeyActualEq[A]: Eq[Actual[A]] = {
    implicit val realEq: Eq[Real] = Eq.fromUniversalEquals
    implicit val offsetDateTimeEq: Eq[OffsetDateTime] = Eq.fromUniversalEquals

    Eq.by[Actual[A], (Option[Real], Option[String], Option[OffsetDateTime], Option[ExternalOffsetKey])] {
      case k: RealKey[Id] => (Some(k.value), None, None, None)
      case k: StringKey[Id] => (None, Some(k.value), None, None)
      case k: DateTimeKey[Id] => (None, None, Some(k.value), None)
      case k: ExternalKey[Id] => (None, None, None, Some(k.value))
    }
  }

  implicit def offsetKeyFormalEq[T: Eq, A]: Eq[Formal[T, A]] =
    Eq.by[Formal[T, A], (Int, T)] {
      case k: RealKey[Const[T, ?]] => (0, k.value.getConst)
      case k: StringKey[Const[T, ?]] => (1, k.value.getConst)
      case k: DateTimeKey[Const[T, ?]] => (2, k.value.getConst)
      case k: ExternalKey[Const[T, ?]] => (3, k.value.getConst)
    }
}
