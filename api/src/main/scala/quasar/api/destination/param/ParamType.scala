/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.api.destination.param

import cats._
import cats.data.{Ior, NonEmptyMap}
import cats.implicits._

import java.lang.String

import scala.{Boolean => SBoolean, Int, Nothing, Option, Product, Serializable, Unit}

sealed trait ParamType[F[_], A] extends Product with Serializable

object ParamType {
  final case class Boolean[F[_]](value: F[Unit])
      extends ParamType[F, SBoolean]

  final case class Integer[F[_]](value: F[Integer.Args])
      extends ParamType[F, Int]

  object Integer {
    type Min = Int
    type Max = Int

    final case class Args(bounds: Option[Min Ior Max], step: Option[IntegerStep])

    object Args {
      implicit val argsEq: Eq[Args] =
        Eq.by(a => (a.bounds, a.step))

      implicit val argsShow: Show[Args] =
        Show.fromToString
    }
  }

  final case class Enum[F[_], A](value: F[NonEmptyMap[String, A]])
      extends ParamType[F, A]

  final case class EnumSelect[F[_]](value: F[Nothing])
      extends ParamType[F, String]
}
