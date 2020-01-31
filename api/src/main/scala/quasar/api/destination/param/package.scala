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

package quasar.api.destination

import cats.Id
import cats.data.{Const, Ior, NonEmptyMap}
import cats.instances.string._

import java.lang.String
import scala.{Boolean, Int, Option}

package object param {
  type Formal[A] = ParamType[Id, A]
  type Actual[A] = ParamType[Const[A, ?], A]

  object Formal {
    val boolean: Formal[Boolean] =
      ParamType.Boolean[Id](())

    def integer(bounds: Option[Int Ior Int], step: Option[IntegerStep]): Formal[Int] =
      ParamType.Integer[Id](ParamType.Integer.Args(bounds, step))

    def enum[A](x: (String, A), xs: (String, A)*): Formal[A] =
      ParamType.Enum[Id, A](NonEmptyMap.of(x, xs: _*))
  }

  object Actual {
    def boolean(b: Boolean): Actual[Boolean] =
      ParamType.Boolean(Const(b))

    def integer(i: Int): Actual[Int] =
      ParamType.Integer(Const(i))

    def enumSelect(s: String): Actual[String] =
      ParamType.EnumSelect(Const(s))
  }
}
