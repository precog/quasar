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

package quasar.connector.destination

import quasar.api.Labeled
import quasar.api.push.param._

import scala.{Product, Serializable}

sealed trait Constructor[T] extends Product with Serializable

object Constructor {
  final case class Unary[A, T](
      param1: Labeled[Formal[A]],
      apply: A => T)
      extends Constructor[T]

  final case class Binary[A, B, T](
      param1: Labeled[Formal[A]],
      param2: Labeled[Formal[B]],
      apply: (A, B) => T)
      extends Constructor[T]

  final case class Ternary[A, B, C, T](
      param1: Labeled[Formal[A]],
      param2: Labeled[Formal[B]],
      param3: Labeled[Formal[C]],
      apply: (A, B, C) => T)
      extends Constructor[T]
}
