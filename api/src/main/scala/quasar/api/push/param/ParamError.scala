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

package quasar.api.push.param

import java.lang.String
import scala.{Int, Product, Serializable}

import cats.data.{Ior, NonEmptyList, NonEmptySet}

import skolems.∃

sealed trait ParamError extends Product with Serializable

object ParamError {
  final case class IntOutOfBounds(name: String, i: Int, bounds: Ior[Int, Int]) extends ParamError
  final case class IntOutOfStep(name: String, i: Int, step: IntegerStep) extends ParamError
  final case class ParamMismatch(name: String, expected: ∃[Formal], actual: ∃[Actual]) extends ParamError
  final case class ParamMissing(name: String, expected: ∃[Formal]) extends ParamError
  final case class ExcessiveParams(expected: Int, actual: Int, extra: NonEmptyList[∃[Actual]]) extends ParamError
  final case class ValueNotInEnum(name: String, selector: String, possibilities: NonEmptySet[String]) extends ParamError
}
