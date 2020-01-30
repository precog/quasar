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

package quasar

import slamdata.Predef._

import scalaz.std.string._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import shapeless._

sealed abstract class ArgumentError extends Product with Serializable {
  def message: String
}

object ArgumentError {
  final case class InvalidArgumentError(message: String)
      extends ArgumentError

  final case class TemporalFormatError[N <: Nat](func: GenericFunc[N], str: String, hint: Option[String])
      extends ArgumentError {
    def message = s"Unable to parse '$str' as ${func.shows}." + ~hint.map(" (" + _ + ")")
  }

  def temporalFormatError[N <: Nat](func: GenericFunc[N], str: String, hint: Option[String]): ArgumentError =
    TemporalFormatError(func, str, hint)
}
