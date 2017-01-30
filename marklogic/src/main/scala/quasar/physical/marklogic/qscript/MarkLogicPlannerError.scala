/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.qscript

import quasar.Predef._

import monocle.Prism
import scalaz._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._

sealed abstract class MarkLogicPlannerError

object MarkLogicPlannerError {
  final case class InvalidQName(strLit: String) extends MarkLogicPlannerError
  final case class Unimplemented(function: String) extends MarkLogicPlannerError
  final case class Unreachable(desc: String) extends MarkLogicPlannerError

  val invalidQName = Prism.partial[MarkLogicPlannerError, String] {
    case InvalidQName(s) => s
  } (InvalidQName)

  val unimplemented = Prism.partial[MarkLogicPlannerError, String] {
    case Unimplemented(f) => f
  } (Unimplemented)

  val unreachable = Prism.partial[MarkLogicPlannerError, String] {
    case Unreachable(d) => d
  } (Unreachable)

  implicit val equal: Equal[MarkLogicPlannerError] =
    Equal.equalBy(e => (invalidQName.getOption(e), unimplemented.getOption(e), unreachable.getOption(e)))

  implicit val show: Show[MarkLogicPlannerError] =
    Show.shows {
      case InvalidQName(s) =>
        s"'$s' is not a valid XML QName."

      case Unimplemented(f) =>
        s"The function $f is not implemented."

      case Unreachable(d) =>
        s"Should not have been reached: $d."
    }
}
