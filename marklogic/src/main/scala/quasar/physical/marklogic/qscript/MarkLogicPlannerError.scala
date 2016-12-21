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
import quasar.ejson.EJson
import quasar.fp.{coproductEqual, coproductShow}
import quasar.physical.marklogic.ErrorMessages

import matryoshka._
import matryoshka.data._
import monocle.Prism
import scalaz._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.show._

sealed abstract class MarkLogicPlannerError

object MarkLogicPlannerError {
  final case class InvalidQName(strLit: String) extends MarkLogicPlannerError
  final case class UnrepresentableEJson(ejson: Fix[EJson], msgs: ErrorMessages) extends MarkLogicPlannerError

  val invalidQName = Prism.partial[MarkLogicPlannerError, String] {
    case InvalidQName(s) => s
  } (InvalidQName)

  val unrepresentableEJson = Prism.partial[MarkLogicPlannerError, (Fix[EJson], ErrorMessages)] {
    case UnrepresentableEJson(ejs, msgs) => (ejs, msgs)
  } (UnrepresentableEJson.tupled)

  implicit val equal: Equal[MarkLogicPlannerError] =
    Equal.equalBy(e => (invalidQName.getOption(e), unrepresentableEJson.getOption(e)))

  implicit val show: Show[MarkLogicPlannerError] =
    Show.shows {
      case InvalidQName(s) =>
        s"'$s' is not a valid XML QName."

      case UnrepresentableEJson(ejs, msgs) =>
        s"'${ejs.shows}' does not have an XQuery representation: ${msgs.intercalate(", ")}"
    }
}

