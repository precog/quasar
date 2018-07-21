/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.qscript

import slamdata.Predef._
import quasar._
import quasar.common.PathError
import quasar.common.data.Data
import quasar.contrib.pathy.ADir
import quasar.fp._

import pathy.Path.posixCodec
import scalaz._, Scalaz._

sealed abstract class PlannerError {
  def message: String
}

object PlannerError {
  final case class NonRepresentableData(data: Data) extends PlannerError {
    def message = "The back-end has no representation for the constant: " + data.shows
  }

  final case class PlanPathError(error: PathError) extends PlannerError {
    def message = error.shows
  }

  final case class NoFilesFound(dirs: List[ADir]) extends PlannerError {
    def message = dirs match {
      case Nil => "No paths provided to read from."
      case ds  =>
        "None of these directories contain any files to read from: " ++
          ds.map(posixCodec.printPath).mkString(", ")
      }
  }

  final case class InternalError(msg: String, cause: Option[Exception]) extends PlannerError {
    def message = msg + ~cause.map(ex => s" (caused by: $ex)")
  }

  object InternalError {
    def fromMsg(msg: String): PlannerError = apply(msg, None)
  }

  implicit val PlannerErrorRenderTree: RenderTree[PlannerError] = new RenderTree[PlannerError] {
    def render(v: PlannerError) = Terminal(List("Error"), Some(v.message))
  }

  implicit val plannerErrorShow: Show[PlannerError] =
    Show.show(_.message)
}
