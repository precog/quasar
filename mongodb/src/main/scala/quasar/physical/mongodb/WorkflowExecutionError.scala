/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.RenderTree
import quasar.fp._
import quasar.fp.ski._
import quasar.physical.mongodb.workflowtask._

import monocle.Prism
import scalaz._
import scalaz.syntax.show._

/** Error conditions possible during `Workflow` execution. */
sealed abstract class WorkflowExecutionError

object WorkflowExecutionError {
  final case class InvalidTask private (task: WorkflowTask, reason: String)
      extends WorkflowExecutionError

  final case class InsertFailed private (bson: Bson, reason: String)
      extends WorkflowExecutionError

  final case object NoDatabase extends WorkflowExecutionError

  val invalidTask = Prism.partial[WorkflowExecutionError, (WorkflowTask, String)] {
    case InvalidTask(t, r) => (t, r)
  } (InvalidTask.tupled)

  val insertFailed = Prism.partial[WorkflowExecutionError, (Bson, String)] {
    case InsertFailed(b, r) => (b, r)
  } (InsertFailed.tupled)

  val noDatabase = Prism.partial[WorkflowExecutionError, Unit] {
    case NoDatabase => ()
  } (κ(NoDatabase))

  implicit val workflowExecutionErrorShow: Show[WorkflowExecutionError] =
    Show.shows { err =>
      val msg = err match {
        case InvalidTask(t, r) =>
          s"Invalid task, $r\n\n" + RenderTree[WorkflowTask].render(t).shows
        case InsertFailed(b, r) =>
          s"Failed to insert BSON, `$b`, $r"
        case NoDatabase =>
          "Unable to determine a database in which to store temporary collections"
      }

      s"Error executing workflow: $msg"
    }
}
