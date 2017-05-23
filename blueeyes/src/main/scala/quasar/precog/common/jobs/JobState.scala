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

package quasar.precog.common.jobs

import quasar.blueeyes.json.{ jfield, jobject, JNum, JObject, JValue }
import quasar.blueeyes.json.serialization.{ Decomposer, Extractor }
import quasar.blueeyes.json.serialization.DefaultSerialization._

import java.time.LocalDateTime

import scalaz._

/**
 * The Job state is used to keep track of the overall state of a Job. A Job is
 * put in a special initial state (`NotStarted`) when it is first created, and
 * is moved to the `Started` state once it gets its first status update. From
 * here it can either be `Cancelled` or put into one of several terminal
 * states. Once a job is in a terminal state, it can no longer be moved to a
 * new state.
 */
sealed abstract class JobState(val isTerminal: Boolean)

object JobState extends JobStateSerialization {
  case object NotStarted extends JobState(false)
  case class Started(timestamp: LocalDateTime, prev: JobState) extends JobState(false)
  case class Cancelled(reason: String, timestamp: LocalDateTime, prev: JobState) extends JobState(false)
  case class Aborted(reason: String, timestamp: LocalDateTime, prev: JobState) extends JobState(true)
  case class Expired(timestamp: LocalDateTime, prev: JobState) extends JobState(true)
  case class Finished(timestamp: LocalDateTime, prev: JobState) extends JobState(true)

  def describe(state: JobState): String = state match {
    case NotStarted => "The job has not yet been started."
    case Started(started, _) => "The job was started at %s." format started
    case Cancelled(reason, _, _) => "The job has been cancelled due to '%s'." format reason
    case Aborted(reason, _, _) => "The job was aborted early due to '%s'." format reason
    case Expired(expiration, _) => "The job expired at %s." format expiration
    case Finished(_, _) => "The job has finished successfully."
  }
}

trait JobStateSerialization {
  import Extractor._
  import JobState._
  import scalaz.Validation._
  import scalaz.syntax.apply._

  import quasar.blueeyes.json.serialization.DefaultExtractors._
  import quasar.blueeyes.json.serialization.DefaultDecomposers._

  implicit object JobStateDecomposer extends Decomposer[JobState] {
    private def base(state: String, timestamp: LocalDateTime, previous: JobState, reason: Option[String] = None): JObject = {
      JObject(
        jfield("state", state) ::
        jfield("timestamp", timestamp) ::
        jfield("previous", decompose(previous)) ::
        (reason map { jfield("reason", _) :: Nil } getOrElse Nil)
      )
    }

    override def decompose(job: JobState): JValue = job match {
      case NotStarted =>
        jobject(jfield("state", "not_started"))

      case Started(ts, prev) =>
        base("started", ts, prev)

      case Cancelled(reason, ts, prev) =>
        base("cancelled", ts, prev, Some(reason))

      case Aborted(reason, ts, prev) =>
        base("aborted", ts, prev, Some(reason))

      case Expired(ts, prev) =>
        base("expired", ts, prev)

      case Finished(ts, prev) =>
        base("finished", ts, prev)
    }
  }

  implicit object JobStateExtractor extends Extractor[JobState] {
    def extractBase(obj: JValue): Validation[Error, (LocalDateTime, JobState)] = {
      ((obj \ "timestamp").validated[LocalDateTime] |@| (obj \ "previous").validated[JobState]).tupled
    }

    override def validated(obj: JValue) = {
      (obj \ "state").validated[String] flatMap {
        case "not_started" =>
          success[Error, JobState](NotStarted)

        case "started" =>
          extractBase(obj) map (Started(_, _)).tupled

        case "cancelled" =>
          ((obj \ "reason").validated[String] |@| extractBase(obj)) { case (reason, (timestamp, previous)) =>
            Cancelled(reason, timestamp, previous)
          }

        case "aborted" =>
          ((obj \ "reason").validated[String] |@| extractBase(obj)) { case (reason, (timestamp, previous)) =>
            Aborted(reason, timestamp, previous)
          }

        case "expired" =>
          extractBase(obj) map (Expired(_, _)).tupled

        case "finished" =>
          extractBase(obj) flatMap { case (timestamp, previous) =>
            success(Finished(timestamp, previous))
          }
      }
    }
  }
}
