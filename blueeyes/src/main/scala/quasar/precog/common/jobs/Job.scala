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

import quasar.precog.common.security._

import quasar.blueeyes.json._
import quasar.blueeyes.json.serialization.{ Decomposer, Extractor }
import quasar.blueeyes.json.serialization.DefaultSerialization._
import quasar.blueeyes.json.serialization.Versioned._

import shapeless._

import scalaz.syntax.std.boolean._

case class Job(id: JobId, apiKey: APIKey, name: String, jobType: String, data: Option[JValue], state: JobState)
object Job {
  val schemaV1 = "id" :: "apiKey" :: "name" :: "type" :: "data" :: "state" :: HNil
  implicit val decomposerV1: Decomposer[Job] = decomposerV[Job](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[Job] = extractorV[Job](schemaV1, Some("1.0".v))
}

case class Message(job: JobId, id: MessageId, channel: String, value: JValue)
object Message {
  object channels {
    val Status = "status"
    val Errors = "errors"
    val Warnings = "warnings"
  }

  val schemaV1 = "jobId" :: "id" :: "channel" :: "value" :: HNil
  implicit val decomposerV1: Decomposer[Message] = decomposerV[Message](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[Message] = extractorV[Message](schemaV1, Some("1.0".v))
}

case class Status(job: JobId, id: StatusId, message: String, progress: BigDecimal, unit: String, info: Option[JValue])
object Status {
  import JobManager._
  import scalaz.syntax.apply._

  val schemaV1 = "job" :: "id" :: "message" :: "progress" :: "unit" :: "info" :: HNil
  implicit val decomposerV1: Decomposer[Status] = decomposerV[Status](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[Status] = extractorV[Status](schemaV1, Some("1.0".v))

  def fromMessage(message: Message): Option[Status] = {
    (message.channel == channels.Status) option {
      ((message.value \ "message").validated[String] |@|
       (message.value \ "progress").validated[BigDecimal] |@|
       (message.value \ "unit").validated[String]) { (msg, progress, unit) =>
        Status(message.job, message.id, msg, progress, unit, message.value \? "info")
      }
    } flatMap {
      _.toOption
    } 
  }

  def toMessage(status: Status): Message = {
    Message(status.job, status.id, channels.Status, JObject(
      jfield("message", status.message) ::
      jfield("progress", status.progress) ::
      jfield("unit", status.unit) ::
      (status.info map (jfield("info", _) :: Nil) getOrElse Nil)
    ))
  }
}
