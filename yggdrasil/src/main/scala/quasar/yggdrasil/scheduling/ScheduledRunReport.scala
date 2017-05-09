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

package quasar.yggdrasil.scheduling

import quasar.yggdrasil

import quasar.blueeyes.json._
import quasar.blueeyes.json.serialization._
import quasar.blueeyes.json.serialization.Versioned._
import quasar.blueeyes.json.serialization.DefaultSerialization._

import java.util.UUID

import java.time.LocalDateTime

import scalaz._

import shapeless._

object ScheduledRunReport {
  import quasar.precog.common.JavaSerialization._

  val schemaV1 = "id" :: "startedAt" :: "endedAt" :: "records" :: "messages" :: HNil

  implicit val decomposer = decomposerV[ScheduledRunReport](schemaV1, Some("1.0".v))
  implicit val extractor  = extractorV[ScheduledRunReport](schemaV1, Some("1.0".v))
}

case class ScheduledRunReport(id: UUID, startedAt: LocalDateTime, endedAt: LocalDateTime, records: Long, messages: List[String] = Nil)
