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

package quasar.yggdrasil.execution

import quasar.blueeyes._, json._, serialization._
import IsoSerialization._, Iso8601Serialization._, Versioned._
import quasar.precog.common._, security._, accounts._

import java.time.LocalDateTime

final case class EvaluationContext(apiKey: APIKey, account: AccountDetails, basePath: Path, scriptPath: Path, startTime: LocalDateTime)

object EvaluationContext {
  val schemaV1 = "apiKey" :: "account" :: "basePath" :: "scriptPath" :: "startTime" :: HNil

  implicit val decomposer: Decomposer[EvaluationContext] = decomposerV(schemaV1, Some("1.0".v))
  implicit val extractor: Extractor[EvaluationContext]   = extractorV(schemaV1, Some("1.0".v))
}
