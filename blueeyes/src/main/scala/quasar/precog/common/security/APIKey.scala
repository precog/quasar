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

package quasar.precog.common
package security

import quasar.blueeyes._, json._, serialization._
import IsoSerialization._, Iso8601Serialization._, Versioned._

import shapeless.HNil

import scalaz._, Scalaz._

case class APIKeyRecord(apiKey: APIKey, name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId], isRoot: Boolean)

object APIKeyRecord {
  val schemaV1 = "apiKey" :: "name" :: "description" :: ("issuerKey" ||| "(undefined)") :: "grants" :: "isRoot" :: HNil

  @deprecated("V0 serialization schemas should be removed when legacy data is no longer needed", "2.1.5")
  val schemaV0 = "tid" :: "name" :: "description" :: ("cid" ||| "(undefined)") :: "gids" :: ("isRoot" ||| false) :: HNil

  val decomposerV1: Decomposer[APIKeyRecord] = decomposerV[APIKeyRecord](schemaV1, Some("1.0".v))
  val extractorV2: Extractor[APIKeyRecord]   = extractorV[APIKeyRecord](schemaV1, Some("1.0".v))
  val extractorV1: Extractor[APIKeyRecord]   = extractorV[APIKeyRecord](schemaV1, None)
  val extractorV0: Extractor[APIKeyRecord]   = extractorV[APIKeyRecord](schemaV0, None)

  implicit val decomposer: Decomposer[APIKeyRecord] = decomposerV1
  implicit val extractor: Extractor[APIKeyRecord]   = extractorV2 <+> extractorV1 <+> extractorV0
}
