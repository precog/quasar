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
package service

import quasar.blueeyes._, json._, serialization._
import quasar.precog.common.accounts._

import java.time.LocalDateTime

import IsoSerialization._, Iso8601Serialization._

object v1 {
  case class GrantDetails(grantId: GrantId,
                          name: Option[String],
                          description: Option[String],
                          permissions: Set[Permission],
                          createdAt: Instant,
                          expirationDate: Option[LocalDateTime]) {
    def isValidAt(timestamp: Instant) = {
      createdAt.isBefore(timestamp) && expirationDate.forall(_ isAfter (dateTime fromMillis timestamp.getMillis))
    }
  }
  object GrantDetails {
    val schema = "grantId" :: "name" :: "description" :: "permissions" :: ("createdAt" ||| instant.zero) :: "expirationDate" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[GrantDetails](schema)
  }

  case class APIKeyDetails(apiKey: APIKey, name: Option[String], description: Option[String], grants: Set[GrantDetails], issuerChain: List[APIKey])
  object APIKeyDetails {
    val schema = "apiKey" :: "name" :: "description" :: "grants" :: "issuerChain" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[APIKeyDetails](schema)
  }

  case class NewGrantRequest(name: Option[String],
                             description: Option[String],
                             parentIds: Set[GrantId],
                             permissions: Set[Permission],
                             expirationDate: Option[LocalDateTime]) {
    def isExpired(at: Option[LocalDateTime]) = (expirationDate, at) match {
      case (None, _)                 => false
      case (_, None)                 => true
      case (Some(expiry), Some(ref)) => expiry.isBefore(ref)
    }
  }

  object NewGrantRequest {
    private implicit val reqPermDecomposer = Permission.decomposerV1Base

    val schemaV1 = "name" :: "description" :: ("parentIds" ||| Set.empty[GrantId]) :: "permissions" :: "expirationDate" :: HNil

    implicit val decomposerV1 = IsoSerialization.decomposer[NewGrantRequest](schemaV1)
    implicit val extractorV1  = IsoSerialization.extractor[NewGrantRequest](schemaV1)
  }

  case class NewAPIKeyRequest(name: Option[String], description: Option[String], grants: Set[NewGrantRequest])

  object NewAPIKeyRequest {
    val schemaV1 = "name" :: "description" :: "grants" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[NewAPIKeyRequest](schemaV1)
  }
}
