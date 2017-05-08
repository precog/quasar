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

import quasar.blueeyes._
import quasar.precog.common.accounts._
import quasar.precog.common.security.service._

import org.slf4s.Logging

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._

import java.time.LocalDateTime

import Permission._

class StaticAPIKeyFinder[M[+ _]](apiKey: APIKey)(implicit val M: Monad[M]) extends APIKeyFinder[M] with Logging { self =>
  private val permissions = Set[Permission](
    ReadPermission(Path("/"), WrittenByAny),
    WritePermission(Path("/"), WriteAs.any),
    DeletePermission(Path("/"), WrittenByAny)
  )

  val rootGrant        = v1.GrantDetails(java.util.UUID.randomUUID.toString, None, None, permissions, instant.zero, None)
  val rootAPIKeyRecord = v1.APIKeyDetails(apiKey, Some("Static api key"), None, Set(rootGrant), Nil)

  val rootGrantId = M.point(rootGrant.grantId)
  val rootAPIKey  = M.point(rootAPIKeyRecord.apiKey)

  def findAPIKey(apiKey: APIKey, rootKey: Option[APIKey]) = M.point(if (apiKey == self.apiKey) Some(rootAPIKeyRecord) else None)
  def findGrant(grantId: GrantId)                         = M.point(if (rootGrant.grantId == grantId) Some(rootGrant) else None)

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]] = findAPIKey(fromRoot, None) map { _.toSet }

  def createAPIKey(accountId: AccountId, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails] = {
    throw new UnsupportedOperationException("API key management unavailable for standalone system.")
  }

  def addGrant(accountKey: APIKey, grantId: GrantId): M[Boolean] = {
    throw new UnsupportedOperationException("Grant management unavailable for standalone system.")
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[LocalDateTime]): M[Boolean] = M.point(true)
}
