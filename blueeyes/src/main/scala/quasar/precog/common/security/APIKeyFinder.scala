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

package quasar.precog.common.security

import quasar.precog.common.accounts.AccountId
import quasar.precog.common.security.service.v1

import org.slf4s.Logging

import scalaz._, Scalaz._

import java.time.LocalDateTime

trait APIKeyFinder[M[+ _]] extends AccessControl[M] with Logging { self =>
  def findAPIKey(apiKey: APIKey, rootKey: Option[APIKey]): M[Option[v1.APIKeyDetails]]

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]]

  def createAPIKey(accountId: AccountId, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails]

  def addGrant(accountKey: APIKey, grantId: GrantId): M[Boolean]

  def withM[N[+ _]](implicit t: M ~> N) = new APIKeyFinder[N] {
    def findAPIKey(apiKey: APIKey, rootKey: Option[APIKey]) =
      t(self.findAPIKey(apiKey, rootKey))

    def findAllAPIKeys(fromRoot: APIKey) =
      t(self.findAllAPIKeys(fromRoot))

    def createAPIKey(accountId: AccountId, keyName: Option[String] = None, keyDesc: Option[String] = None) =
      t(self.createAPIKey(accountId, keyName, keyDesc))

    def addGrant(accountKey: APIKey, grantId: GrantId) =
      t(self.addGrant(accountKey, grantId))

    def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[LocalDateTime]) =
      t(self.hasCapability(apiKey, perms, at))
  }
}

class DirectAPIKeyFinder[M[+ _]](underlying: APIKeyManager[M])(implicit val M: Monad[M]) extends APIKeyFinder[M] with Logging {
  val grantDetails: Grant => v1.GrantDetails = {
    case Grant(gid, gname, gdesc, _, _, perms, createdAt, exp) => v1.GrantDetails(gid, gname, gdesc, perms, createdAt, exp)
  }

  def recordDetails(rootKey: Option[APIKey]): PartialFunction[APIKeyRecord, M[v1.APIKeyDetails]] = {
    case APIKeyRecord(apiKey, name, description, issuer, grantIds, false) =>
      underlying.findAPIKeyAncestry(apiKey).flatMap { ancestors =>
        val ancestorKeys = ancestors.drop(1).map(_.apiKey) // The first element of ancestors is the key itself, so we drop it
        grantIds.map(underlying.findGrant).toList.sequence map { grants =>
          val divulgedIssuers = rootKey.map { rk =>
            ancestorKeys.reverse.dropWhile(_ != rk).reverse
          }.getOrElse(Nil)
          log.debug("Divulging issuers %s for key %s based on root key %s and ancestors %s".format(divulgedIssuers, apiKey, rootKey, ancestorKeys))
          v1.APIKeyDetails(apiKey, name, description, grants.flatten.map(grantDetails)(collection.breakOut), divulgedIssuers)
        }
      }
  }

  val recordDetails: PartialFunction[APIKeyRecord, M[v1.APIKeyDetails]] = recordDetails(None)

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[LocalDateTime]): M[Boolean] = {
    underlying.hasCapability(apiKey, perms, at)
  }

  def findAPIKey(apiKey: APIKey, rootKey: Option[APIKey]) = {
    underlying.findAPIKey(apiKey) flatMap {
      _.collect(recordDetails(rootKey)).sequence
    }
  }

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]] = {
    underlying.findAPIKey(fromRoot) flatMap {
      case Some(record) =>
        underlying.findAPIKeyChildren(record.apiKey) flatMap { recs =>
          (recs collect recordDetails).toList.sequence.map(_.toSet)
        }

      case None =>
        M.point(Set())
    }
  }

  def createAPIKey(accountId: AccountId, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails] = {
    underlying.newStandardAPIKeyRecord(accountId, keyName, keyDesc) flatMap recordDetails
  }

  def addGrant(accountKey: APIKey, grantId: GrantId): M[Boolean] = {
    underlying.addGrants(accountKey, Set(grantId)) map { _.isDefined }
  }
}
