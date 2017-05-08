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
import service._
import quasar.precog.common.accounts.{ Account, AccountId }

import java.util.concurrent.TimeUnit._
import org.slf4s.Logging

import scalaz._, Scalaz._

import java.time.LocalDateTime

object APIKeyManager {
  def newUUID() = java.util.UUID.randomUUID.toString

  // 128 bit API key
  def newAPIKey(): String = newUUID().toUpperCase

  // 384 bit grant ID
  def newGrantId(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-", "")
}

trait APIKeyManager[M[+ _]] extends Logging { self =>
  implicit def M: Monad[M]

  def rootGrantId: M[GrantId]
  def rootAPIKey: M[APIKey]

  def createGrant(name: Option[String],
                  description: Option[String],
                  issuerKey: APIKey,
                  parentIds: Set[GrantId],
                  perms: Set[Permission],
                  expiration: Option[LocalDateTime]): M[Grant]

  def createAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]): M[APIKeyRecord]

  def newStandardAccountGrant(accountId: AccountId, path: Path, name: Option[String] = None, description: Option[String] = None): M[Grant] =
    for {
      rk <- rootAPIKey
      rg <- rootGrantId
      ng <- createGrant(name, description, rk, Set(rg), Account.newAccountPermissions(accountId, path), None)
    } yield ng

  def newStandardAPIKeyRecord(accountId: AccountId, name: Option[String] = None, description: Option[String] = None): M[APIKeyRecord] = {
    val path             = Path("/%s/".format(accountId))
    val grantName        = name.map(_ + "-grant")
    val grantDescription = name.map(_ + " account grant")
    val grant            = newStandardAccountGrant(accountId, path, grantName, grantDescription)
    for {
      rk <- rootAPIKey
      ng <- grant
      nk <- createAPIKey(name, description, rk, Set(ng.grantId))
    } yield nk
  }

  def listAPIKeys: M[Seq[APIKeyRecord]]

  def findAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]
  def findAPIKeyChildren(apiKey: APIKey): M[Set[APIKeyRecord]]
  def findAPIKeyAncestry(apiKey: APIKey): M[List[APIKeyRecord]] = {
    findAPIKey(apiKey) flatMap {
      case Some(keyRecord) =>
        if (keyRecord.issuerKey == apiKey) M.point(List(keyRecord))
        else findAPIKeyAncestry(keyRecord.issuerKey) map { keyRecord :: _ }

      case None =>
        M.point(List.empty[APIKeyRecord])
    }
  }

  def listGrants: M[Seq[Grant]]
  def findGrant(gid: GrantId): M[Option[Grant]]
  def findGrantChildren(gid: GrantId): M[Set[Grant]]

  def listDeletedAPIKeys: M[Seq[APIKeyRecord]]
  def findDeletedAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]

  def listDeletedGrants: M[Seq[Grant]]
  def findDeletedGrant(gid: GrantId): M[Option[Grant]]
  def findDeletedGrantChildren(gid: GrantId): M[Set[Grant]]

  def addGrants(apiKey: APIKey, grants: Set[GrantId]): M[Option[APIKeyRecord]]
  def removeGrants(apiKey: APIKey, grants: Set[GrantId]): M[Option[APIKeyRecord]]

  def deleteAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]
  def deleteGrant(apiKey: GrantId): M[Set[Grant]]

  def findValidGrant(grantId: GrantId, at: Option[LocalDateTime] = None): M[Option[Grant]] =
    findGrant(grantId) flatMap { grantOpt =>
      grantOpt map { (grant: Grant) =>
        if (grant.isExpired(at)) None.point[M]
        else
          grant.parentIds.foldLeft(some(grant).point[M]) {
            case (accM, parentId) =>
              accM flatMap {
                _ traverse { grant =>
                  findValidGrant(parentId, at).map(_ => grant)
                }
              }
          }
      } getOrElse {
        None.point[M]
      }
    }

  def validGrants(apiKey: APIKey, at: Option[LocalDateTime] = None): M[Set[Grant]] = {
    log.trace("Checking grant validity for apiKey " + apiKey)
    findAPIKey(apiKey) flatMap {
      _ map {
        _.grants.toList.traverse(findValidGrant(_, at)) map {
          _.flatMap(_.toSet)(collection.breakOut): Set[Grant]
        }
      } getOrElse {
        Set.empty.point[M]
      }
    }
  }

  def deriveGrant(name: Option[String],
                  description: Option[String],
                  issuerKey: APIKey,
                  perms: Set[Permission],
                  expiration: Option[LocalDateTime] = None): M[Option[Grant]] = {
    validGrants(issuerKey, expiration).flatMap { grants =>
      if (!Grant.implies(grants, perms, expiration)) none[Grant].point[M]
      else {
        val minimized = Grant.coveringGrants(grants, perms, expiration).map(_.grantId)
        if (minimized.isEmpty) {
          none[Grant].point[M]
        } else {
          createGrant(name, description, issuerKey, minimized, perms, expiration) map { some }
        }
      }
    }
  }

  def deriveAndAddGrant(name: Option[String],
                        description: Option[String],
                        issuerKey: APIKey,
                        perms: Set[Permission],
                        recipientKey: APIKey,
                        expiration: Option[LocalDateTime] = None): M[Option[Grant]] = {
    deriveGrant(name, description, issuerKey, perms, expiration) flatMap {
      case Some(grant) =>
        addGrants(recipientKey, Set(grant.grantId)) map {
          _ map { _ =>
            grant
          }
        }
      case None => none[Grant].point[M]
    }
  }

  def newAPIKeyWithGrants(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[v1.NewGrantRequest]): M[Option[APIKeyRecord]] = {
    val grantList = grants.toList
    grantList.traverse(grant => hasCapability(issuerKey, grant.permissions, grant.expirationDate)) flatMap { checks =>
      if (checks.forall(_ == true)) {
        for {
          newGrants <- grantList traverse { g =>
                        deriveGrant(g.name, g.description, issuerKey, g.permissions, g.expirationDate)
                      }
          newKey <- createAPIKey(name, description, issuerKey, newGrants.flatMap(_.map(_.grantId))(collection.breakOut))
        } yield some(newKey)
      } else {
        none[APIKeyRecord].point[M]
      }
    }
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[LocalDateTime] = None): M[Boolean] =
    validGrants(apiKey, at).map(Grant.implies(_, perms, at))
}
