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

import quasar.precog.common.security.service._
import quasar.blueeyes._
import quasar.blueeyes.util.Clock
import scalaz._, Scalaz._
import quasar.precog.TestSupport._

trait APIKeyFinderSpec[M[+_]] extends Specification {
  import Permission._

  implicit def M: Monad[M] with Comonad[M]

  def withAPIKeyFinder[A](mgr: APIKeyManager[M])(f: APIKeyFinder[M] => A): A

  private def emptyAPIKeyManager = new InMemoryAPIKeyManager[M](Clock.System)

  "API key finders" should {
    "create and find API keys" in {
      withAPIKeyFinder(emptyAPIKeyManager) { keyFinder =>
        val v1.APIKeyDetails(apiKey0, _, _, _, _) = keyFinder.createAPIKey("Anything works.").copoint
        val Some(v1.APIKeyDetails(apiKey1, _, _, _, _)) = keyFinder.findAPIKey(apiKey0, None).copoint
        apiKey0 must_== apiKey1
      }
    }

    "find existing API key" in {
      val (key, mgr) = (for {
        mgr <- M.point(emptyAPIKeyManager)
        key0 <- mgr.newStandardAPIKeyRecord("user1", None, None)
      } yield (key0.apiKey -> mgr)).copoint
      withAPIKeyFinder(mgr) { keyFinder =>
        keyFinder.findAPIKey(key, None).copoint map (_.apiKey) must_== Some(key)
      }
    }

    "new API keys should have standard permissions" in {
      withAPIKeyFinder(emptyAPIKeyManager) { keyFinder =>
        val accountId = "user1"
        val path = Path("/user1/")
        val key = keyFinder.createAPIKey(accountId, None, None).copoint
        val permissions: Set[Permission] = Set(
          ReadPermission(path, WrittenByAccount(accountId)),
          DeletePermission(path, WrittenByAccount(accountId)),
          WritePermission(path, WriteAs(accountId))
        )

        keyFinder.hasCapability(key.apiKey, permissions, None).copoint must beTrue
      }
    }

    "grant full permissions to another user" in {
      val path = Path("/user1/")
      val permissions: Set[Permission] = Set(
        ReadPermission(path, WrittenByAccount("user1")),
        WritePermission(path, WriteAsAny),
        DeletePermission(path, WrittenByAny)
      )

      val (key0, key1, grantId, mgr) = (for {
        mgr <- M.point(emptyAPIKeyManager)
        key0 <- mgr.newStandardAPIKeyRecord("user1", None, None)
        key1 <- mgr.newStandardAPIKeyRecord("user2", None, None)
        grant <- mgr.createGrant(None, None, key0.apiKey, Set.empty, permissions, None)
      } yield (key0.apiKey, key1.apiKey, grant.grantId, mgr)).copoint

      withAPIKeyFinder(mgr) { keyFinder =>
        keyFinder.addGrant(key1, grantId).copoint must beTrue
        keyFinder.hasCapability(key1, permissions, None).copoint must beTrue
      }
    }

    "find all child API Keys" in {
      val (parent, keys, mgr) = (for {
        mgr <- M.point(emptyAPIKeyManager)
        key0 <- mgr.newStandardAPIKeyRecord("user1", None, None)
        key1 <- mgr.createAPIKey(None, None, key0.apiKey, Set.empty)
        key2 <- mgr.createAPIKey(None, None, key0.apiKey, Set.empty)
      } yield ((key0.apiKey, Set(key1, key2) map (_.apiKey), mgr))).copoint

      withAPIKeyFinder(mgr) { keyFinder =>
        val children = keyFinder.findAllAPIKeys(parent).copoint
        children map (_.apiKey) must_== keys
      }
    }

    "not return grand-child API keys or self API key when finding children" in {
      val (parent, child, mgr) = (for {
        mgr <- M.point(emptyAPIKeyManager)
        key0 <- mgr.newStandardAPIKeyRecord("user1", None, None)
        key1 <- mgr.createAPIKey(None, None, key0.apiKey, Set.empty)
        key2 <- mgr.createAPIKey(None, None, key1.apiKey, Set.empty)
      } yield ((key0.apiKey, key1.apiKey, mgr))).copoint

      withAPIKeyFinder(mgr) { keyFinder =>
        val children = keyFinder.findAllAPIKeys(parent).copoint map (_.apiKey)
        children must_== Set(child)
      }
    }

    "return false when capabilities expire" in {
      val path = Path("/user1/")
      val permissions: Set[Permission] = Set(
        ReadPermission(path, WrittenByAccount("user1")),
        WritePermission(path, WriteAsAny),
        DeletePermission(path, WrittenByAny)
      )

      val expiration       = dateTime fromMillis 100
      val beforeExpiration = dateTime fromMillis 50
      val afterExpiration  = dateTime fromMillis 150

      val (key0, key1, grantId, mgr) = (for {
        mgr <- M.point(emptyAPIKeyManager)
        key0 <- mgr.newStandardAPIKeyRecord("user1", None, None)
        key1 <- mgr.newStandardAPIKeyRecord("user2", None, None)
        grant <- mgr.createGrant(None, None, key0.apiKey, Set.empty, permissions, Some(expiration))
      } yield (key0.apiKey, key1.apiKey, grant.grantId, mgr)).copoint

      withAPIKeyFinder(mgr) { keyFinder =>
        keyFinder.addGrant(key1, grantId).copoint must beTrue
        keyFinder.hasCapability(key1, permissions, Some(beforeExpiration)).copoint must beTrue
        keyFinder.hasCapability(key1, permissions, Some(afterExpiration)).copoint must beFalse
      }
    }

    "return issuer details when a proper root key is passed to findAPiKey" in {
      val (rootKey, key0, key1, mgr) = (for {
        mgr <- M.point(emptyAPIKeyManager)
        rootKey <- mgr.rootAPIKey
        key0 <- mgr.createAPIKey(Some("key0"), None, rootKey, Set.empty)
        key1 <- mgr.createAPIKey(Some("key1"), None, key0.apiKey, Set.empty)
      } yield (rootKey, key0.apiKey, key1.apiKey, mgr)).copoint

      withAPIKeyFinder(mgr) { keyFinder =>
        keyFinder.findAPIKey(key0, Some(rootKey)).copoint.get.issuerChain mustEqual List(rootKey)
        keyFinder.findAPIKey(key1, Some(rootKey)).copoint.get.issuerChain mustEqual List(key0, rootKey)
      }
    }

    "hide issuer details when a root key is not passed to findAPIKey" in {
      val (rootKey, key0, key1, mgr) = (for {
        mgr <- M.point(emptyAPIKeyManager)
        rootKey <- mgr.rootAPIKey
        key0 <- mgr.createAPIKey(Some("key0"), None, rootKey, Set.empty)
        key1 <- mgr.createAPIKey(Some("key1"), None, key0.apiKey, Set.empty)
      } yield (rootKey, key0.apiKey, key1.apiKey, mgr)).copoint

      withAPIKeyFinder(mgr) { keyFinder =>
        keyFinder.findAPIKey(key0, None).copoint.get.issuerChain mustEqual Nil
        keyFinder.findAPIKey(key1, None).copoint.get.issuerChain mustEqual Nil
      }
    }
  }
}

class DirectAPIKeyFinderSpec extends Specification {
  // include( 5 )

  //   new APIKeyFinderSpec[Need] {
  //   val M = Need.need
  //   def withAPIKeyFinder[A](mgr: APIKeyManager[Need])(f: APIKeyFinder[Need] => A): A = {
  //     f(new DirectAPIKeyFinder(mgr))
  //   }
  // })
}
