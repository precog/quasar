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

import quasar.precog.common._, accounts._
import quasar.precog.common.security.service._
import quasar.precog.TestSupport._

import scalaz._, Scalaz._

trait APIKeyManagerSpec[M[+_]] extends Specification {
  implicit def M: Monad[M] with Comonad[M]

  "API Key Manager" should {
    "properly ascribe parentage for grants" in {
      val path = Path("/user1/")
      val mgr = new InMemoryAPIKeyManager[M](quasar.blueeyes.util.Clock.System)

      val grantParentage = for {
        rootKey <- mgr.rootAPIKey
        rootGrantId <- mgr.rootGrantId
        perms = Account.newAccountPermissions("012345", Path("/012345/"))
        grantRequest = v1.NewGrantRequest(Some("testGrant"), None, Set(rootGrantId), perms, None)
        record <- mgr.newAPIKeyWithGrants(Some("test"), None, rootKey, Set(grantRequest))
        grants <- record.toList.flatMap(_.grants).map(mgr.findGrant).sequence
      } yield {
        (grants.flatten.flatMap(_.parentIds), rootGrantId)
      }

      val (grantParents, rootGrantId) = grantParentage.copoint

      val gs = grantParents collect { case gid: GrantId => gid }
      grantParents must haveSize(1)
      grantParents must contain(rootGrantId)
    }
  }
}

object APIKeyManagerSpec extends APIKeyManagerSpec[Need] {
  val M = Need.need
}

