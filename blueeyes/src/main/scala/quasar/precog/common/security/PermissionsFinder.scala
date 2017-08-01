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

import quasar.blueeyes._
import quasar.precog.common.Path
import quasar.precog.common.accounts.AccountFinder
import quasar.precog.common.security.service.v1

import scalaz._, Scalaz._

object PermissionsFinder {
  import Permission._
  def canWriteAs(permissions: Set[WritePermission], authorities: Authorities): Boolean = {
    val permWriteAs = permissions.map(_.writeAs)
    permWriteAs.exists(_ == WriteAsAny) || {
      val writeAsAlls = permWriteAs.collect({ case WriteAsAll(s) if s.subsetOf(authorities.accountIds) => s })

      permWriteAs.nonEmpty &&
      writeAsAlls.foldLeft(authorities.accountIds)({ case (remaining, s) => remaining diff s }).isEmpty
    }
  }
}

class PermissionsFinder[M[+ _]: Monad](val apiKeyFinder: APIKeyFinder[M], val accountFinder: AccountFinder[M], timestampRequiredAfter: Instant)
    extends org.slf4s.Logging {

  private def filterWritePermissions(keyDetails: v1.APIKeyDetails, path: Path, at: Option[Instant]): Set[WritePermission] = {
    keyDetails.grants filter { g =>
      (at exists { g.isValidAt _ }) || g.createdAt.isBefore(timestampRequiredAfter)
    } flatMap {
      _.permissions collect {
        case perm @ WritePermission(path0, _) if path0.isEqualOrParentOf(path) => perm
      }
    }
  }

  def writePermissions(apiKey: APIKey, path: Path, at: Instant): M[Set[WritePermission]] = {
    apiKeyFinder.findAPIKey(apiKey, None) map {
      case Some(details) =>
        log.debug("Filtering write grants from " + details + " for " + path + " at " + at)
        filterWritePermissions(details, path, Some(at))

      case None =>
        log.warn("No API key details found for %s %s at %s".format(apiKey, path.path, at.toString))
        Set()
    }
  }

  def findBrowsableChildren(apiKey: APIKey, path: Path): M[Set[Path]] = {
    for {
      permissions <- apiKeyFinder.findAPIKey(apiKey, None) map { details =>
                      details.toSet.flatMap(_.grants).flatMap(_.permissions)
                    }
      accountId <- accountFinder.findAccountByAPIKey(apiKey)
      accountPath <- accountId.traverse(accountFinder.findAccountDetailsById)
    } yield {
      // FIXME: Not comprehensive/exhaustive in terms of finding all possible data you could read
      permissions flatMap {
        case perm @ WrittenByPermission(p0, _) if p0.isEqualOrParentOf(path) =>
          if (perm.path == Path.Root) accountPath.flatten.map(_.rootPath) else Some(perm.path)

        case _ => None
      }
    }
  }
}
