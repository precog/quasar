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
package accounts

import org.slf4s.Logging

import scalaz.Monad
import scalaz.syntax.monad._

import quasar.blueeyes._
import quasar.precog.common.security._

class StaticAccountFinder[M[+ _]: Monad](accountId: AccountId, apiKey: APIKey, rootPath: Option[String] = None, email: String = "static@precog.com")
    extends AccountFinder[M]
    with Logging {
  private[this] val details = Some(AccountDetails(accountId, email, dateTime.zero, apiKey, Path(rootPath.getOrElse("/" + accountId)), AccountPlan.Root))

  log.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(details.get))

  def findAccountByAPIKey(apiKey: APIKey) = Some(accountId).point[M]

  def findAccountDetailsById(accountId: AccountId) = details.point[M]
}
