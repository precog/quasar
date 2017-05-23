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

import quasar.precog.common.security._

import org.slf4s.Logging

import scalaz._
import scalaz.syntax.monad._

trait AccountFinder[M[+ _]] extends Logging { self =>
  def findAccountByAPIKey(apiKey: APIKey): M[Option[AccountId]]

  def findAccountDetailsById(accountId: AccountId): M[Option[AccountDetails]]

  def withM[N[+ _]](implicit t: M ~> N) = new AccountFinder[N] {
    def findAccountByAPIKey(apiKey: APIKey) = t(self.findAccountByAPIKey(apiKey))

    def findAccountDetailsById(accountId: AccountId) = t(self.findAccountDetailsById(accountId))
  }
}

object AccountFinder {
  def Empty[M[+ _]: Monad] = new AccountFinder[M] {
    def findAccountByAPIKey(apiKey: APIKey)          = None.point[M]
    def findAccountDetailsById(accountId: AccountId) = None.point[M]
  }
}
