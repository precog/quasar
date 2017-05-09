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

import quasar.blueeyes.json._
import quasar.blueeyes.json.serialization._
import quasar.blueeyes.json.serialization.Extractor._
import quasar.blueeyes.json.serialization.IsoSerialization._
import quasar.blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.syntax.std.boolean._

case class Authorities private (accountIds: Set[AccountId]) {
  def expand(ownerAccountId: AccountId) =
    this.copy(accountIds = this.accountIds + ownerAccountId)

  def render = accountIds.mkString("[", ", ", "]")
}

object Authorities {
  def apply(accountIds: NonEmptyList[AccountId]): Authorities = apply(accountIds.list.toVector.toSet)

  def apply(firstAccountId: AccountId, others: AccountId*): Authorities =
    apply(others.toSet + firstAccountId)

  def ifPresent(accountIds: Set[AccountId]): Option[Authorities] = accountIds.nonEmpty.option(apply(accountIds))

  implicit val AuthoritiesDecomposer: Decomposer[Authorities] = new Decomposer[Authorities] {
    override def decompose(authorities: Authorities): JValue = {
      JObject(JField("uids", JArray(authorities.accountIds.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val AuthoritiesExtractor: Extractor[Authorities] = new Extractor[Authorities] {
    override def validated(obj: JValue): Validation[Error, Authorities] =
      (obj \ "uids").validated[Set[String]].map(Authorities(_))
  }

  implicit object AuthoritiesSemigroup extends Semigroup[Authorities] {
    def append(a: Authorities, b: => Authorities): Authorities = {
      Authorities(a.accountIds ++ b.accountIds)
    }
  }
}
