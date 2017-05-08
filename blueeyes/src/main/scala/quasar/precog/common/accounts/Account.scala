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

import quasar.precog.common.Path
import quasar.precog.common.security.{ APIKey, Permission, ReadPermission, WritePermission, DeletePermission }
import Permission._

import quasar.blueeyes._, json._, serialization._
import quasar.blueeyes.json.serialization.IsoSerialization._
import quasar.blueeyes.json.serialization.DefaultSerialization._
import quasar.blueeyes.json.serialization.Versioned._

import scalaz.Validation
import scalaz.syntax.apply._
import scalaz.syntax.plus._

import java.time.LocalDateTime

case class AccountPlan(planType: String)
object AccountPlan {
  val Root = AccountPlan("Root")
  val Free = AccountPlan("Free")

  val schema                           = "type" :: HNil
  implicit val (decomposer, extractor) = serializationV[AccountPlan](schema, None)
}

case class Account(accountId: AccountId,
                   email: String,
                   passwordHash: String,
                   passwordSalt: String,
                   accountCreationDate: LocalDateTime,
                   apiKey: APIKey,
                   rootPath: Path,
                   plan: AccountPlan,
                   parentId: Option[String] = None,
                   lastPasswordChangeTime: Option[LocalDateTime] = None,
                   profile: Option[JValue] = None)

object Account {
  val schemaV1     = "accountId" :: "email" :: "passwordHash" :: "passwordSalt" :: "accountCreationDate" :: "apiKey" :: "rootPath" :: "plan" :: "parentId" :: "lastPasswordChangeTime" :: "profile" :: HNil

  val extractorPreV             = extractorV[Account](schemaV1, None)
  val extractorV1               = extractorV[Account](schemaV1, Some("1.1".v))
  implicit val accountExtractor = extractorV1 <+> extractorPreV

  implicit val decomposerV1 = decomposerV[Account](schemaV1, Some("1.1".v))

  private val randomSource = new java.security.SecureRandom

  def randomSalt() = {
    val saltBytes = new Array[Byte](256)
    randomSource.nextBytes(saltBytes)
    saltBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut): String
  }

  def newAccountPermissions(accountId: AccountId, accountPath: Path): Set[Permission] = {
    // Path is "/" so that an account may read data it wrote no matter what path it exists under.
    // See AccessControlSpec, NewGrantRequest
    Set[Permission](
      WritePermission(accountPath, WriteAsAny),
      DeletePermission(accountPath, WrittenByAny),
      ReadPermission(Path.Root, WrittenByAccount(accountId))
    )
  }
}

case class WrappedAccountId(accountId: AccountId)

object WrappedAccountId {
  val schema = "accountId" :: HNil

  implicit val (wrappedAccountIdDecomposer, wrappedAccountIdExtractor) = serializationV[WrappedAccountId](schema, None)
}
