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

import accounts.AccountId

import quasar.blueeyes._, json._, serialization._
import Extractor._
import Iso8601Serialization._
import Versioned._
import org.slf4s.Logging
import scalaz._, Scalaz._, Validation._
import Permission._

sealed trait AccessMode { def name: String }
sealed trait ReadMode  extends AccessMode
sealed trait WriteMode extends AccessMode

object AccessMode {
  case object Read         extends AccessMode with ReadMode { val name = "read" }
  case object Execute      extends AccessMode with ReadMode { val name = "execute" }
  case object ReadMetadata extends AccessMode with ReadMode { val name = "metadata" }

  case object Create  extends AccessMode with WriteMode { val name = "create" }
  case object Replace extends AccessMode with WriteMode { val name = "replace" }
  case object Append  extends AccessMode with WriteMode { val name = "append" }
}

sealed trait Permission extends Logging {
  def path: Path

  def implies(other: Permission): Boolean
}

sealed trait WrittenByPermission extends Permission {
  def writtenBy: WrittenBy
}

object WrittenByPermission {
  def unapply(perm: WrittenByPermission): Option[(Path, WrittenBy)] = Some((perm.path, perm.writtenBy))
}

case class WritePermission(path: Path, writeAs: WriteAs) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case WritePermission(p0, w0) => path.isEqualOrParentOf(p0) && (writeAs == WriteAsAny || writeAs == w0)
    case _                       => false
  }
}

case class ExecutePermission(path: Path, writtenBy: WrittenBy) extends Permission with WrittenByPermission {
  def implies(other: Permission): Boolean = other match {
    case p @ ExecutePermission(path0, w0) => path.isEqualOrParentOf(path0) && WrittenBy.implies(this, p)
    case _                                => false
  }
}

case class ReadPermission(path: Path, writtenBy: WrittenBy) extends Permission with WrittenByPermission {
  def implies(other: Permission): Boolean = other match {
    case p: ReadPermission                => WrittenBy.implies(this, p)
    case p: ReducePermission              => WrittenBy.implies(this, p)
    case p @ ExecutePermission(path0, w0) => path.isEqualOrParentOf(path0) && WrittenBy.implies(this, p)
    case _                                => false
  }
}

case class ReducePermission(path: Path, writtenBy: WrittenBy) extends Permission with WrittenByPermission {
  def implies(other: Permission): Boolean = other match {
    case p: ReducePermission => WrittenBy.implies(this, p)
    case _                   => false
  }
}

case class DeletePermission(path: Path, writtenBy: WrittenBy) extends Permission with WrittenByPermission {
  def implies(other: Permission): Boolean = other match {
    case p: DeletePermission => WrittenBy.implies(this, p)
    case _                   => false
  }
}

object Permission {
  sealed trait WriteAs
  case object WriteAsAny extends WriteAs
  case class WriteAsAll private[Permission] (accountIds: Set[AccountId]) extends WriteAs

  object WriteAs {
    def all(accountIds: NonEmptyList[AccountId]): WriteAs = WriteAsAll(accountIds.list.toVector.toSet)
    val any: WriteAs = WriteAsAny

    private[Permission] def apply(accountIds: Set[AccountId]): WriteAs = if (accountIds.isEmpty) WriteAsAny else WriteAsAll(accountIds)

    def apply(accountId: AccountId): WriteAs = apply(Set(accountId))
  }

  sealed trait WrittenBy
  case object WrittenByAny extends WrittenBy
  case class WrittenByAccount(accountId: AccountId) extends WrittenBy

  object WrittenBy {
    val any: WrittenBy = WrittenByAny
    def apply(accountId: AccountId): WrittenBy = WrittenByAccount(accountId)

    def implies(permission: WrittenByPermission, candidate: WrittenByPermission): Boolean = {
      permission.path.isEqualOrParentOf(candidate.path) &&
      (permission.writtenBy match {
            case WrittenByAny => true
            case WrittenByAccount(accountId) =>
              candidate.writtenBy match {
                case WrittenByAny          => false
                case WrittenByAccount(cid) => cid == accountId
              }
          })
    }
  }

  def accessType(p: Permission) = p match {
    case _: ExecutePermission => "execute"
    case _: ReadPermission    => "read"
    case _: ReducePermission  => "reduce"
    case _: WritePermission   => "write"
    case _: DeletePermission  => "delete"
  }

  def ownerAccountIds(p: Permission): Set[AccountId] = p match {
    case WritePermission(_, WriteAsAll(ids))          => ids
    case WritePermission(_, WriteAsAny)               => Set()
    case WrittenByPermission(_, WrittenByAccount(id)) => Set(id)
    case WrittenByPermission(_, WrittenByAny)         => Set()
  }

  val decomposerV1Base: Decomposer[Permission] = new Decomposer[Permission] {
    override def decompose(p: Permission): JValue = {
      JObject(
        "accessType"      -> accessType(p).serialize,
        "path"            -> p.path.serialize,
        "ownerAccountIds" -> ownerAccountIds(p).serialize
      )
    }
  }

  val extractorV1Base: Extractor[Permission] = new Extractor[Permission] {
    private def writtenByPermission(obj: JValue, pathV: Validation[Error, Path])(f: (Path, WrittenBy) => Permission): Validation[Error, Permission] = {
      ((obj \? "ownerAccountIds") map { ids: JValue =>
        pathV flatMap { path =>
          ids.validated[Set[AccountId]] flatMap { accountIds =>
            if (accountIds.isEmpty) success(f(path, WrittenByAny))
            else if (accountIds.size == 1) success(f(path, WrittenByAccount(accountIds.head)))
            else failure(Invalid("Cannot extract read permission for more than one account ID."))
          }
        }
      }).getOrElse( pathV map { f(_: Path, WrittenByAny) })
    }

    override def validated(obj: JValue) = {
      val pathV = obj.validated[Path]("path")
      obj.validated[String]("accessType").map(_.toLowerCase.trim) flatMap {
        case "write" =>
          (obj \? "ownerAccountIds") map { ids =>
            (pathV |@| ids.validated[Set[AccountId]]) { (path, accountIds) =>
              WritePermission(path, WriteAs(accountIds))
            }
          } getOrElse {
            pathV map { WritePermission(_: Path, WriteAsAny) }
          }

        case "read"             => writtenByPermission(obj, pathV) { ReadPermission.apply _ }
        case "reduce"           => writtenByPermission(obj, pathV) { ReducePermission.apply _ }
        case "owner" | "delete" => writtenByPermission(obj, pathV) { DeletePermission.apply _ }
        case other              => failure(Invalid("Unrecognized permission type: " + other))
      }
    }
  }

  val extractorV0: Extractor[Permission] = new Extractor[Permission] {
    private def writtenByPermission(obj: JValue, pathV: Validation[Error, Path])(f: (Path, WrittenBy) => Permission): Validation[Error, Permission] = {
      obj.validated[Option[String]]("ownerAccountId") flatMap { opt =>
        opt map { id =>
          pathV map { f(_: Path, WrittenByAccount(id)) }
        } getOrElse {
          pathV map { f(_: Path, WrittenByAny) }
        }
      }
    }

    override def validated(obj: JValue) = {
      val pathV = obj.validated[Path]("path")
      obj.validated[String]("type").map(_.toLowerCase.trim) flatMap {
        case "write" =>
          obj.validated[Option[String]]("ownerAccountId") flatMap { opt =>
            opt map { id =>
              pathV map { WritePermission(_: Path, WriteAs(Set(id))) }
            } getOrElse {
              pathV map { WritePermission(_: Path, WriteAsAny) }
            }
          }

        case "read"             => writtenByPermission(obj, pathV) { ReadPermission.apply _ }
        case "reduce"           => writtenByPermission(obj, pathV) { ReducePermission.apply _ }
        case "owner" | "delete" => writtenByPermission(obj, pathV) { DeletePermission.apply _ }
        case other              => failure(Invalid("Unrecognized permission type: " + other))
      }
    }
  }

  implicit val decomposer = decomposerV1Base.versioned(Some("1.0".v))
  implicit val extractor  = extractorV1Base.versioned(Some("1.0".v)) <+> extractorV1Base
}
