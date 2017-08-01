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

package quasar

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, AFile, APath, unsafeSandboxAbs}
import quasar.db.Schema
import quasar.fs.FileSystemType
import quasar.fs.mount.MountType

import doobie.imports._
import pathy.Path, Path._
import scalaz._, Scalaz._

/* NB: the `Meta` and `Composite` instances defined here handle errors by
  throwing exceptions. This departs from the convention established for _all_
  other quasar(-advanced) code, which deserves some explanation.

  `Meta` instances are always and only called by doobie from query execution,
  which is expected to be effectful and exception-throwing, so it's always
  wrapped in a `Catchable` (i.e. `Task`). Doobie embraces this as the reality
  for database interaction, at least with JDBC (see
  http://tpolecat.github.io/doobie-0.2.3/08-Error-Handling.html).

  To make this at least a bit disciplined, we now define our own sub-type of
  SQLException so that these errors will be easy to trace when they occur at
  runtime. That should only happen in the case of bugs or admins tampering with
  the database.
  */
package object metastore {

  sealed trait MetastoreInitializationFailure {
    def message: String
  }
  final case object MetastoreRequiresInitialization extends MetastoreInitializationFailure {
    def message: String = "MetaStore requires initialization, try running the 'initUpdateMetaStore' command."
  }
  final case class MetastoreRequiresMigration(current: String, latest: String) extends MetastoreInitializationFailure {
    def message: String = "MetaStore schema requires migrating, current version is '$cur' latest version is '$nxt'."
  }
  final case class UnknownInitializationError(causedBy: scala.Throwable) extends MetastoreInitializationFailure {
    private val metastorePrompt: String = "Is the metastore database running?"
    def message: String = s"While verifying MetaStore schema: ${causedBy.getMessage}. $metastorePrompt"
  }

  def verifyMetaStoreSchema[A: Show](schema: Schema[A]): EitherT[ConnectionIO, MetastoreInitializationFailure, Unit] =
    EitherT(schema.updateRequired map {
      case Some((None, _)) =>
        MetastoreRequiresInitialization.left
      case Some((Some(cur), nxt)) =>
        MetastoreRequiresMigration(cur.shows, nxt.shows).left
      case None =>
        ().right
    })

  implicit val aPathMeta: Meta[APath] =
    Meta[String].xmap[APath](
      str => posixCodec.parsePath[APath](
        _ => unexpectedValue(s"absolute path required; found: $str"),
        unsafeSandboxAbs,
        _ => unexpectedValue(s"absolute path required; found: $str"),
        unsafeSandboxAbs
      )(str),
      posixCodec.printPath(_))

  // The `APath` existential type seems to confuse doobie, so here's an alternative:
  implicit val refinedAPathMeta: Meta[ADir \/ AFile] =
    Meta[String].xmap[ADir \/ AFile](
      str => posixCodec.parsePath[ADir \/ AFile](
        _ => unexpectedValue(s"absolute path required; found: $str"),
        unsafeSandboxAbs(_).right,
        _ => unexpectedValue(s"absolute path required; found: $str"),
        unsafeSandboxAbs(_).left
      )(str),
      p => posixCodec.printPath(p.merge[APath]))

  implicit val aDirMeta: Meta[ADir] =
    Meta[String].xmap[ADir](
      str => unsafeSandboxAbs(
        posixCodec.parseAbsDir(str).getOrElse(unexpectedValue("not an absolute dir path: " + str))),
      posixCodec.printPath(_))

  implicit val mountTypeMeta: Meta[MountType] = {
    import MountType._
    Meta[String].xmap[MountType](
      { case "view" => viewMount(); case "module" => moduleMount(); case fsType => fileSystemMount(FileSystemType(fsType)) },
      _.fold(_.value, "view", "module"))
  }

  // See comment above.
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def unexpectedValue[A](msg: String): A = throw new UnexpectedValueException(msg)
}
