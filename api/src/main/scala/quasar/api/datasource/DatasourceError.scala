/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.api.datasource

import slamdata.Predef._
import quasar.api.resource.ResourcePath

import monocle.Prism
import scalaz.{Cord, Equal, ISet, NonEmptyList, Show}
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._

sealed trait DatasourceError[+I, +C] extends Product with Serializable

object DatasourceError extends DatasourceErrorInstances {
  sealed trait CreateError[+C] extends DatasourceError[Nothing, C]

  final case class DatasourceUnsupported(kind: DatasourceType, supported: ISet[DatasourceType])
      extends CreateError[Nothing]

  final case class DatasourceNameExists(name: DatasourceName)
      extends CreateError[Nothing]

  sealed trait InitializationError[+C] extends CreateError[C]

  final case class MalformedConfiguration[C](kind: DatasourceType, config: C, reason: String)
      extends InitializationError[C]

  final case class InvalidConfiguration[C](kind: DatasourceType, config: C, reasons: NonEmptyList[String])
      extends InitializationError[C]

  final case class ConnectionFailed[C](kind: DatasourceType, config: C, cause: Exception)
    extends InitializationError[C]

  final case class AccessDenied[C](kind: DatasourceType, config: C, reason: String)
    extends InitializationError[C]

  sealed trait DiscoveryError[+I] extends DatasourceError[I, Nothing]

  final case class PathNotFound(path: ResourcePath)
    extends DiscoveryError[Nothing]

  final case class PathNotAResource(path: ResourcePath)
    extends DiscoveryError[Nothing]

  sealed trait ExistentialError[+I] extends DiscoveryError[I]

  final case class DatasourceNotFound[I](datasourceId: I)
      extends ExistentialError[I]

  def connectionFailed[C, E >: InitializationError[C] <: DatasourceError[_, C]]
      : Prism[E, (DatasourceType, C, Exception)] =
    Prism.partial[E, (DatasourceType, C, Exception)] {
      case ConnectionFailed(k, c, e) => (k, c, e)
    } ((ConnectionFailed[C](_, _, _)).tupled)

  def datasourceNameExists[E >: CreateError[Nothing] <: DatasourceError[_, _]]
      : Prism[E, DatasourceName] =
    Prism.partial[E, DatasourceName] {
      case DatasourceNameExists(n) => n
    } (DatasourceNameExists(_))

  def datasourceNotFound[I, E >: ExistentialError[I] <: DatasourceError[I, _]]
      : Prism[E, I] =
    Prism.partial[E, I] {
      case DatasourceNotFound(id) => id
    } (DatasourceNotFound(_))

  def datasourceUnsupported[E >: CreateError[Nothing] <: DatasourceError[_, _]]
      : Prism[E, (DatasourceType, ISet[DatasourceType])] =
    Prism.partial[E, (DatasourceType, ISet[DatasourceType])] {
      case DatasourceUnsupported(k, s) => (k, s)
    } (DatasourceUnsupported.tupled)

  def invalidConfiguration[C, E >: InitializationError[C] <: DatasourceError[_, C]]
      : Prism[E, (DatasourceType, C, NonEmptyList[String])] =
    Prism.partial[E, (DatasourceType, C, NonEmptyList[String])] {
      case InvalidConfiguration(t, c, rs) => (t, c, rs)
    } ((InvalidConfiguration[C](_, _, _)).tupled)

  def accessDenied[C, E >: InitializationError[C] <: DatasourceError[_, C]]
      : Prism[E, (DatasourceType, C, String)] =
    Prism.partial[E, (DatasourceType, C, String)] {
      case AccessDenied(t, c, r) => (t, c, r)
    } ((AccessDenied[C](_, _, _)).tupled)

  def malformedConfiguration[C, E >: InitializationError[C] <: DatasourceError[_, C]]
      : Prism[E, (DatasourceType, C, String)] =
    Prism.partial[E, (DatasourceType, C, String)] {
      case MalformedConfiguration(t, c, r) => (t, c, r)
    } ((MalformedConfiguration[C](_, _, _)).tupled)

  def pathNotAResource[E >: DiscoveryError[Nothing] <: DatasourceError[_, _]]
      : Prism[E, ResourcePath] =
    Prism.partial[E, ResourcePath] {
      case PathNotAResource(p) => p
    } (PathNotAResource(_))

  def pathNotFound[E >: DiscoveryError[Nothing] <: DatasourceError[_, _]]
      : Prism[E, ResourcePath] =
    Prism.partial[E, ResourcePath] {
      case PathNotFound(p) => p
    } (PathNotFound(_))
}

sealed abstract class DatasourceErrorInstances {
  import DatasourceError._

  implicit def equal[I: Equal, C: Equal]: Equal[DatasourceError[I, C]] = {
    implicit val ignoreExceptions: Equal[Exception] =
      Equal.equal((_, _) => true)

    Equal.equalBy { de => (
      connectionFailed[C, DatasourceError[I, C]].getOption(de),
      datasourceNameExists[DatasourceError[I, C]].getOption(de),
      datasourceNotFound[I, DatasourceError[I, C]].getOption(de),
      datasourceUnsupported[DatasourceError[I, C]].getOption(de),
      invalidConfiguration[C, DatasourceError[I, C]].getOption(de),
      malformedConfiguration[C, DatasourceError[I, C]].getOption(de),
      pathNotAResource[DatasourceError[I, C]].getOption(de),
      pathNotFound[DatasourceError[I, C]].getOption(de)
    )}
  }

  implicit def show[I: Show, C: Show]: Show[DatasourceError[I, C]] =
    Show.show {
      case e: CreateError[C]    => showCreateError[C].show(e)
      case e: DiscoveryError[I] => showDiscoveryError[I].show(e)
    }

  implicit def showExistentialError[I: Show]: Show[ExistentialError[I]] =
    Show.show {
      case DatasourceNotFound(i) =>
        Cord("DatasourceNotFound(") ++ i.show ++ Cord(")")
    }

  implicit def showDiscoveryError[I: Show]: Show[DiscoveryError[I]] =
    Show.show {
      case PathNotAResource(p) =>
        Cord("PathNotAResource(") ++ p.show ++ Cord(")")

      case PathNotFound(p) =>
        Cord("PathNotFound(") ++ p.show ++ Cord(")")

      case e: ExistentialError[I] =>
        showExistentialError[I].show(e)
    }

  implicit def showCreateError[C: Show]: Show[CreateError[C]] =
    Show.show {
      case DatasourceNameExists(n) =>
        Cord("DatasourceNameExists(") ++ n.show ++ Cord(")")

      case DatasourceUnsupported(k, s) =>
        Cord("DatasourceUnsupported(") ++ k.show ++ Cord(", ") ++ s.show ++ Cord(")")

      case InvalidConfiguration(k, c, rs) =>
        Cord("InvalidConfiguration(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ rs.show ++ Cord(")")

      case MalformedConfiguration(k, c, r) =>
        Cord("MalformedConfiguration(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ r.show ++ Cord(")")

      case ConnectionFailed(k, c, e) =>
        Cord("ConnectionFailed(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(s")\n\n$e")

      case AccessDenied(k, c, r) =>
        Cord("AccessDenied(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ r.show ++ Cord(")")
    }
}
