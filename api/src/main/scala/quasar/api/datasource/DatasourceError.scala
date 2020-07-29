/*
 * Copyright 2020 Precog Data
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

import cats.{Eq, Show}
import cats.instances.option._
import cats.instances.string._
import cats.instances.tuple._
import cats.instances.unit._
import cats.syntax.show._

import monocle.Prism

import scalaz.{ISet, NonEmptyList}

import shims.{eqToScalaz, equalToCats, showToCats, showToScalaz}

sealed trait DatasourceError[+I, +C] extends Product with Serializable

object DatasourceError extends DatasourceErrorInstances {
  sealed trait CreateError[+C] extends DatasourceError[Nothing, C]

  final case class DatasourceUnsupported(kind: DatasourceType, supported: ISet[DatasourceType])
      extends CreateError[Nothing]

  final case class DatasourceNameExists(name: DatasourceName)
      extends CreateError[Nothing]

  sealed trait InitializationError[+C] extends CreateError[C]

  final case class ConnectionFailed[C](kind: DatasourceType, config: C, cause: Exception)
    extends InitializationError[C]

  final case class AccessDenied[C](kind: DatasourceType, config: C, reason: String)
    extends InitializationError[C]

  sealed trait ConfigurationError[C] extends InitializationError[C]

  final case class MalformedConfiguration[C](kind: DatasourceType, config: C, reason: String)
      extends ConfigurationError[C]

  final case class InvalidConfiguration[C](kind: DatasourceType, config: C, reasons: NonEmptyList[String])
      extends ConfigurationError[C]

  sealed trait ExistentialError[+I] extends DatasourceError[I, Nothing]

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

  def invalidConfiguration[C, E >: InitializationError[C] <: DatasourceError[_, C]]
      : Prism[E, (DatasourceType, C, NonEmptyList[String])] =
    Prism.partial[E, (DatasourceType, C, NonEmptyList[String])] {
      case InvalidConfiguration(t, c, rs) => (t, c, rs)
    } ((InvalidConfiguration[C](_, _, _)).tupled)
}

sealed abstract class DatasourceErrorInstances {
  import DatasourceError._

  implicit def equal[I: Eq, C: Eq]: Eq[DatasourceError[I, C]] = {
    implicit val ignoreExceptions: Eq[Exception] =
      Eq.by(_ => ())

    Eq by { de => (
      connectionFailed[C, DatasourceError[I, C]].getOption(de),
      datasourceNameExists[DatasourceError[I, C]].getOption(de),
      datasourceNotFound[I, DatasourceError[I, C]].getOption(de),
      datasourceUnsupported[DatasourceError[I, C]].getOption(de),
      invalidConfiguration[C, DatasourceError[I, C]].getOption(de),
      malformedConfiguration[C, DatasourceError[I, C]].getOption(de)
    )}
  }

  implicit def show[I: Show, C: Show]: Show[DatasourceError[I, C]] =
    Show show {
      case e: CreateError[C] => showCreateError[C].show(e)
      case e: ExistentialError[I] => showExistentialError[I].show(e)
    }

  implicit def showExistentialError[I: Show]: Show[ExistentialError[I]] =
    Show show {
      case DatasourceNotFound(i) =>
        "DatasourceNotFound(" + i.show + ")"
    }

  implicit def showCreateError[C: Show]: Show[CreateError[C]] =
    Show show {
      case DatasourceNameExists(n) =>
        "DatasourceNameExists(" + n.show + ")"

      case DatasourceUnsupported(k, s) =>
        "DatasourceUnsupported(" + k.show + ", " + s.show + ")"

      case InvalidConfiguration(k, c, rs) =>
        "InvalidConfiguration(" + k.show + ", " + c.show + ", " + rs.show + ")"

      case MalformedConfiguration(k, c, r) =>
        "MalformedConfiguration(" + k.show + ", " + c.show + ", " + r.show + ")"

      case ConnectionFailed(k, c, e) =>
        "ConnectionFailed(" + k.show + ", " + c.show + s")\n\n$e"

      case AccessDenied(k, c, r) =>
        "AccessDenied(" + k.show + ", " + c.show + ", " + r.show + ")"
    }
}
