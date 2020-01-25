/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.connector

import slamdata.Predef._

import quasar.api.resource._

import monocle.Prism
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._
import scalaz.{Equal, Show}

sealed trait ResourceError extends Product with Serializable {
  def path: ResourcePath
  def detail: Option[String]
  def cause: Option[Throwable]
}

object ResourceError extends ResourceErrorInstances{
  final case class MalformedResource(path: ResourcePath, expectedFormat: String, detail: Option[String], cause: Option[Throwable]) extends ResourceError
  final case class NotAResource(path: ResourcePath) extends ResourceError { val detail = None; val cause = None }
  final case class ConnectionFailed(path: ResourcePath, detail: Option[String], cause: Option[Throwable]) extends ResourceError
  final case class AccessDenied(path: ResourcePath, detail: Option[String], cause: Option[Throwable]) extends ResourceError
  sealed trait ExistentialError extends ResourceError
  final case class PathNotFound(path: ResourcePath) extends ExistentialError { val detail = None; val cause = None }

  val malformedResource: Prism[ResourceError, (ResourcePath, String, Option[String], Option[Throwable])] =
    Prism.partial[ResourceError, (ResourcePath, String, Option[String], Option[Throwable])] {
      case MalformedResource(p, e, d, t) => (p, e, d, t)
    } (MalformedResource.tupled)

  val notAResource: Prism[ResourceError, ResourcePath] =
    Prism.partial[ResourceError, ResourcePath] {
      case NotAResource(p) => p
    } (NotAResource(_))

  val connectionFailed: Prism[ResourceError, (ResourcePath, Option[String], Option[Throwable])] =
    Prism.partial[ResourceError, (ResourcePath, Option[String], Option[Throwable])] {
      case ConnectionFailed(p, d, t) => (p, d, t)
    } (ConnectionFailed.tupled)

  val accessDenied: Prism[ResourceError, (ResourcePath, Option[String], Option[Throwable])] =
    Prism.partial[ResourceError, (ResourcePath, Option[String], Option[Throwable])] {
      case AccessDenied(p, d, t) => (p, d, t)
    } (AccessDenied.tupled)

  def pathNotFound[E >: ExistentialError <: ResourceError]: Prism[E, ResourcePath] =
    Prism.partial[E, ResourcePath] {
      case PathNotFound(p) => p
    } (PathNotFound(_))

  val throwableP: Prism[Throwable, ResourceError] =
    Prism.partial[Throwable, ResourceError] {
      case ResourceErrorException(re) => re
    } (ResourceErrorException(_))

  ////

  private final case class ResourceErrorException(err: ResourceError)
      extends Exception(err.shows)
}

sealed abstract class ResourceErrorInstances {
  implicit val equal: Equal[ResourceError] = {
    implicit val ignoreThrowable: Equal[Throwable] =
      Equal.equal((_, _) => true)

    Equal.equalBy(e => (
      ResourceError.notAResource.getOption(e),
      ResourceError.pathNotFound.getOption(e),
      ResourceError.malformedResource.getOption(e),
      ResourceError.connectionFailed.getOption(e),
      ResourceError.accessDenied.getOption(e)))
  }

  def printThrowable(throwable: Option[Throwable]): String = throwable match {
    case None => ""
    case Some(t) => s"\n$t"
  }

  implicit val show: Show[ResourceError] =
    Show.shows {
      case ResourceError.NotAResource(p) =>
        "NotAResource(" + p.shows + ")"

      case ResourceError.PathNotFound(p) =>
        "PathNotFound(" + p.shows + ")"

      case ResourceError.MalformedResource(p, e, d, t) =>
        s"MalformedResource(path: ${p.show}, expected: $e, detail: ${d.show})${printThrowable(t)}"

      case ResourceError.ConnectionFailed(p, d, t) =>
        s"ConnectionFailed(path: ${p.show}, detail: ${d.show})${printThrowable(t)}"

      case ResourceError.AccessDenied(p, d, t) =>
        s"AccessDenied(path: ${p.show}, detail: ${d.show})${printThrowable(t)}"
    }
}
