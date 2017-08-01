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

package quasar.yggdrasil.vfs

import quasar.blueeyes.json.serialization.Extractor

import scalaz.{NonEmptyList, Semigroup, Show}
import scalaz.NonEmptyList.nels

sealed trait ResourceError {
  def fold[A](fatalError: ResourceError.FatalError => A, userError: ResourceError.UserError => A): A
  def messages: NonEmptyList[String]
}

object ResourceError {
  implicit val semigroup = new Semigroup[ResourceError] {
    def append(e1: ResourceError, e2: => ResourceError) = all(nels(e1, e2))
  }

  implicit val show = Show.showFromToString[ResourceError]

  def corrupt(message: String): ResourceError with FatalError         = Corrupt(message)
  def ioError(ex: Throwable): ResourceError with FatalError           = IOError(ex)
  def permissionsError(message: String): ResourceError with UserError = PermissionsError(message)
  def notFound(message: String): ResourceError with UserError         = NotFound(message)

  def all(errors: NonEmptyList[ResourceError]): ResourceError with FatalError with UserError = new ResourceErrors(
    errors flatMap {
      case ResourceErrors(e0) => e0
      case other              => NonEmptyList(other)
    }
  )

  def fromExtractorError(msg: String): Extractor.Error => ResourceError = { error =>
    Corrupt("%s:\n%s" format (msg, error.message))
  }

  sealed trait FatalError { self: ResourceError =>
    def fold[A](fatalError: FatalError => A, userError: UserError => A) = fatalError(self)
    def messages: NonEmptyList[String]
  }

  sealed trait UserError { self: ResourceError =>
    def fold[A](fatalError: FatalError => A, userError: UserError => A) = userError(self)
    def messages: NonEmptyList[String]
  }

  case class Corrupt(message: String) extends ResourceError with FatalError {
    def messages = nels(message)
  }
  case class IOError(exception: Throwable) extends ResourceError with FatalError {
    def messages = nels(Option(exception.getMessage).getOrElse(exception.getClass.getName))
  }

  case class IllegalWriteRequestError(message: String) extends ResourceError with UserError {
    def messages = nels(message)
  }
  case class PermissionsError(message: String) extends ResourceError with UserError {
    def messages = nels(message)
  }

  case class NotFound(message: String) extends ResourceError with UserError {
    def messages = nels(message)
  }

  case class ResourceErrors private[ResourceError] (errors: NonEmptyList[ResourceError]) extends ResourceError with FatalError with UserError { self =>
    override def fold[A](fatalError: FatalError => A, userError: UserError => A) = {
      val hasFatal = errors.list.toList.exists(_.fold(_ => true, _ => false))
      if (hasFatal) fatalError(self) else userError(self)
    }

    def messages: NonEmptyList[String] = errors.flatMap(_.messages)
  }
}
