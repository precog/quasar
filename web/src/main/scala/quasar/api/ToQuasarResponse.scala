/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.api

import quasar.Predef._
import quasar.fs._

import argonaut._, Argonaut._
import org.http4s.Status, Status._
import pathy.Path._
import scalaz._, syntax.show._
import scalaz.concurrent.Task
import simulacrum.typeclass

trait ToQuasarResponse[A, S[_]] {
  def toResponse(v: A): QuasarResponse[S]
}

object ToQuasarResponse {

  def apply[A, S[_]](implicit ev: ToQuasarResponse[A, S]): ToQuasarResponse[A, S] = ev

  implicit def jsonQuasarResponse[A: EncodeJson, S[_]]: ToQuasarResponse[A, S] =
    new ToQuasarResponse[A, S] {
      def toResponse(v: A) = QuasarResponse.json(Ok, v)
    }

  implicit def disjunctionQuasarResponse[A, B, S[_]]
    (implicit ev1: ToQuasarResponse[A, S], ev2: ToQuasarResponse[B, S])
    : ToQuasarResponse[A \/ B, S] =
    new ToQuasarResponse[A \/ B, S] {
      def toResponse(v: A \/ B) = v.fold(ev1.toResponse, ev2.toResponse)
    }

  implicit def fileSystemErrorResponse[S[_]]: ToQuasarResponse[FileSystemError, S] =
    new ToQuasarResponse[FileSystemError, S] {
      import FileSystemError._

      def toResponse(v: FileSystemError): QuasarResponse[S] = v match {
        case PathError(e)                => ToQuasarResponse[PathError2, S].toResponse(e)
        case PlannerError(_, _)          => QuasarResponse.error(BadRequest, v.shows)
        case UnknownReadHandle(handle)   => QuasarResponse.error(InternalServerError, s"Unknown read handle: $handle")
        case UnknownWriteHandle(handle)  => QuasarResponse.error(InternalServerError, s"Unknown write handle: $handle")
        case UnknownResultHandle(handle) => QuasarResponse.error(InternalServerError, s"Unknown result handle: $handle")
        case PartialWrite(numFailed)     => QuasarResponse.error(InternalServerError, s"Failed to write $numFailed records")
        case WriteFailed(data, reason)   => QuasarResponse.error(InternalServerError, s"Failed to write ${data.shows} because of $reason")
      }
    }

  implicit def pathErrorResponse[S[_]]: ToQuasarResponse[PathError2, S] =
    new ToQuasarResponse[PathError2, S] {
      import PathError2.Case._

      def toResponse(v: PathError2): QuasarResponse[S] = v match {
        case PathExists(path) => QuasarResponse.error(Conflict, s"${posixCodec.printPath(path)} already exists")
        case PathNotFound(path) => QuasarResponse.error(NotFound, s"${posixCodec.printPath(path)} doesn't exist")
        case InvalidPath(path, reason) => QuasarResponse.error(BadRequest, s"${posixCodec.printPath(path)} is an invalid path because $reason")
      }
    }

}
