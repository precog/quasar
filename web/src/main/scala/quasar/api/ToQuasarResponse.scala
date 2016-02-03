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
import quasar.{EnvironmentError2, Planner}
import quasar.SKI._
import quasar.fs._
import quasar.fs.mount.{Mounting, MountingError}

import argonaut._, Argonaut._
import org.http4s.Status, Status._
import pathy.Path._
import scalaz._, syntax.show._
import scalaz.concurrent.Task
import simulacrum.typeclass

trait ToQuasarResponse[A, S[_]] {
  def toResponse(v: A): QuasarResponse[S]
}

object ToQuasarResponse extends ToQuasarResponseInstances {
  def apply[A, S[_]](implicit ev: ToQuasarResponse[A, S]): ToQuasarResponse[A, S] = ev

  def response[A, S[_]](f: A => QuasarResponse[S]): ToQuasarResponse[A, S] =
    new ToQuasarResponse[A, S] { def toResponse(a: A) = f(a) }
}

abstract class ToQuasarResponseInstances extends ToQuasarResponseInstances0 {
  import ToQuasarResponse._

  implicit def disjunctionQuasarResponse[A, B, S[_]]
    (implicit ev1: ToQuasarResponse[A, S], ev2: ToQuasarResponse[B, S])
    : ToQuasarResponse[A \/ B, S] =
      response(_.fold(ev1.toResponse, ev2.toResponse))

  implicit def environmentErrorQuasarResponse[S[_]]: ToQuasarResponse[EnvironmentError2, S] =
    response(ee => QuasarResponse.error(InternalServerError, ee.shows))

  implicit def fileSystemErrorResponse[S[_]]: ToQuasarResponse[FileSystemError, S] = {
    import FileSystemError._

    response {
      case PathError(e)                => ToQuasarResponse[PathError2, S].toResponse(e)
      case PlannerError(_, e)          => ToQuasarResponse[Planner.PlannerError, S].toResponse(e)
      case UnknownReadHandle(handle)   => QuasarResponse.error(InternalServerError, s"Unknown read handle: $handle")
      case UnknownWriteHandle(handle)  => QuasarResponse.error(InternalServerError, s"Unknown write handle: $handle")
      case UnknownResultHandle(handle) => QuasarResponse.error(InternalServerError, s"Unknown result handle: $handle")
      case PartialWrite(numFailed)     => QuasarResponse.error(InternalServerError, s"Failed to write $numFailed records")
      case WriteFailed(data, reason)   => QuasarResponse.error(InternalServerError, s"Failed to write ${data.shows} because of $reason")
    }
  }

  implicit def mountingErrorResponse[S[_]]: ToQuasarResponse[MountingError, S] = {
    import MountingError._, PathError2.Case.InvalidPath

    response {
      case PathError(InvalidPath(p, rsn)) =>
        QuasarResponse.error(Conflict, s"cannot mount at ${posixCodec.printPath(p)} because $rsn")

      case PathError(e)             => ToQuasarResponse[PathError2, S].toResponse(e)
      case EnvironmentError(e)      => ToQuasarResponse[EnvironmentError2, S].toResponse(e)
      case InvalidConfig(cfg, rsns) => QuasarResponse.error(BadRequest, rsns.list.mkString("; "))
    }
  }

  implicit def mountingPathTypeErrorResponse[S[_]]: ToQuasarResponse[Mounting.PathTypeMismatch, S] =
    response { err =>
      val expectedType = refineType(err.path).fold(κ("file"), κ("directory"))
      QuasarResponse.error(
        BadRequest,
        s"wrong path type for mount: ${posixCodec.printPath(err.path)}; $expectedType path required")
    }

  implicit def pathErrorResponse[S[_]]: ToQuasarResponse[PathError2, S] = {
    import PathError2.Case._

    response {
      case PathExists(path)          => QuasarResponse.error(Conflict, s"${posixCodec.printPath(path)} already exists")
      case PathNotFound(path)        => QuasarResponse.error(NotFound, s"${posixCodec.printPath(path)} doesn't exist")
      case InvalidPath(path, reason) => QuasarResponse.error(BadRequest, s"${posixCodec.printPath(path)} is an invalid path because $reason")
    }
  }

  implicit def plannerErrorQuasarResponse[S[_]]: ToQuasarResponse[Planner.PlannerError, S] =
    response(pe => QuasarResponse.error(BadRequest, pe.shows))
}

abstract class ToQuasarResponseInstances0 {
  implicit def jsonQuasarResponse[A: EncodeJson, S[_]]: ToQuasarResponse[A, S] =
    ToQuasarResponse.response(a => QuasarResponse.json(Ok, a))
}
