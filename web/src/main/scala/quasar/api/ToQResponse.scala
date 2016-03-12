/*
 * Copyright 2014–2016 SlamData Inc.
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
import quasar.{EnvironmentError, Planner, SemanticErrors}
import quasar.fs._
import quasar.fs.mount.{Mounting, MountingError}
import quasar.fs.mount.hierarchical.HierarchicalFileSystemError
import quasar.fp._
import quasar.physical.mongodb.WorkflowExecutionError
import quasar.sql.ParsingError

import argonaut._, Argonaut._
import com.mongodb.MongoException
import org.http4s._, Status._
import pathy.Path._
import scalaz._, syntax.show._
import scalaz.concurrent.Task

trait ToQResponse[A, S[_]] {
  def toResponse(v: A): QResponse[S]
}

object ToQResponse extends ToQResponseInstances {
  def apply[A, S[_]](implicit ev: ToQResponse[A, S]): ToQResponse[A, S] = ev

  def response[A, S[_]](f: A => QResponse[S]): ToQResponse[A, S] =
    new ToQResponse[A, S] { def toResponse(a: A) = f(a) }

  object ops {
    final implicit class ToQResponseOps[A](val a: A) extends scala.AnyVal {
      def toResponse[S[_]](implicit A: ToQResponse[A, S]): QResponse[S] =
        A.toResponse(a)
    }
  }
}

sealed abstract class ToQResponseInstances extends ToQResponseInstances0 {
  import ToQResponse._, ops._

  implicit def disjunctionQResponse[A, B, S[_]]
    (implicit ev1: ToQResponse[A, S], ev2: ToQResponse[B, S])
    : ToQResponse[A \/ B, S] =
      response(_.fold(ev1.toResponse, ev2.toResponse))

  implicit def environmentErrorQResponse[S[_]]: ToQResponse[EnvironmentError, S] =
    response(ee => QResponse.error(InternalServerError, ee.shows))

  implicit def fileSystemErrorResponse[S[_]]: ToQResponse[FileSystemError, S] = {
    import FileSystemError._

    response {
      case PathError(e)                => e.toResponse
      case PlannerError(_, e)          => e.toResponse
      case UnknownReadHandle(handle)   => QResponse.error(InternalServerError, s"Unknown read handle: $handle")
      case UnknownWriteHandle(handle)  => QResponse.error(InternalServerError, s"Unknown write handle: $handle")
      case UnknownResultHandle(handle) => QResponse.error(InternalServerError, s"Unknown result handle: $handle")
      case PartialWrite(numFailed)     => QResponse.error(InternalServerError, s"Failed to write $numFailed records")
      case WriteFailed(data, reason)   => QResponse.error(InternalServerError, s"Failed to write ${data.shows} because of $reason")
    }
  }

  implicit def hierarchicalFileSystemErrorToQResponse[S[_]]: ToQResponse[HierarchicalFileSystemError, S] = {
    import HierarchicalFileSystemError._

    response {
      case MultipleMountsApply(_, _) =>
        QResponse.error(BadRequest, "The request could not be handled by a unique mounted filesystem.")
    }
  }

  implicit def mountingErrorResponse[S[_]]: ToQResponse[MountingError, S] = {
    import MountingError._, PathError2.InvalidPath

    response {
      case PError(InvalidPath(p, rsn)) =>
        QResponse.error(Conflict, s"cannot mount at ${posixCodec.printPath(p)} because $rsn")

      case PError(e)                => e.toResponse
      case EError(e)                => e.toResponse
      case InvalidConfig(cfg, rsns) => QResponse.error(BadRequest, rsns.list.mkString("; "))
    }
  }

  implicit def mountingPathTypeErrorResponse[S[_]]: ToQResponse[Mounting.PathTypeMismatch, S] =
    response { err =>
      val expectedType = refineType(err.path).fold(κ("file"), κ("directory"))
      QResponse.error(
        BadRequest,
        s"wrong path type for mount: ${posixCodec.printPath(err.path)}; $expectedType path required")
    }

  implicit def pathErrorResponse[S[_]]: ToQResponse[PathError2, S] = {
    import PathError2._

    response {
      case PathExists(path)          => QResponse.error(Conflict, s"${posixCodec.printPath(path)} already exists")
      case PathNotFound(path)        => QResponse.error(NotFound, s"${posixCodec.printPath(path)} doesn't exist")
      case InvalidPath(path, reason) => QResponse.error(BadRequest, s"${posixCodec.printPath(path)} is an invalid path because $reason")
    }
  }

  implicit def parsingErrorQResponse[S[_]]: ToQResponse[ParsingError, S] =
    response(pe => QResponse.error(BadRequest, pe.message))

  implicit def parseFailureQResponse[S[_]]: ToQResponse[ParseFailure, S] =
    response(pf => QResponse.error(BadRequest, pf.sanitized))

  implicit def plannerErrorQResponse[S[_]]: ToQResponse[Planner.PlannerError, S] =
    response(pe => QResponse.error(BadRequest, pe.shows))

  implicit def semanticErrorQResponse[S[_]]: ToQResponse[SemanticErrors, S] =
    response(se => QResponse.error(BadRequest, se.shows))

  implicit def workflowExecutionError[S[_]]: ToQResponse[WorkflowExecutionError, S] = {
    import WorkflowExecutionError._

    response(err => err match {
      case InvalidTask(_, _) =>
        QResponse.error(InternalServerError, err.shows)

      case InsertFailed(_, _) =>
        QResponse.error(BadRequest, err.shows)

      case NoDatabase =>
        QResponse.error(InternalServerError, err.shows)
    })
  }

  implicit def qResponseToQResponse[S[_]]: ToQResponse[QResponse[S], S] =
    response(ι)

  implicit def http4sResponseToQResponse[S[_]: Functor](implicit ev: Task :<: S): ToQResponse[Response, S] =
    response(r =>
      QResponse(
        status = r.status,
        headers = r.headers,
        body = r.body.translate[Free[S, ?]](injectFT[Task, S])))

  implicit def decodeFailureToQResponse[S[_]]: ToQResponse[DecodeFailure, S] =
    response{
      case MediaTypeMissing(expectedMediaTypes) =>
        val expected = expectedMediaTypes.map(_.renderString).mkString(", ")
        QResponse.json(
          UnsupportedMediaType,
          Json(
            "error" := s"Request has no media type. Please specify a media type in the following ranges: $expected",
            "supported media types" := jArray(expectedMediaTypes.map(m => jString(m.renderString)).toList)))
      case MediaTypeMismatch(messageType, expectedMediaTypes) =>
        val actual = messageType.renderString
        val expected = expectedMediaTypes.map(_.renderString).mkString(", ")
        QResponse.json(
          UnsupportedMediaType,
          Json(
            "error" := s"Request has an unsupported media type. Please specify a media type in the following ranges: $expected",
            "supported media types" := jArray(expectedMediaTypes.map(m => jString(m.renderString)).toList)))
      case other =>
        QResponse.error(BadRequest, other.msg)
    }

  implicit def mongoExceptionToQResponse[S[_]]: ToQResponse[MongoException, S] =
    response(merr => QResponse.error(InternalServerError, s"MongoDB Error: ${merr.getMessage}"))

  implicit def stringQResponse[S[_]]: ToQResponse[String, S] =
    response(QResponse.string(Ok, _))

  implicit def unitQResponse[S[_]]: ToQResponse[Unit, S] =
    response(κ(QResponse.empty[S]))
}

sealed abstract class ToQResponseInstances0 {
  implicit def jsonQResponse[A: EncodeJson, S[_]]: ToQResponse[A, S] =
    ToQResponse.response(a => QResponse.json(Ok, a))
}
