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

package quasar.api

import slamdata.Predef._
import quasar.{Data, DataCodec, Planner, SemanticError}
import quasar.RenderTree.ops._
import quasar.connector.EnvironmentError
import quasar.fp._
import quasar.fp.ski._
import quasar.fs._
import quasar.fs.mount.{Mounting, MountingError}
import quasar.fs.mount.module.Module
import quasar.sql._

import argonaut._, Argonaut._
import argonaut.ArgonautScalaz._
import org.http4s._, Status._
import pathy.Path._
import pathy.argonaut.PosixCodecJson._
import scalaz.{NonEmptyList, Scalaz}, Scalaz._

abstract class ToApiError[A] {
  def toApiError(a: A): ApiError
}

object ToApiError extends ToApiErrorInstances {
  def apply[A](implicit A: ToApiError[A]): ToApiError[A] = A

  def error[A](f: A => ApiError): ToApiError[A] =
    new ToApiError[A] { def toApiError(a: A) = f(a) }

  object ops {
    final implicit class ToApiErrorOps[A](val a: A) extends scala.AnyVal {
      def toApiError(implicit A: ToApiError[A]): ApiError =
        A.toApiError(a)
    }
  }
}

sealed abstract class ToApiErrorInstances extends ToApiErrorInstances0 {
  import ToApiError._, ops._
  import ApiError._
  import ReadFile.ReadHandle
  import WriteFile.WriteHandle
  import QueryFile.ResultHandle

  implicit def environmentErrorQResponse: ToApiError[EnvironmentError] = {
    import EnvironmentError._
    error {
      case ConnectionFailed(msg) =>
        fromMsg_(
          InternalServerError withReason "Connection to backend failed.",
          s"Connection failed: $msg.")

      case InvalidCredentials(msg) =>
        fromMsg_(
          InternalServerError withReason "Invalid backend credentials.",
          s"Invalid credentials: $msg")

      case UnsupportedVersion(name, ver) =>
        apiError(
          InternalServerError withReason s"Unsupported $name version.",
          "backendName" := name,
          "version"     := ver)
    }
  }

  implicit def fileSystemErrorResponse: ToApiError[FileSystemError] = {
    import FileSystemError._
    error {
      case ExecutionFailed(lp, reason, det, cause) =>
        fromMsg(
          InternalServerError withReason "Failed to execute SQL^2 query.",
          reason,
          det.toList : _*)            :+
        ("logicalplan" :=  lp.render) :?+
        ("cause"       :?= cause.map(_.shows))
      case PathErr(e) =>
        e.toApiError
      case PlanningFailed(lp, e) =>
        e.toApiError :+ ("logicalplan" := lp.render)
      case QScriptPlanningFailed(e) =>
        e.toApiError
      case UnknownReadHandle(ReadHandle(path, id)) =>
        apiError(
          InternalServerError withReason "Unknown read handle.",
          "path"     := path,
          "handleId" := id)
      case UnknownWriteHandle(WriteHandle(path, id)) =>
        apiError(
          InternalServerError withReason "Unknown write handle.",
          "path"     := path,
          "handleId" := id)
      case UnknownResultHandle(ResultHandle(id)) =>
        apiError(
          InternalServerError withReason "Unknown result handle.",
          "handleId" := id)
      case ReadFailed(data, reason) =>
        fromMsg_(
          InternalServerError withReason "Failed to read data.",
          s"Failed to read data: $reason."
        ) :+ ("data" := data)
      case PartialWrite(numFailed) =>
        apiError(
          InternalServerError withReason "Failed to write some values.",
          "failedCount" := numFailed)
      case WriteFailed(data, reason) =>
        fromMsg_(
          InternalServerError withReason "Failed to write data.",
          s"Failed to write data: $reason."
        ) :?+ ("data" :?= encodeData(data))
    }
  }

  implicit def moduleErrorToApiError: ToApiError[Module.Error] =
    error {
      case Module.Error.FSError(fsErr)            => fsErr.toApiError
      case Module.Error.SemErrors(semErrs)        => semErrs.toApiError
      case Module.Error.ArgumentsMissing(missing) =>
        apiError(
          BadRequest withReason "Arguments missing to function call",
          "missing arguments" := missing)
      case Module.Error.ParsingErr(parsingErr)    => parsingErr.toApiError
    }

  implicit def physicalErrorToApiError: ToApiError[PhysicalError] =
    error(err => fromMsg_(
      InternalServerError withReason "Physical filesystem error.",
      err.shows))

  implicit def mountingErrorToApiError: ToApiError[MountingError] = {
    import MountingError._, PathError.InvalidPath
    error {
      case PError(InvalidPath(p, rsn)) =>
        fromMsg(
          Conflict withReason "Unable to mount at path.",
          s"Unable to mount at ${posixCodec.printPath(p)} because $rsn",
          "path" := p)

      case InvalidConfig(cfg, rsns) =>
        apiError(
          BadRequest withReason "Invalid mount configuration.",
          "reasons" := rsns)

      case InvalidMount(_, e) =>
        fromMsg_(InternalServerError withReason "Invalid mount.", e)

      case PError(e) => e.toApiError
      case EError(e) => e.toApiError
    }
  }

  implicit def mountingPathTypeErrorToApiError: ToApiError[Mounting.PathTypeMismatch] =
    error { err =>
      val expectedType = refineType(err.path).fold(κ("file"), κ("directory"))
      fromMsg(
        BadRequest withReason "Incorrect path type.",
        s"Incorrect path type, expected a $expectedType.",
        "path" := err.path)
    }

  implicit def pathErrorToApiError: ToApiError[PathError] = {
    import PathError._
    error {
      case PathExists(path) =>
        apiError(Conflict withReason "Path exists.", "path" := path)

      case PathNotFound(path) =>
        apiError(NotFound withReason "Path not found.", "path" := path)

      case InvalidPath(path, reason) =>
        fromMsg(
          BadRequest withReason "Invalid path.",
          s"Invalid path: $reason.",
          "path" := path)
    }
  }

  implicit def parsingErrorToApiError: ToApiError[ParsingError] =
    error {
      case GenericParsingError(msg) =>
        fromMsg_(BadRequest withReason "Malformed SQL^2 query.", msg)

      case ParsingPathError(e) =>
        e.toApiError
    }

  implicit def plannerErrorToApiError: ToApiError[Planner.PlannerError] = {
    import Planner._
    error(err => err match {
      case NonRepresentableData(data) =>
        fromMsg_(
          InternalServerError withReason "Unsupported constant.",
          err.message
        ) :?+ ("data" :?= encodeData(data))
      case NonRepresentableEJson(_) =>
        fromMsg_(
          InternalServerError withReason "Unsupported constant.",
          err.message)
      case UnsupportedFunction(fn, _) =>
        fromMsg(
          InternalServerError withReason "Unsupported function.",
          err.message,
          "functionName" := fn.shows)
      case PlanPathError(e) =>
        e.toApiError
      case UnsupportedJoinCondition(cond) =>
        fromMsg(
          InternalServerError withReason "Unsupported join condition.",
          err.message,
          "joinCondition" := cond.render)
      case UnsupportedPlan(lp, hint) =>
        fromMsg(
          InternalServerError withReason "Unsupported query plan.",
          err.message,
          "term" := lp.void.render.shows
        ) :?+ ("reason" :?= hint)
      case FuncApply(fn, exp, act) =>
        fromMsg(
          BadRequest withReason "Illegal function argument.",
          err.message,
          "functionName" := fn.shows,
          "expectedArg"  := exp,
          "actualArg"    := act)
      case ObjectIdFormatError(oid) =>
        fromMsg(
          BadRequest withReason "Invalid ObjectId.",
          err.message,
          "objectId" := oid)
      case CompilationFailed(semErrs) =>
        fromMsg(
          BadRequest withReason "Compilation failed",
          err.message,
          "compilation errors" := semErrs.map(_.toApiError)
        )
      case NonRepresentableInJS(value) =>
        fromMsg(
          InternalServerError withReason "Unable to compile to JavaScript.",
          err.message,
          "value" := value)
      case UnsupportedJS(value) =>
        fromMsg(
          InternalServerError withReason "Unsupported JavaScript in query plan.",
          err.message,
          "value" := value)
      case InternalError(msg, cause) =>
        fromMsg(
          InternalServerError withReason "Failed to plan query.",
          msg
        ) :?+ ("cause" :?= cause.map(_.toString))
      case UnboundVariable(v) =>
        fromMsg_(
          InternalServerError withReason "Unbound variable.", v.toString)
      case NoFilesFound(dirs) =>
        fromMsg(
          BadRequest withReason "No files to read from.",
          err.message,
          "dirs" := dirs.map(posixCodec.printPath))
    })
  }

  implicit def semanticErrorToApiError: ToApiError[SemanticError] = {
    import SemanticError._
    error(err => err match {
      case GenericError(msg) =>
        fromMsg_(BadRequest withReason "Error in query.", msg)
      case DomainError(data, _) =>
        fromMsg_(
          BadRequest withReason "Illegal argument.",
          err.message
        ) :?+ ("data" :?= encodeData(data))
      case FunctionNotFound(name) =>
        fromMsg(
          BadRequest withReason "Unknown function.",
          err.message,
          "functionName" := name)
      case TypeError(exp, act, _) =>
        fromMsg(
          BadRequest withReason "Type error.",
          err.message,
          "expectedType" := exp,
          "actualType"   := act)
      case VariableParseError(vname, vval, cause) =>
        fromMsg(
          BadRequest withReason "Malformed query variable.",
          err.message,
          "varName"  := vname.value,
          "varValue" := vval.value,
          "cause"    := cause.toApiError)
      case UnboundVariable(vname) =>
        fromMsg(
          BadRequest withReason "Unbound variable.",
          err.message,
          "varName" := vname.value)
      case DuplicateRelationName(name) =>
        fromMsg(
          BadRequest withReason "Duplicate relation name.",
          err.message,
          "relName" := name)
      case NoTableDefined(node) =>
        fromMsg(
          BadRequest withReason "No table defined.",
          err.message,
          "sql" := node.render)
      case MissingField(name) =>
        fromMsg(
          BadRequest withReason "Missing field.",
          err.message,
          "fieldName" := name)
      case DuplicateAlias(name) =>
        fromMsg(
          BadRequest withReason "Duplicate alias name.",
          err.message,
          "name" := name)
      case MissingIndex(i) =>
        fromMsg(
          BadRequest withReason "No element at index.",
          err.message,
          "index" := i)
      case WrongArgumentCount(fn, exp, act) =>
        fromMsg(
          BadRequest withReason "Wrong number of arguments to function.",
          err.message,
          "functionName" := fn,
          "expectedArgs" := exp,
          "actualArgs"   := act)
      case AmbiguousReference(expr, _) =>
        fromMsg(
          BadRequest withReason "Ambiguous table reference.",
          err.message,
          "sql" := expr.render)
      case DateFormatError(fn, str, _) =>
        fromMsg(
          BadRequest withReason "Malformed date/time string.",
          err.message,
          "functionName" := fn.shows,
          "input"        := str)
      case e@AmbiguousFunctionInvoke(name, funcs) =>
        fromMsg(
          BadRequest withReason "Ambiguous function call",
          err.message,
          "invoke"              := name.value,
          "ambiguous functions" := e.fullyQualifiedFuncs)
      case other =>
        fromMsg_(
          InternalServerError withReason "Compilation error.",
          other.message)
    })
  }

  ////

  private def encodeData(data: Data): Option[Json] =
    DataCodec.Precise.encode(data)
}

sealed abstract class ToApiErrorInstances0 {
  import ToApiError._, ops._
  import ApiError._

  implicit def messageFailureToApiError[A <: MessageFailure]: ToApiError[A] =
    error {
      case err @ InvalidMessageBodyFailure(_, _) =>
        fromMsg_(
          BadRequest withReason "Invalid request body.",
          err.message)

      case ParseFailure(sanitized, _) =>
        fromMsg_(
          BadRequest withReason "Malformed request.",
          sanitized)

      case err @ MalformedMessageBodyFailure(_, _) =>
        fromMsg_(
          BadRequest withReason "Malformed request body.",
          err.message)

      case MediaTypeMissing(expectedMediaTypes) =>
        apiError(
          BadRequest withReason "Media type missing.",
          "supportedMediaTypes" := expectedMediaTypes.map(_.renderString))

      case MediaTypeMismatch(messageType, expectedMediaTypes) =>
        apiError(
          UnsupportedMediaType,
          "requestedMediaType"  := messageType.renderString,
          "supportedMediaTypes" := expectedMediaTypes.map(_.renderString))
    }

  implicit def nonEmptyListToApiError[A: ToApiError]: ToApiError[NonEmptyList[A]] =
    error { nel =>
      val herr = nel.head.toApiError
      if (nel.size ≟ 1) herr
      else {
        val stat = (Status.fromInt(herr.status.code) getOrElse herr.status) withReason "Multiple errors"
        apiError(stat, "errors" := nel.map(_.toApiError))
      }
    }
}
