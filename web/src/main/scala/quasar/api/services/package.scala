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
import quasar.Data
import quasar.Errors.flatten
import quasar.fp._
import quasar.fs.{Path => QPath, _}, FileSystemError._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.http4s.dsl.{Path => HPath, _}
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

package object services {
  import Validation.FlatMap._

  def formattedDataResponse[S[_]: Functor](
    format: MessageFormat,
    data: Process[FileSystemErrT[Free[S, ?], ?], Data]
  )(implicit
    S0: FileSystemFailureF :<: S,
    S1: Task :<: S
  ): QuasarResponse[S] = {
    val ctype = `Content-Type`(format.mediaType, Some(Charset.`UTF-8`))
    QuasarResponse.headers.modify(
      _.put(ctype) ++ format.disposition.toList
    )(QuasarResponse.streaming(format.encode[FileSystemErrT[Free[S,?],?]](data)))
  }

  def limitOrInvalid[S[_]](
    limitParam: Option[ValidationNel[ParseFailure, Positive]]
  ): QuasarResponse[S] \/ Option[Positive] =
    valueOrInvalid("limit", limitParam)

  def offsetOrInvalid[S[_]](
    offsetParam: Option[ValidationNel[ParseFailure, Natural]]
  ): QuasarResponse[S] \/ Option[Natural] =
    valueOrInvalid("offset", offsetParam)

  def valueOrInvalid[S[_], F[_]: Traverse, A](
    paramName: String,
    paramResult: F[ValidationNel[ParseFailure, A]]
  ): QuasarResponse[S] \/ F[A] =
    orBadRequest(paramResult, nel =>
      s"invalid ${paramName}: ${nel.head.sanitized} (${nel.head.details})")

  /** Convert a parameter validation response into a `400 Bad Request` with the
    * error message produced by the given function when it failed, otherwise
    * return the parsed value.
    */
  def orBadRequest[S[_], F[_]: Traverse, A](
    param: F[ValidationNel[ParseFailure, A]],
    msg: NonEmptyList[ParseFailure] => String
  ): QuasarResponse[S] \/ F[A] =
    param.traverseU(_.disjunction.leftMap(nel =>
      QuasarResponse.error[S](BadRequest, msg(nel))))

  def requiredHeader2[F[_]](key: HeaderKey.Extractable, request: Request): QuasarResponse[F] \/ key.HeaderT =
    request.headers.get(key) \/> QuasarResponse.error(BadRequest, s"The '${key.name}' header must be specified")

  def respond[S[_], A, F[_]](a: Free[S, A])(implicit ev: ToQuasarResponse[A, F]): Free[S, QuasarResponse[F]] =
    a.map(ev.toResponse)

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object Offset extends OptionalValidatingQueryParamDecoderMatcher[Natural]("offset")

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object Limit  extends OptionalValidatingQueryParamDecoderMatcher[Positive]("limit")

  implicit val naturalParamDecoder: QueryParamDecoder[Natural] = new QueryParamDecoder[Natural] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Natural] =
      QueryParamDecoder[Long].decode(value).flatMap(long =>
        Natural(long).toSuccess(NonEmptyList(ParseFailure(value.value, "must be >= 0")))
      )
  }

  implicit val positiveParamDecoder: QueryParamDecoder[Positive] = new QueryParamDecoder[Positive] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Positive] =
      QueryParamDecoder[Long].decode(value).flatMap(long =>
        Positive(long).toSuccess(NonEmptyList(ParseFailure(value.value, "must be >= 1")))
      )
  }

  //----- TO DELETE -----//

  // TODO: remove fileSystemErrorResponse, pathErrorResponse, and errorResponse once usages are removed

  // TODO: Polish this up
  def fileSystemErrorResponse(error: FileSystemError): Task[Response] =
    error match {
      case PathError(e)                => pathErrorResponse(e)
      case PlannerError(_, _)          => errorResponse(BadRequest, error.shows)
      case UnknownReadHandle(handle)   => errorResponse(InternalServerError, s"Unknown read handle: $handle")
      case UnknownWriteHandle(handle)  => errorResponse(InternalServerError, s"Unknown write handle: $handle")
      case UnknownResultHandle(handle) => errorResponse(InternalServerError, s"Unknown result handle: $handle")
      case PartialWrite(numFailed)     => errorResponse(InternalServerError, s"Failed to write $numFailed records")
      case WriteFailed(data, reason)   => errorResponse(InternalServerError, s"Failed to write ${data.shows} because of $reason")
    }

  def pathErrorResponse(error: PathError2): Task[Response] =
    error match {
      case PathError2.Case.PathExists(path) => errorResponse(Conflict, s"${posixCodec.printPath(path)} already exists")
      case PathError2.Case.PathNotFound(path) => errorResponse(NotFound, s"${posixCodec.printPath(path)} doesn't exist")
      case PathError2.Case.InvalidPath(path, reason) => errorResponse(BadRequest, s"${posixCodec.printPath(path)} is an invalid path because $reason")
    }

  def errorResponse(
    status: org.http4s.dsl.impl.EntityResponseGenerator,
    message: String):
      Task[Response] =
    status(Json("error" := message))

  def requiredHeader(key: HeaderKey.Singleton, req: Request): Task[Response] \/ String =
    req.headers.get(key).cata(
      hdr => \/-(hdr.value),
      -\/(errorResponse(BadRequest, "The '" + key.name + "' header must be specified")))

  type FilesystemTask[A] = FileSystemErrT[Task, A]

  def formatAsHttpResponse[S[_]: Functor,A: EntityEncoder](f: S ~> Task)(data: Process[FileSystemErrT[Free[S,?], ?], A],
                                                                         contentType: `Content-Type`,
                                                                         disposition: Option[`Content-Disposition`]): Task[Response] = {
    // Check the first element of data, if it's an error return an error response, otherwise serialize any
    // other errors among the remaining data that is sent to the client.
    convert(f)(data).unconsOption.fold(
      fileSystemErrorResponse,
      _.fold(Ok(""))({ case (first, rest) =>
        Ok(Process.emit(first) ++ rest.translate(flatten))
      }).map(_.putHeaders(contentType :: disposition.toList : _*))).join
  }

  def formatQuasarDataStreamAsHttpResponse[S[_]: Functor](f: S ~> Task)(data: Process[FileSystemErrT[Free[S,?], ?], Data],
                                                                        format: MessageFormat): Task[Response] = {
    formatAsHttpResponse(f)(
      data = format.encode[FileSystemErrT[Free[S,?], ?]](data),
      contentType = `Content-Type`(format.mediaType, Some(Charset.`UTF-8`)),
      disposition = format.disposition
    )
  }

  def convert[S[_]: Functor, A](f: S ~> Task)(from: Process[FileSystemErrT[Free[S,?],?], A]): Process[FilesystemTask, A] = {
    type F[A] = Free[S,A]
    type M[A] = FileSystemErrT[F,A]
    val trans: F ~> Task = hoistFree(f)
    val trans2: M ~> FilesystemTask = Hoist[FileSystemErrT].hoist(trans)
    from.translate(trans2)
  }

  def handleOffsetLimitParams(offset: Option[ValidationNel[ParseFailure, Natural]],
                              limit: Option[ValidationNel[ParseFailure, Positive]])(
                              f: (Option[Natural], Option[Positive]) => Task[Response]) = {
    val offsetWithErrorMsg: String \/ Option[Natural] = offset.traverseU(_.disjunction.leftMap(
      nel => s"invalid offset: ${nel.head.sanitized} (${nel.head.details})"))
    val limitWithErrorMsg: String \/ Option[Positive] = limit.traverseU(_.disjunction.leftMap(
      nel => s"invalid limit: ${nel.head.sanitized} (${nel.head.details})"))
    val possibleResponse = (offsetWithErrorMsg |@| limitWithErrorMsg)(f)
    possibleResponse.leftMap(errMessage => BadRequest(errMessage)).merge
  }
}
