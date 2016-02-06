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
import quasar.fs.{Path => QPath, _}

import org.http4s._
import org.http4s.dsl.{Path => HPath, _}
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}
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

  def requiredHeader[F[_]](key: HeaderKey.Extractable, request: Request): QuasarResponse[F] \/ key.HeaderT =
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

}
