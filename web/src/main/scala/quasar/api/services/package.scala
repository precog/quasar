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
import quasar.Data
import quasar.contrib.argonaut._
import quasar.effect.Failure
import quasar.ejson.{EJson, JsonCodec}
import quasar.fp.numeric._

import argonaut._, Argonaut._
import eu.timepit.refined.auto._
import matryoshka.Recursive
import matryoshka.implicits._
import org.http4s._
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{Process, Process1, process1}

package object services {
  import Validation.FlatMap._

  /** Set the `Content-Type` of a response to `application/json;mode=ejson`. */
  def contentJsonEncodedEJson[S[_]]: QResponse[S] => QResponse[S] = {
    val ejsAsJs   = MediaType.`application/json`.withExtensions(Map("mode" -> "ejson"))
    val ctype     = `Content-Type`(ejsAsJs, Some(Charset.`UTF-8`))
    QResponse.headers.modify(_.put(ctype))
  }

  // TODO: Add `Content-Disposition` support?
  def ejsonResponse[S[_], E, J](
    ejson: Process[EitherT[Free[S, ?], E, ?], J]
  )(implicit
    J : Recursive.Aux[J, EJson],
    S0: Failure[E, ?] :<: S,
    S1: Task :<: S
  ): QResponse[S] = {
    val js = ejson.map(_.cata[Json](JsonCodec.encodeƒ[Json]))
    contentJsonEncodedEJson(QResponse.streaming(js |> jsonArrayLines))
  }

  // TODO: Handle when response isn't an object or array.
  def firstEJsonResponse[S[_], E, J](
    ejson: Process[EitherT[Free[S, ?], E, ?], J]
  )(implicit
    J : Recursive.Aux[J, EJson],
    S0: Failure[E, ?] :<: S,
    S1: Task :<: S
  ): QResponse[S] = {
    val js = ejson.take(1).map(_.cata[Json](JsonCodec.encodeƒ[Json]).spaces2)
    contentJsonEncodedEJson(QResponse.streaming(js))
  }

  def formattedDataResponse[S[_], E](
    format: MessageFormat,
    data: Process[EitherT[Free[S, ?], E, ?], Data]
  )(implicit
    S0: Failure[E, ?] :<: S,
    S1: Task :<: S
  ): QResponse[S] = {
    val ctype = `Content-Type`(format.mediaType, Some(Charset.`UTF-8`))
    QResponse.headers.modify(
      _.put(ctype) ++ format.disposition.toList
    )(QResponse.streaming(format.encode[EitherT[Free[S,?], E,?]](data)))
  }

  /** Transform a stream of `Json` into a stream of text representing the lines
    * of a file containing a JSON array of the values.
    */
  val jsonArrayLines: Process1[Json, String] = {
    val lineSep = "\r\n"
    process1.lift((_: Json).nospaces)
      .intersperse("," + lineSep)
      .prepend(List("[" + lineSep))
      .append(Process.emit(lineSep + "]" + lineSep))
  }

  def limitOrInvalid(
    limitParam: Option[ValidationNel[ParseFailure, Positive]]
  ): ApiError \/ Option[Positive] =
    valueOrInvalid("limit", limitParam)

  def offsetOrInvalid(
    offsetParam: Option[ValidationNel[ParseFailure, Natural]]
  ): ApiError \/ Natural =
    valueOrInvalid("offset", offsetParam).map(_.getOrElse(0L))

  def valueOrInvalid[F[_]: Traverse, A](
    paramName: String,
    paramResult: F[ValidationNel[ParseFailure, A]]
  ): ApiError \/ F[A] =
    orBadRequest(paramResult, nel =>
      s"invalid ${paramName}: ${nel.head.sanitized} (${nel.head.details})")

  /** Convert a parameter validation response into a `400 Bad Request` with the
    * error message produced by the given function when it failed, otherwise
    * return the parsed value.
    */
  def orBadRequest[F[_]: Traverse, A](
    param: F[ValidationNel[ParseFailure, A]],
    msg: NonEmptyList[ParseFailure] => String
  ): ApiError \/ F[A] =
    param.traverse(_.disjunction.leftMap(nel =>
      ApiError.fromMsg_(BadRequest withReason "Invalid query parameter.", msg(nel))))

  def requiredHeader(key: HeaderKey.Extractable, request: Request): ApiError \/ key.HeaderT =
    request.headers.get(key) \/> ApiError.apiError(
      BadRequest withReason s"'${key.name}' header missing.",
      "headerName" := key.name.toString)

  def respond[S[_], A](a: Free[S, A])(implicit A: ToQResponse[A, S]): Free[S, QResponse[S]] =
    a.map(A.toResponse)

  def respond_[S[_], A](a: A)(implicit A: ToQResponse[A, S]): Free[S, QResponse[S]] =
    respond(Free.pure(a))

  def respondT[S[_], A, B](
    a: EitherT[Free[S, ?], A, B]
  )(implicit
    A: ToQResponse[A, S],
    B: ToQResponse[B, S]
  ): Free[S, QResponse[S]] =
    a.fold(A.toResponse, B.toResponse)

  object Offset extends OptionalValidatingQueryParamDecoderMatcher[Natural]("offset")

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
