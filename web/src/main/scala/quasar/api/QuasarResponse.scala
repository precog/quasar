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
import quasar.effect.{Failure, FailureF}
import quasar.fp._

import argonaut._, Argonaut._
import monocle._, macros.Lenses
import org.http4s._
import org.http4s.headers._
import org.http4s.dsl._
import scalaz.{Optional => _, _}
import scalaz.syntax.monad._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scodec.bits.ByteVector

@Lenses
final case class QuasarResponse[S[_]](status: Status, headers: Headers, body: Process[Free[S, ?], ByteVector]) {
  import QuasarResponse.HttpResponseStreamFailureException

  def modifyHeaders(f: Headers => Headers): QuasarResponse[S] =
    QuasarResponse.headers.modify(f)(this)

  def toHttpResponse(i: S ~> ResponseOr)(implicit S: Functor[S]): Task[Response] = {
    val failTask: ResponseOr ~> Task = new (ResponseOr ~> Task) {
      def apply[A](ror: ResponseOr[A]) =
        ror.fold(resp => Task.fail(new HttpResponseStreamFailureException(resp)), _.point[Task]).join
    }

    def handleRest(vec: ByteVector, rest: Process[ResponseOr, ByteVector]): Response =
      Response(body = Process.emit(vec) ++ rest.translate(failTask))

    body.translate[ResponseOr](free.foldMapNT(i))
      .unconsOption.map(
        _.fold(Response())((handleRest _).tupled)
          .withStatus(status)
          .putHeaders(headers.toList: _*)
      ).merge
  }

  def withHeaders(hdrs: Headers): QuasarResponse[S] =
    QuasarResponse.headers.set(hdrs)(this)

  def withStatus(s: Status): QuasarResponse[S] =
    QuasarResponse.status.set(s)(this)
}

object QuasarResponse {
  final class HttpResponseStreamFailureException(alternate: Response)
    extends java.lang.Exception

  def empty[S[_]]: QuasarResponse[S] =
    QuasarResponse(NoContent, Headers.empty, Process.halt)

  def ok[S[_]]: QuasarResponse[S] =
    empty[S].withStatus(Ok)

  def header[S[_]](key: HeaderKey.Extractable): Optional[QuasarResponse[S], key.HeaderT] =
    Optional[QuasarResponse[S], key.HeaderT](
      qr => qr.headers.get(key))(
      h  => _.modifyHeaders(_.put(h)))

  def error[S[_]](status: Status, s: String): QuasarResponse[S] =
    json(status, Json("error" := s))

  def json[A: EncodeJson, S[_]](status: Status, a: A): QuasarResponse[S] =
    string[S](status, a.asJson.pretty(minspace)).modifyHeaders(_.put(
      `Content-Type`(MediaType.`application/json`, Some(Charset.`UTF-8`))))

  def response[S[_]: Functor, A]
      (status: Status, a: A)
      (implicit E: EntityEncoder[A], S0: Task :<: S)
      : QuasarResponse[S] =
    QuasarResponse(
      status,
      E.headers,
      Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](injectFT))

  def streaming[S[_]: Functor, A]
      (p: Process[Free[S, ?], A])
      (implicit E: EntityEncoder[A], S0: Task :<: S)
      : QuasarResponse[S] =
    QuasarResponse(
      Ok,
      E.headers,
      p.flatMap[Free[S, ?], ByteVector](a =>
        Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](injectFT)))

  def streaming[S[_]: Functor, A, E]
      (p: Process[EitherT[Free[S, ?], E, ?], A])
      (implicit A: EntityEncoder[A], S0: Task :<: S, S1: FailureF[E, ?] :<: S)
      : QuasarResponse[S] = {
    val failure = Failure.Ops[E, S]
    streaming(p.translate(failure.unattemptT))
  }

  def string[S[_]](status: Status, s: String): QuasarResponse[S] =
    QuasarResponse(
      status,
      Headers(`Content-Type`(MediaType.`text/plain`, Some(Charset.`UTF-8`))),
      Process.emit(ByteVector.view(s.getBytes(Charset.`UTF-8`.nioCharset))))
}
