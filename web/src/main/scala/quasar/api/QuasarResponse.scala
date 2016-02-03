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
import quasar.fp._

import argonaut._, Argonaut._
import org.http4s._
// import org.http4s.argonaut._
import org.http4s.headers.`Content-Type`
import org.http4s.dsl._
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scodec.bits.ByteVector

final case class QuasarResponse[S[_]](status: Status, headers: Headers, body: Process[Free[S, ?], ByteVector])

object QuasarResponse {

  def json[A: EncodeJson, S[_]](status: Status, a: A): QuasarResponse[S] = {
    val response = string[S](status, a.asJson.pretty(minspace))
    response.copy(headers = response.headers.put(
      `Content-Type`(MediaType.`application/json`, Some(Charset.`UTF-8`))))
  }

  def string[S[_]](status: Status, s: String): QuasarResponse[S] = QuasarResponse(
    status,
    Headers(`Content-Type`(MediaType.`text/plain`, Some(Charset.`UTF-8`))),
    Process.emit(ByteVector.view(s.getBytes(Charset.`UTF-8`.nioCharset))))

  def error[S[_]](status: Status, s: String): QuasarResponse[S] = json(status, Json("error" := s))

  def response[S[_]: Functor, A]
    (status: Status, a: A)
    (implicit E: EntityEncoder[A], S0: Task :<: S)
    : QuasarResponse[S] =
    QuasarResponse(
      status,
      E.headers,
      Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](lift[S]))

  def streaming[S[_]: Functor, A]
    (status: Status, p: Process[Free[S, ?], A])
    (implicit E: EntityEncoder[A], S0: Task :<: S)
    : QuasarResponse[S] =
    QuasarResponse(
      status,
      E.headers,
      p.flatMap[Free[S, ?], ByteVector](a =>
        Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](lift[S])))

  def toHttpResponse[S[_]:Functor](a: QuasarResponse[S], i: S ~> Task): Task[org.http4s.Response] =
    Response(status = a.status).withBody(a.body.translate(free.foldMapNT(i))).map(_.putHeaders(a.headers.toList: _*))

  private def lift[S[_]: Functor](implicit S0: Task :<: S): Task ~> Free[S, ?] = liftFT[S] compose injectNT[Task, S]

}
