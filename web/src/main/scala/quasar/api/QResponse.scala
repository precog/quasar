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
import quasar.contrib.scalaz.eitherT._
import quasar.effect.Failure
import quasar.fp._

import argonaut._, Argonaut._
import monocle._, macros.Lenses
import org.http4s._
import org.http4s.headers._
import org.http4s.dsl._
import scalaz.{Optional => _, _}
import scalaz.std.anyVal._
import scalaz.std.iterable._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scodec.bits.ByteVector

@Lenses
final case class QResponse[S[_]](status: Status, headers: Headers, body: Process[Free[S, ?], ByteVector]) {
  import QResponse.{PROCESS_EFFECT_THRESHOLD_BYTES, HttpResponseStreamFailureException}

  def flatMapS[T[_]](f: S ~> Free[T, ?]): QResponse[T] =
    copy[T](body = body.translate[Free[T, ?]](free.flatMapSNT(f)))

  def mapS[T[_]](f: S ~> T): QResponse[T] =
    copy[T](body = body.translate[Free[T, ?]](free.mapSNT(f)))

  def translate[T[_]](f: Free[S, ?] ~> Free[T, ?]): QResponse[T] =
    copy[T](body = body.translate(f))

  def modifyHeaders(f: Headers => Headers): QResponse[S] =
    QResponse.headers.modify(f)(this)

  def toHttpResponse(i: S ~> ResponseOr): Task[Response] =
    toHttpResponseF(free.foldMapNT(i))

  def toHttpResponseF(i: Free[S, ?] ~> ResponseOr): Task[Response] = {
    val failTask: ResponseOr ~> Task = new (ResponseOr ~> Task) {
      def apply[A](ror: ResponseOr[A]) =
        ror.fold(resp => Task.fail(new HttpResponseStreamFailureException(resp)), _.point[Task]).join
    }

    def handleBytes(bytes: Process[ResponseOr, ByteVector]): Response =
      Response(body = bytes.translate(failTask))
        .withStatus(status)
        .putHeaders(headers.toList: _*)

    body.translate[ResponseOr](i)
      .stepUntil(_.foldMap(_.length) >= PROCESS_EFFECT_THRESHOLD_BYTES)
      .map(handleBytes)
      .merge
  }

  def withStatus(s: Status): QResponse[S] =
    QResponse.status.set(s)(this)
}

object QResponse {
  /** Producing this many bytes from a `Process[F, ByteVector]` should require
    * at least one `F` effect.
    *
    * The scenarios this is intended to handle involve prepending a small wrapper
    * around a stream, like "[\n" when outputting JSON data in an array, and thus
    * 100 bytes seemed large enough to contain these cases and small enough as to
    * not force more than is needed.
    *
    * This exists because of how http4s handles errors from `Process` responses.
    * If an error is produced by a `Process` while streaming the connection is
    * severed, but the headers and status code have already been emitted to the
    * wire so it isn't possible to emit a useful error message or status. In an
    * attempt to handle many common scenarios where the first effect in the stream
    * is the most likely to error (i.e. opening a file, or other resource to stream
    * from) we'd like to step the stream until we've reached the first `F` effect
    * so that we can see if it succeeds before continuing with the rest of the
    * stream, providing a chance to respond with an error in the failure case.
    *
    * We cannot just `Process.unemit` the `Process` as there may be non-`F` `Await`
    * steps encountered before an actual `F` effect (from many of the combinators
    * in `process1` and the like).
    *
    * This leads us to the current workaround which is to define this threshold
    * which should be, ideally, just large enough to require the first `F` to
    * produce the bytes, but not more. We then consume the byte stream until it
    * ends or we've consumed this many bytes. Finally we have a chance to inspect
    * the `F` and see if anything failed before handing the rest of the process
    * to http4s to continue streaming to the client as normal.
    */
  val PROCESS_EFFECT_THRESHOLD_BYTES = 100L

  final class HttpResponseStreamFailureException(alternate: Response)
    extends java.lang.Exception

  def empty[S[_]]: QResponse[S] =
    QResponse(NoContent, Headers.empty, Process.halt)

  def ok[S[_]]: QResponse[S] =
    empty[S].withStatus(Ok)

  def header[S[_]](key: HeaderKey.Extractable): Optional[QResponse[S], key.HeaderT] =
    Optional[QResponse[S], key.HeaderT](
      qr => qr.headers.get(key))(
      h  => _.modifyHeaders(_.put(h)))

  def json[A: EncodeJson, S[_]](status: Status, a: A): QResponse[S] =
    string[S](status, a.asJson.pretty(minspace)).modifyHeaders(_.put(
      `Content-Type`(MediaType.`application/json`, Some(Charset.`UTF-8`))))

  def response[S[_], A]
      (status: Status, a: A)
      (implicit E: EntityEncoder[A], S0: Task :<: S)
      : QResponse[S] =
    QResponse(
      status,
      E.headers,
      Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](free.injectFT))

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  def streaming[S[_], A]
      (p: Process[Free[S, ?], A])
      (implicit E: EntityEncoder[A], S0: Task :<: S)
      : QResponse[S] =
    QResponse(
      Ok,
      E.headers,
      p.flatMap[Free[S, ?], ByteVector](a =>
        Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](free.injectFT)))

  def streaming[S[_], A, E]
      (p: Process[EitherT[Free[S, ?], E, ?], A])
      (implicit A: EntityEncoder[A], S0: Task :<: S, S1: Failure[E, ?] :<: S)
      : QResponse[S] = {
    val failure = Failure.Ops[E, S]
    streaming(p.translate(failure.unattemptT))
  }

  def string[S[_]](status: Status, s: String): QResponse[S] =
    QResponse(
      status,
      Headers(`Content-Type`(MediaType.`text/plain`, Some(Charset.`UTF-8`))),
      Process.emit(ByteVector.view(s.getBytes(Charset.`UTF-8`.nioCharset))))
}
