/*
 * Copyright 2014–2018 SlamData Inc.
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
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.Failure
import quasar.fp._

import argonaut._, Argonaut._
import monocle._, macros.Lenses
import org.http4s._
import org.http4s.headers._
import org.http4s.dsl._
import scalaz.{Optional => _, _}
import scalaz.std.iterable._
import scalaz.Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scodec.bits.ByteVector

@Lenses
final case class QResponse[S[_]](status: Status, headers: Headers, body: Process[Free[S, ?], ByteVector]) {
  import QResponse.HttpResponseStreamFailureException

  def flatMapS[T[_]](f: S ~> Free[T, ?]): QResponse[T] =
    copy[T](body = body.translate[Free[T, ?]](free.flatMapSNT(f)))

  def mapS[T[_]](f: S ~> T): QResponse[T] =
    copy[T](body = body.translate[Free[T, ?]](free.mapSNT(f)))

  def translate[T[_]](f: Free[S, ?] ~> Free[T, ?]): QResponse[T] =
    copy[T](body = body.translate(f))

  def modifyHeaders(f: Headers => Headers): QResponse[S] =
    QResponse.headers.modify(f)(this)

  def toHttpResponse(i: S ~> FailedResponseOr): Response =
    toHttpResponseF(free.foldMapNT(i))

  def toHttpResponseF(i: Free[S, ?] ~> FailedResponseOr): Response = {
    val failTask: FailedResponseOr ~> Task =
      λ[FailedResponseOr ~> Task](
        _.fold(
          fr => Task.fail(new HttpResponseStreamFailureException(fr.toThrowable)),
          _.point[Task]).join)

    def handleBytes(bytes: Process[FailedResponseOr, ByteVector]): Response =
      Response(body = bytes.translate(failTask))
        .withStatus(status)
        .putHeaders(headers.toList: _*)

    handleBytes(body.translate[FailedResponseOr](i))
  }

  def withStatus(s: Status): QResponse[S] =
    QResponse.status.set(s)(this)
}

object QResponse {
  final class HttpResponseStreamFailureException(cause: Throwable)
    extends java.lang.Exception(cause)

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

  /**
    * This QResponse constructor attempts to eliminate all effects in this
    * stream that aren't actually part of the response stream by eagerly evaluating
    * the "first few" effects of the stream. Examples of such effects are
    * opening a quasar filesystem file, initiating a query to a connector, etc. Doing so
    * allows us to provide a proper error response to clients instead of sending a
    * Http OK and then failing to stream the response body because actually it
    * turned out the file didn't exist after all.
    *
    * The better fix for this problem would probably be to avoid constructing
    * such processes (at least for use in the web layer) and instead make a
    * distinction between the effects that create a stream and the effects
    * that "run" the stream.
    *
    * In the meantime, this constructor should mostly do the right thing if
    * provided with such processes.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  def streaming[S[_], A]
      (p: Process[Free[S, ?], A])
      (implicit E: EntityEncoder[A], S0: Task :<: S)
      : Free[S, QResponse[S]] = {
    val responseBody = p.flatMap[Free[S, ?], ByteVector](a =>
      Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](free.injectFT))

    /** Producing this many bytes from a `Process[F, ByteVector]` should require
      * at least one `F` effect.
      *
      * The scenarios this is intended to handle involve prepending a small wrapper
      * around a stream, like "[\n" when outputting JSON data in an array, and thus
      * 100 bytes seemed large enough to contain these cases and small enough as to
      * not force more than is needed.
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

    guardStream(responseBody, PROCESS_EFFECT_THRESHOLD_BYTES).map(guardedStream =>
      QResponse(
        Ok,
        E.headers,
        guardedStream))
  }

  def streaming[S[_], A, E]
      (p: Process[EitherT[Free[S, ?], E, ?], A])
      (implicit A: EntityEncoder[A], S0: Task :<: S, S1: Failure[E, ?] :<: S)
      : Free[S, QResponse[S]] = {
    val failure = Failure.Ops[E, S]
    streaming(p.translate(failure.unattemptT))
  }

  def string[S[_]](status: Status, s: String): QResponse[S] =
    QResponse(
      status,
      Headers(`Content-Type`(MediaType.`text/plain`, Some(Charset.`UTF-8`))),
      Process.emit(ByteVector.view(s.getBytes(Charset.`UTF-8`.nioCharset))))

  /**
    * If any errors were to occur before lookAhead bytes would be read from `stream`, these errors will
    * now happen whenever the outer `M` is first evaluated.
    */
  private def guardStream[M[_]: Catchable: Monad](stream: Process[M, ByteVector], lookAhead: Long): M[Process[M, ByteVector]] =
    stream.stepUntil(_.foldMap(_.length) >= lookAhead)
}
