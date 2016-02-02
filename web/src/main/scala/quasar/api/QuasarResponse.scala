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

import org.http4s.Response
import org.http4s.dsl._
import org.http4s.argonaut._
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

sealed trait QuasarResponse[S[_]]

object QuasarResponse {
  final case class Streaming[S[_]](p: Process[Free[S, ?], String]) extends QuasarResponse[S]
  final case class Json[S[_]](json: argonaut.Json) extends QuasarResponse[S]
  final case class Error[S[_]](responseCode: org.http4s.Status, a: String) extends QuasarResponse[S]
  final case class NotFound[S[_]](json: Option[argonaut.Json]) extends QuasarResponse[S]

  def toHttpResponse[S[_]:Functor](a: QuasarResponse[S], i: S ~> Task): Task[org.http4s.Response] = a match {
    case Streaming(p) =>
      val dataStream = p.translate(new (Free[S,?] ~> Task) {
        def apply[A](pr: Free[S,A]): Task[A] = pr.foldMap(i)
      })
      Ok(dataStream)
    case Json(json) => Ok(json)
    case Error(status, s) => Response().withBody(s).withStatus(status)
    case NotFound(json) =>
      json.map(json => Response().withBody(json).withStatus(org.http4s.Status.NotFound)).getOrElse(
        Task.now(Response().withStatus(org.http4s.Status.NotFound)))
  }
}
