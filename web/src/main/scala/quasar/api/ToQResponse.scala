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
import quasar.fp._
import quasar.fp.ski._

import argonaut._, Argonaut._
import org.http4s._, Status.Ok
import scalaz._
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
  import ToQResponse.{response, ops}, ops._
  import ToApiError.ops._

  implicit def apiErrorQResponse[S[_]]: ToQResponse[ApiError, S] =
    response(err => QResponse.json(err.status, Json("error" := err)))

  implicit def toApiErrorQResponse[A: ToApiError, S[_]]: ToQResponse[A, S] =
    response(_.toApiError.toResponse)

  implicit def disjunctionQResponse[A, B, S[_]]
    (implicit ev1: ToQResponse[A, S], ev2: ToQResponse[B, S])
    : ToQResponse[A \/ B, S] =
      response(_.fold(ev1.toResponse, ev2.toResponse))

  implicit def http4sResponseToQResponse[S[_]](implicit ev: Task :<: S): ToQResponse[Response, S] =
    response(r => QResponse(
      status = r.status,
      headers = r.headers,
      body = r.body.translate[Free[S, ?]](free.injectFT[Task, S])))

  implicit def qResponseToQResponse[S[_]]: ToQResponse[QResponse[S], S] =
    response(ι)

  implicit def statusToQResponse[S[_]]: ToQResponse[Status, S] =
    response(QResponse.empty[S] withStatus _)

  implicit def stringQResponse[S[_]]: ToQResponse[String, S] =
    response(QResponse.string(Ok, _))

  implicit def unitQResponse[S[_]]: ToQResponse[Unit, S] =
    response(κ(QResponse.empty[S]))
}

sealed abstract class ToQResponseInstances0 {
  implicit def jsonQResponse[A: EncodeJson, S[_]]: ToQResponse[A, S] =
    ToQResponse.response(a => QResponse.json(Ok, a))
}
