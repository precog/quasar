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

import quasar.Predef.PartialFunction
import quasar.SKI._
import org.http4s.{Request, Response, Status}
import org.http4s.server.HttpService
import scalaz._
import scalaz.concurrent.Task

final case class QHttpService[S[_]](f: PartialFunction[Request, Free[S, QuasarResponse[S]]]) {
  def apply(req: Request)(implicit S: Functor[S]): Free[S, QuasarResponse[S]] =
    f.applyOrElse(req, κ(Free.pure(QuasarResponse.empty[S].withStatus(Status.NotFound))))

  def toHttpService(i: S ~> ResponseOr)(implicit S: Functor[S]): HttpService = {
    def mkResponse(prg: Free[S, QuasarResponse[S]]) =
      prg.foldMap(i).flatMap(r => EitherT.right(r.toHttpResponse(i))).merge

    HttpService(f andThen mkResponse)
  }
}
