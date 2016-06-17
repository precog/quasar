/*
 * Copyright 2014–2016 SlamData Inc.
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
import quasar.fp.free
import quasar.SKI._
import org.http4s.{Request, Status, HttpService}
import scalaz._

final case class QHttpService[S[_]](f: PartialFunction[Request, Free[S, QResponse[S]]]) {
  def apply(req: Request): Free[S, QResponse[S]] =
    f.applyOrElse(req, κ(Free.pure(QResponse.empty[S].withStatus(Status.NotFound))))

  def flatMapS[T[_]](g: S ~> Free[T, ?]): QHttpService[T] =
    QHttpService(f.andThen(_.map(_.flatMapS(g)).flatMapSuspension(g)))

  def mapS[T[_]](g: S ~> T): QHttpService[T] =
    QHttpService(f.andThen(_.map(_.mapS(g)).mapSuspension(g)))

  def translate[T[_]](g: Free[S, ?] ~> Free[T, ?]): QHttpService[T] =
    QHttpService(f.andThen(resp => g(resp).map(_.translate(g))))

  def orElse(other: QHttpService[S]): QHttpService[S] =
    QHttpService(f orElse other.f)

  def toHttpService(i: S ~> ResponseOr): HttpService =
    toHttpServiceF(free.foldMapNT(i))

  def toHttpServiceF(i: Free[S, ?] ~> ResponseOr): HttpService = {
    def mkResponse(prg: Free[S, QResponse[S]]) =
      i(prg).flatMap(r => EitherT.right(r.toHttpResponseF(i))).merge

    HttpService(f andThen mkResponse)
  }
}
