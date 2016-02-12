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

package quasar.api.services.query

import quasar.Predef._
import quasar._, api._, fp._, fs._
import quasar.fs.InMemory._

import org.http4s._
import org.http4s.server.HttpService
import org.specs2.matcher._, MustMatchers._
import pathy.Path._, posixCodec._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryFixture {

  type File = pathy.Path[_,pathy.Path.File,Sandboxed]

  type Eff0[A] = Coproduct[FileSystemFailureF, FileSystem, A]
  type Eff[A]  = Coproduct[Task, Eff0, A]

  case class Query(
    q: String,
    offset: Option[Natural] = None,
    limit: Option[Positive] = None,
    varNameAndValue: Option[(String, String)] = None)

  def get[A:EntityDecoder](service: InMemState => HttpService)(path: ADir,
            query: Option[Query],
            state: InMemState,
            status: Status,
            response: A => MatchResult[scala.Any]) = {
    val offset = query.flatMap(_.offset.map(_.shows))
    val limit = query.flatMap(_.limit.map(_.shows))
    val baseUri = Uri(path = printPath(path))
      .+??("q", query.map(_.q))
      .+??("offset", offset)
      .+??("limit", limit)
    val uriWithVar = query.flatMap(_.varNameAndValue).map { case (varName, value) =>
      baseUri.+?("var."+varName,value)
    }.getOrElse(baseUri)
    val req = Request(uri = uriWithVar)
    val actualResponse = service(state)(req).run
    response(actualResponse.as[A].run)
    actualResponse.status must_== status
  }

  def compileService(state: InMemState): HttpService = compile.service[FileSystem].toHttpService(
      liftMT[Task, ResponseT].compose(runFs(state).run))

  def executeService(state: InMemState): HttpService =
    execute.service[Eff].toHttpService(effRespOr(runFs(state).run))

  def selectAll(from: File) = "select * from `" + printPath(from) + "`"
  def selectAllWithVar(from: File, varName: String) = selectAll(from) + " where pop < :" + varName

  def effRespOr(fs: FileSystem ~> Task): Eff ~> ResponseOr =
    free.interpret3[Task, FileSystemFailureF, FileSystem, ResponseOr](
      liftMT[Task, ResponseT].compose(NaturalTransformation.refl),
      Coyoneda.liftTF[FileSystemFailure, ResponseOr](
        failureResponseOr[FileSystemError]),
      liftMT[Task, ResponseT].compose(fs))

}
