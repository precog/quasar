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

package quasar.api.services.query

import slamdata.Predef._
import quasar.api._
import quasar.api.PathUtils._
import quasar.api.services.Fixture._
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._, InMemory._, mount._
import quasar.sql._
import quasar.sql.fixpoint._

import org.http4s._
import org.specs2.matcher._, MustMatchers._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryFixture {
  type Eff0[A] = Coproduct[FileSystemFailure, FileSystem, A]
  type Eff1[A] = Coproduct[Mounting, Eff0, A]
  type Eff[A]  = Coproduct[Task, Eff1, A]

  case class Query(
    q: String,
    offset: Option[Natural] = None,
    limit: Option[Positive] = None,
    varNameAndValue: Option[(String, String)] = None)

  def get[A:EntityDecoder](service: (InMemState, Map[APath, MountConfig]) => HttpService)(path: ADir,
            query: Option[Query],
            state: InMemState,
            mounts: Map[APath, MountConfig] = Map.empty,
            status: Status,
            response: A => MatchResult[scala.Any]) = {
    val offset = query.flatMap(_.offset.map(_.shows))
    val limit = query.flatMap(_.limit.map(_.shows))
    val baseUri = pathUri(path)
      .+??("q", query.map(_.q))
      .+??("offset", offset)
      .+??("limit", limit)
    val uriWithVar = query.flatMap(_.varNameAndValue).map { case (varName, value) =>
      baseUri.+?("var."+varName,value)
    }.getOrElse(baseUri)
    val req = Request(uri = uriWithVar)
    val actualResponse = service(state, mounts)(req).unsafePerformSync
    response(actualResponse.as[A].unsafePerformSync)
    actualResponse.status must_== status
  }

  def serviceInter(state: InMemState, mounts: Map[APath, MountConfig]): Task[Eff ~> ResponseOr] =
    (runFs(state) |@| mountingInter(mounts))(effRespOr)

  def compileService(state: InMemState, mounts: Map[APath, MountConfig] = Map.empty): HttpService =
    HttpService.lift(req => serviceInter(state, mounts).flatMap { inter =>
      compile.service[Eff].toHttpService(inter).apply(req)
    })

  def executeService(state: InMemState, mounts: Map[APath, MountConfig] = Map.empty): HttpService =
    HttpService.lift(req => serviceInter(state, mounts).flatMap { inter =>
      execute.service[Eff].toHttpService(inter).apply(req)
    })

  def selectAll(from: FPath) = {
    val ast = SelectR(
      SelectAll,
      List(Proj(SpliceR(None), None)),
      Some(TableRelationAST(unsandbox(from), None)),
      None, None, None)
    pprint(ast)
  }
  def selectAllWithVar(from: FPath, varName: String) = {
    val ast = SelectR(
      SelectAll,
      List(Proj(SpliceR(None), None)),
      Some(TableRelationAST(unsandbox(from), None)),
      Some(BinopR(IdentR("pop"), VariR(varName), Gt)),
      None, None)
    pprint(ast)
  }

  def effRespOr(fs: FileSystem ~> Task, m: Mounting ~> Task): Eff ~> ResponseOr =
    liftMT[Task, ResponseT]             :+:
    liftMT[Task, ResponseT].compose(m)  :+:
    failureResponseOr[FileSystemError]  :+:
    liftMT[Task, ResponseT].compose(fs)
}
