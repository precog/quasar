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

package quasar.api.services.query

import slamdata.Predef._
import quasar.api.PathUtils._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.catchable._
import quasar.effect.ScopeExecution
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs._, InMemory._, mount._
import quasar.main.CoreEffIO
import quasar.api.services.Fixture
import quasar.sql._
import quasar.sql.fixpoint._

import eu.timepit.refined.scalaz._
import org.http4s._
import org.http4s.syntax.service._
import org.specs2.matcher._, MustMatchers._
import pathy.Path._
import scalaz._, Scalaz._

object queryFixture {

  case class Query(
    q: String,
    offset: Option[Natural] = None,
    limit: Option[Positive] = None,
    varNameAndValue: Option[(String, String)] = None)

  implicit val scopeExecutionCoreEffIO: ScopeExecution[Free[CoreEffIO, ?], Nothing] =
    ScopeExecution.ignore[Free[CoreEffIO, ?], Nothing]
  val executionIdRef: TaskRef[Long] = TaskRef(0L).unsafePerformSync

  def get[A:EntityDecoder](service: (InMemState, Map[APath, MountConfig]) => Service[Request, Response])(path: ADir,
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

  def compileService(state: InMemState, mounts: Map[APath, MountConfig] = Map.empty): Service[Request, Response] =
    Fixture.inMemFSWeb(state, MountingsConfig(mounts)).map(inter =>
      compile.service[CoreEffIO].toHttpService(inter).orNotFound).unsafePerformSync

  def executeService(state: InMemState, mounts: Map[APath, MountConfig] = Map.empty): Service[Request, Response] =
    Fixture.inMemFSWeb(state, MountingsConfig(mounts)).map(inter =>
      execute.service[CoreEffIO, Nothing](executionIdRef).toHttpService(inter).orNotFound).unsafePerformSync

  def selectAll(from: FPath) = {
    val ast = SelectR(
      SelectAll,
      List(Proj(SpliceR(None), None)),
      Some(TableRelationAST(unsandbox(from), None)),
      None, None, None)
    pprint(ast)
  }
  def selectAllWithVar(from: FPath, varName: String) =
    SelectR(
      SelectAll,
      List(Proj(SpliceR(None), None)),
      Some(TableRelationAST(unsandbox(from), None)),
      Some(BinopR(IdentR("pop"), VariR(varName), Gt)),
      None, None)
}
