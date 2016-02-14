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
import quasar._, fp._
import quasar.api.services.Fixture._
import quasar.fs.{Path => QPath, _}
import quasar.fs.InMemory._
import quasar.fs.NumericArbitrary._
import quasar.recursionschemes.Fix
import quasar.std.IdentityLib

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.server.HttpService
import org.scalacheck.Arbitrary
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck.{AbsFileOf, RelFileOf}
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.stream.Process

class ExecuteServiceSpec extends Specification with FileSystemFixture with ScalaCheck {
  import queryFixture._

  type FileOf[A] = AbsFileOf[A] \/ RelFileOf[A]

  // Remove if eventually included in upstream scala-pathy
  implicit val arbitraryFileName: Arbitrary[FileName] =
    Arbitrary(Arbitrary.arbitrary[AFile].map(fileName(_)))

  import posixCodec.printPath

  def executeServiceRef(mem: InMemState): (HttpService, Task[InMemState]) = {
    val (inter, ref) = runInspect(mem).run
    val svc = HttpService.lift(req =>
      execute.service[Eff]
        .toHttpService(effRespOr(inter compose fileSystem))
        .apply(req))

    (svc, ref)
  }

  def post[A:EntityDecoder](service: InMemState => HttpService)(path: ADir,
            query: Option[Query],
            destination: Option[String],
            state: InMemState,
            status: Status,
            response: A => MatchResult[scala.Any],
            stateCheck: Option[InMemState => MatchResult[scala.Any]] = None) = {
    val baseURI = Uri(path = printPath(path))
    val baseReq = Request(uri = baseURI, method = Method.POST)
    val req = query.map { query =>
      val uri = baseURI.+??("offset", query.offset.map(_.shows)).+??("limit", query.limit.map(_.shows))
      val uri1 = query.varNameAndValue.map{ case (name, value) => uri.+?("var."+name,value)}.getOrElse(uri)
      baseReq.copy(uri = uri1).withBody(query.q).run
    }.getOrElse(baseReq)
    val req1 = destination.map(destination =>
      req.copy(headers = Headers(Header("Destination", destination)))
    ).getOrElse(req)
    val (service, ref) = executeServiceRef(state)
    val actualResponse = service(req1).run
    response(actualResponse.as[A].run)
    actualResponse.status must_== status
    val stateCheck0 = stateCheck.getOrElse((_: InMemState) ==== state)
    stateCheck0(ref.run)
  }

  def selectAllLP(file: AFile) = LogicalPlan.Invoke(IdentityLib.Squash,List(LogicalPlan.Read(QPath.fromAPath(file))))

  def toLP(q: String, vars: Variables): Fix[LogicalPlan] =
      (new sql.SQLParser()).parse(sql.Query(q)).toOption.map { ast =>
        quasar.queryPlan(ast,vars).run.value.toOption.get
      }.getOrElse(scala.sys.error("could not compile: " + q))

  "Execute" should {
    "execute a simple query" >> {
      "GET" ! prop { filesystem: SingleFileMemState =>
        val query = selectAll(file(filesystem.filename.value))
        get(executeService)(
          path = filesystem.parent,
          query = Some(Query(query)),
          state = filesystem.state,
          status = Status.Ok,
          response = (a: String) => a must_== jsonReadableLine.encode(Process.emitAll(filesystem.contents): Process[Task, Data]).runLog.run.mkString("")
        )
      }
      "POST" ! prop { (filesystem: SingleFileMemState, destination: FileOf[AlphaCharacters]) => {
        val destinationPath = printPath(destination.fold(_.path, _.path))
        val expectedDestinationPath = destination.fold(_.path, filesystem.parent </> _.path)
        post[Json](executeService)(
          path = filesystem.parent,
          query = Some(Query(selectAll(file(filesystem.filename.value)))),
          destination = Some(destinationPath),
          state = filesystem.state,
          status = Status.Ok,
          response = json => json.field("out").flatMap(_.string).map(_ must_== printPath(expectedDestinationPath))
            .getOrElse(ko("Missing out field on json response")),
          stateCheck = Some(
            s => s.contents.contains(expectedDestinationPath) ==== true))
      }}
    }
    "execute a query with offset and limit and a variable" >> {
      def queryAndExpectedLP(aFile: AFile, varName: AlphaCharacters, var_ : Int): (String,Fix[LogicalPlan]) = {
        val query = selectAllWithVar(file(fileName(aFile).value), varName.value)
        val inlineQuery = selectAllWithVar(aFile, varName.value)
        val lp = toLP(inlineQuery, Variables.fromMap(Map(varName.value -> var_.toString)))
        (query,lp)
      }
      "GET" ! prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int, offset: Natural, limit: Positive) =>
        val (query, lp) = queryAndExpectedLP(filesystem.file, varName, var_)
        get(executeService)(
          path = filesystem.parent,
          query = Some(Query(query, offset = Some(offset), limit = Some(limit), varNameAndValue = Some((varName.value,var_.toString)))),
          state = filesystem.state.copy(queryResps = Map(lp -> filesystem.contents)),
          status = Status.Ok,
          response = (a: String) => a must_==
            jsonReadableLine.encode(Process.emitAll(filesystem.contents): Process[Task, Data]).runLog.run
              .drop(offset.value.toInt).take(limit.value.toInt).mkString("")
        )
      }
      "POST" ! prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int, offset: Natural, limit: Positive, destination: FileOf[AlphaCharacters]) =>
        val (query, lp) = queryAndExpectedLP(filesystem.file, varName, var_)
        val destinationPath = printPath(destination.fold(_.path, _.path))
        val expectedDestinationPath = destination.fold(_.path, filesystem.parent </> _.path)
        post[Json](executeService)(
          path = filesystem.parent,
          query = Some(Query(query, offset = Some(offset), limit = Some(limit), varNameAndValue = Some((varName.value, var_.toString)))),
          destination = Some(destinationPath),
          state = filesystem.state.copy(queryResps = Map(lp -> filesystem.contents)),
          status = Status.Ok,
          response = json => json.field("out").flatMap(_.string).map(_ must_== printPath(expectedDestinationPath))
            .getOrElse(ko("Missing expected out field on json response")),
          stateCheck = Some(
            s => s.contents.contains(expectedDestinationPath) ==== true))
      }
    }
    "POST (error conditions)" >> {
      "be 404 for missing directory" ! prop { (dir: ADir, destination: AFile, filename: FileName) =>
        post[String](executeService)(
          path = dir,
          query = Some(Query(selectAll(file(filename.value)))),
          destination = Some(printPath(destination)),
          state = InMemState.empty,
          status = Status.NotFound,
          response = _ must_== "???"
        )
      }.pendingUntilFixed("SD-773")
      "be 400 with missing query" ! prop { (filesystem: SingleFileMemState, destination: AFile) =>
        post[Json](executeService)(
          path = filesystem.parent,
          query = None,
          destination = Some(printPath(destination)),
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must_== Json("error" := "The body of the POST must contain a query")
        )
      }
      "be 400 with missing Destination header" ! prop { filesystem: SingleFileMemState =>
        post[Json](executeService)(
          path = filesystem.parent,
          query = Some(Query(selectAll(file(filesystem.filename.value)))),
          destination = None,
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must_== Json("error" := "The 'Destination' header must be specified")
        )
      }
      "be 400 for query error" ! prop { (filesystem: SingleFileMemState, destination: AFile) =>
        post[Json](executeService)(
          path = filesystem.parent,
          query = Some(Query("select date where")),
          destination = Some(printPath(destination)),
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must_== Json("error" := "end of input; ErrorToken(illegal character)")
        )
      }
    }
  }
}
