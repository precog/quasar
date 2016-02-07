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

package quasar
package api
package services

import Predef._
import quasar.fp._
import numeric._

import argonaut._, Argonaut._

import org.http4s.server.HttpService
import org.http4s._
import org.scalacheck.Arbitrary
import org.specs2.ScalaCheck
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification

import pathy.Path._
import pathy.scalacheck.AbsFileOf
import pathy.scalacheck.PathyArbitrary._
import quasar.fs.{Path => QPath, _}
import quasar.fs.InMemory._
import quasar.recursionschemes._

import Fixture._
import quasar.std.IdentityLib

import scalaz.concurrent.Task
import scalaz.stream.Process

import scalaz._, Scalaz._

import query._

import eu.timepit.refined.numeric.{Positive => RPositive, NonNegative}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.scalacheck.numeric._

class CompileAndQueryServiceSpec extends Specification with FileSystemFixture with ScalaCheck {

  // Remove if eventually included in upstream scala-pathy
  implicit val arbitraryFileName: Arbitrary[FileName] =
    Arbitrary(Arbitrary.arbitrary[AFile].map(fileName(_)))

  import posixCodec.printPath

  def compileService(state: InMemState): HttpService =
    services.query.compileService[FileSystem](runFs(state).run)
  def queryService(state: InMemState): HttpService = service[FileSystem](runFs(state).run)

  case class Query(
    q: String,
    offset: Option[Natural] = None,
    limit: Option[Positive] = None,
    varNameAndValue: Option[(String, String)] = None)

  def post[A:EntityDecoder](service: InMemState => HttpService)(path: ADir,
            query: Option[Query],
            destination: Option[AFile],
            state: InMemState,
            status: Status,
            response: A => MatchResult[scala.Any]) = {
    val baseURI = Uri(path = printPath(path))
    val baseReq = Request(uri = baseURI, method = Method.POST)
    val req = query.map { query =>
      val uri = baseURI.+??("offset", query.offset.map(_.shows)).+??("limit", query.limit.map(_.shows))
      val uri1 = query.varNameAndValue.map{ case (name, value) => uri.+?("var."+name,value)}.getOrElse(uri)
      baseReq.copy(uri = uri1).withBody(query.q).run
    }.getOrElse(baseReq)
    val req1 = destination.map(destination =>
      req.copy(headers = Headers(Header("Destination", printPath(destination))))
    ).getOrElse(req)
    val actualResponse = service(state)(req1).run
    response(actualResponse.as[A].run)
    actualResponse.status must_== status
  }

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

  type File = pathy.Path[_,pathy.Path.File,Sandboxed]

  def selectAllLP(file: AFile) = LogicalPlan.Invoke(IdentityLib.Squash,List(LogicalPlan.Read(QPath.fromAPath(file))))
  def selectAll(from: File) = "select * from \"" + printPath(from) + "\""
  def selectAllWithVar(from: File, varName: String) = selectAll(from) + " where pop < :" + varName

  def toLP(q: String, vars: Variables): Fix[LogicalPlan] =
      (new sql.SQLParser()).parse(sql.Query(q)).toOption.map { ast =>
        quasar.queryPlan(ast,vars).run.value.toOption.get
      }.getOrElse(scala.sys.error("could not compile: " + q))

  "Compile and Query Service" should {
    def testBoth[A](test: (InMemState => HttpService) => Unit) = {
      "Compile" should {
        test(compileService)
      }
      "Query" should {
        test(queryService)
      }
    }
    testBoth { service =>
      "GET" >> {
        "be 404 for missing directory" ! prop { (dir: ADir, file: AFile) =>
          get(service)(
            path = dir,
            query = Some(Query(selectAll(file))),
            state = InMemState.empty,
            status = Status.NotFound,
            response = (a: String) => a must_== "???"
          )
        }.pendingUntilFixed("SD-773")
        "be 400 for missing query" ! prop { filesystem: SingleFileMemState =>
          get(service)(
            path = filesystem.parent,
            query = None,
            state = filesystem.state,
            status = Status.BadRequest,
            response = (a: String) => a must_== "The request must contain a query"
          )
        }
        "be 400 for query error" ! prop { filesystem: SingleFileMemState =>
          get(service)(
            path = filesystem.parent,
            query = Some(Query("select date where")),
            state = filesystem.state,
            status = Status.BadRequest,
            response = (a: String) => a must_== "keyword 'case' expected; `where'"
          )
        }
      }

      () // TODO: Remove after upgrading to specs2 3.x
    }
  }

  "Query" should {
    "execute a simple query" >> {
      "GET" ! prop { filesystem: SingleFileMemState =>
        val query = selectAll(file(filesystem.filename.value))
        get(queryService)(
          path = filesystem.parent,
          query = Some(Query(query)),
          state = filesystem.state,
          status = Status.Ok,
          response = (a: String) => a must_== jsonReadableLine.encode(Process.emitAll(filesystem.contents): Process[Task, Data]).runLog.run.mkString("")
        )
      }
      "POST" ! prop { (filesystem: SingleFileMemState, destination: AbsFileOf[AlphaCharacters]) =>
        post[Json](queryService)(
          path = filesystem.parent,
          query = Some(Query(selectAll(file(filesystem.filename.value)))),
          destination = Some(destination.path),
          state = filesystem.state,
          status = Status.Ok,
          response = json => json.field("out").flatMap(_.string).map(_ must_== printPath(destination.path))
            .getOrElse(ko("Missing out field on json response"))
        )
      }
    }
    "execute a query with offset and limit and a variable" >> {
      def queryAndExpectedLP(aFile: AFile, varName: AlphaCharacters, var_ : Int): (String,Fix[LogicalPlan]) = {
        val query = selectAllWithVar(file(fileName(aFile).value), varName.value)
        val inlineQuery = selectAllWithVar(aFile, varName.value)
        val lp = toLP(inlineQuery, Variables.fromMap(Map(varName.value -> var_.toString)))
        (query,lp)
      }
      "GET" ! prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int, offset: Int Refined NonNegative, limit: Int Refined RPositive) =>
        val (query, lp) = queryAndExpectedLP(filesystem.file, varName, var_)
        get(queryService)(
          path = filesystem.parent,
          query = Some(Query(query, offset = Some(offset), limit = Some(limit), varNameAndValue = Some((varName.value,var_.toString)))),
          state = filesystem.state.copy(queryResps = Map(lp -> filesystem.contents)),
          status = Status.Ok,
          response = (a: String) => a must_==
            jsonReadableLine.encode(Process.emitAll(filesystem.contents): Process[Task, Data]).runLog.run
              .drop(offset.get).take(limit.get).mkString("")
        )
      }
      "POST" ! prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int, offset: Natural, limit: Positive, destination: AbsFileOf[AlphaCharacters]) =>
        val (query, lp) = queryAndExpectedLP(filesystem.file, varName, var_)
        post[Json](queryService)(
          path = filesystem.parent,
          query = Some(Query(query, offset = Some(offset), limit = Some(limit), varNameAndValue = Some((varName.value, var_.toString)))),
          destination = Some(destination.path),
          state = filesystem.state.copy(queryResps = Map(lp -> filesystem.contents)),
          status = Status.Ok,
          response = json => json.field("out").flatMap(_.string).map(_ must_== printPath(destination.path))
              .getOrElse(ko("Missing expected out field on json response"))
        )
      }
    }
    "POST (error conditions)" >> {
      "be 404 for missing directory" ! prop { (dir: ADir, destination: AFile, filename: FileName) =>
        post[String](queryService)(
          path = dir,
          query = Some(Query(selectAll(file(filename.value)))),
          destination = Some(destination),
          state = InMemState.empty,
          status = Status.NotFound,
          response = _ must_== "???"
        )
      }.pendingUntilFixed("SD-773")
      "be 400 with missing query" ! prop { (filesystem: SingleFileMemState, destination: AFile) =>
        post[String](queryService)(
          path = filesystem.parent,
          query = None,
          destination = Some(destination),
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must_== "The body of the POST must contain a query"
        )
      }
      "be 400 with missing Destination header" ! prop { filesystem: SingleFileMemState =>
        post[Json](queryService)(
          path = filesystem.parent,
          query = Some(Query(selectAll(file(filesystem.filename.value)))),
          destination = None,
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must_== Json("error" := "The 'Destination' header must be specified")
        )
      }
      "be 400 for query error" ! prop { (filesystem: SingleFileMemState, destination: AFile) =>
        post[String](queryService)(
          path = filesystem.parent,
          query = Some(Query("select date where")),
          destination = Some(destination),
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must_== "keyword 'case' expected; `where'"
        )
      }
    }
  }
  "Compile" should {
    "plan simple query" ! prop { filesystem: SingleFileMemState =>
      // Representation of the directory as a string without the leading slash
      val pathString = printPath(filesystem.file).drop(1)
      get[String](compileService)(
        path = filesystem.parent,
        query = Some(Query(selectAll(file(filesystem.filename.value)))),
        state = filesystem.state,
        status = Status.Ok,
        response = κ(ok)
      )
    }
    "plan query with var" ! prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int) =>
      val pathString = printPath(filesystem.file).drop(1)
      val query = selectAllWithVar(file(filesystem.filename.value),varName.value)
      get[String](compileService)(
        path = filesystem.parent,
        query = Some(Query(query,varNameAndValue = Some((varName.value, var_.toString)))),
        state = filesystem.state,
        status = Status.Ok,
        response = κ(ok)
      )
    }
  }
}
