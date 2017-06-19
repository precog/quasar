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

package quasar.api.services

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.Data
import quasar.DataArbitrary._
import quasar.api._
import quasar.api.ApiError._
import quasar.api.ApiErrorEntityDecoder._
import quasar.api.matchers._
import quasar.api.PathUtils._
import quasar.contrib.pathy._, PathArbitrary._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.InMemory._
import quasar.fs.mount._
import quasar.fs.mount.Fixture.runConstantMount
import quasar.fs.mount.module.Module
import quasar.sql._
import quasar.sql.fixpoint._

import eu.timepit.refined.numeric.{NonNegative, Positive => RPositive}
import eu.timepit.refined.auto._
import eu.timepit.refined.scalacheck.numeric._
import shapeless.tag.@@
import org.http4s.{Query, _}
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import pathy.scalacheck.PathyArbitrary._
import pathy.Path._
import matryoshka.data.Fix
import scalaz.{Failure => _, Zip =>_, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

class InvokeServiceSpec extends quasar.Qspec with FileSystemFixture with Http4s {
  import FileSystemError.pathErr
  import PathError.pathNotFound

  type Eff0[A] = Coproduct[Module.Failure, Module, A]
  type Eff[A]  = Coproduct[Task, Eff0, A]

  def effRespOr(inter: Module ~> Task): Eff ~> ResponseOr =
    liftMT[Task, ResponseT]              :+:
    failureResponseOr[Module.Error]      :+:
    (liftMT[Task, ResponseT] compose inter)

  def service(mem: InMemState, mounts: Map[APath, MountConfig] = Map.empty): HttpService =
    HttpService.lift(req => runFs(mem).flatMap { fs =>
      val module = Module.impl.default[Coproduct[Mounting, FileSystem, ?]]
      val runModule = foldMapNT(runConstantMount[Task](mounts) :+: fs) compose module
      invoke.service[Eff].toHttpService(effRespOr(runModule)).apply(req)
    })

  def sampleStatement(name: String): Statement[Fix[Sql]] = {
    val selectAll = SelectR(
      SelectAll,
      List(Proj(SpliceR(None), None)),
      Some(VariRelationAST(Vari("Bar"), None)),
      None, None, None)
    FunctionDecl(CIName(name), List(CIName("Bar")), selectAll)
  }

  def isExpectedResponse(data: Vector[Data], response: Response, format: MessageFormat) = {
    val expectedBody: Process[Task, String] = format.encode(Process.emitAll(data))
    response.as[String].unsafePerformSync must_== expectedBody.runLog.unsafePerformSync.mkString("")
    response.status must_== Status.Ok
    response.contentType must_== Some(`Content-Type`(format.mediaType, Charset.`UTF-8`))
  }

  "Invoke Service" should {
    "GET" >> {
      "produce a 404 if file does not exist" >> prop { file: AFile =>
        val response = service(InMemState.empty)(Request(uri = pathUri(file))).unsafePerformSync
        response.status must_= Status.NotFound
        response.as[ApiError].unsafePerformSync must beApiErrorLike[Module.Error](
          Module.Error.FSError(pathErr(pathNotFound(file))))
      }
      "produce a 400 bad request if path is a directory instead of a file" >>
        prop { (file: AFile, sampleData: Vector[Data]) =>
          val state = InMemState.fromFiles(Map(file -> sampleData))
          val response = service(state)(Request(uri = pathUri(fileParent(file)))).unsafePerformSync
          response.status must_= Status.BadRequest
          response.as[ApiError].unsafePerformSync must_=
            apiError(BadRequest withReason "Path must be a file")
        }
      "produce a 400 bad request if not all params are supplied with explanation of " +
      "which function parameters are missing from the query string" >>
        prop { (functionFile: AFile, dataFile: AFile, sampleData: Vector[Data]) =>
          val statements = List(sampleStatement(fileName(functionFile).value))
          val mounts = Map((fileParent(functionFile): APath) -> MountConfig.moduleConfig(statements))
          val state = InMemState.fromFiles(Map(dataFile -> sampleData))
          val response = service(state, mounts)(Request(uri = pathUri(functionFile))).unsafePerformSync
          response.status must_= Status.BadRequest
          response.as[ApiError].unsafePerformSync must beApiErrorLike[Module.Error](
            Module.Error.ArgumentsMissing(List(CIName("Bar"))))
        }
      "return evaluation of sql statement contained within function body if all params are supplied" >> {
        "in straightforward case" >>
          prop { (functionFile: AFile, dataFile: AFile, sampleData: Vector[Data]) =>
            val statements = List(sampleStatement(fileName(functionFile).value))
            val mounts = Map((fileParent(functionFile):APath) -> MountConfig.moduleConfig(statements))
            val state = InMemState.fromFiles(Map(dataFile -> sampleData))
            val arg = "`" + posixCodec.printPath(dataFile) + "`"
            val request = Request(uri = pathUri(functionFile).copy(query = Query.fromPairs("bar" -> arg)))
            val response = service(state, mounts)(request).unsafePerformSync
            isExpectedResponse(sampleData, response, MessageFormat.Default)
          }.pendingUntilFixed
        "if file in module function is relative" >>
          prop { (functionFile: AFile, rDataFile: RFile, sampleData: Vector[Data]) =>
            val statements = List(sampleStatement(fileName(functionFile).value))
            val mounts = Map((fileParent(functionFile):APath) -> MountConfig.moduleConfig(statements))
            val dataFile = fileParent(functionFile) </> rDataFile
            val state = InMemState.fromFiles(Map(dataFile -> sampleData))
            val arg = "`" + posixCodec.printPath(rDataFile) + "`"
            val request = Request(uri = pathUri(functionFile).copy(query = Query.fromPairs("bar" -> arg)))
            val response = service(state, mounts)(request).unsafePerformSync
            isExpectedResponse(sampleData, response, MessageFormat.Default)
          }.pendingUntilFixed
      }
      "support offset and limit" >> {
        prop { (functionFile: AFile,
                dataFile: AFile,
                sampleData: Vector[Data],
                offset: Int @@ NonNegative,
                limit: Int @@ RPositive) =>
          val selectAll =
            SelectR(
              SelectAll,
              List(Proj(SpliceR(None), None)),
              Some(TableRelationAST(unsandbox(dataFile), None)),
              None, None, None)
          val fName = fileName(functionFile).value
          val statements = List(FunctionDecl(CIName(fName), Nil, selectAll))
          val mounts = Map((fileParent(functionFile):APath) -> MountConfig.moduleConfig(statements))
          val state = InMemState.fromFiles(Map(dataFile -> sampleData))
          val request = Request(uri = pathUri(functionFile).copy(
            query = Query.fromPairs("bar" -> "2", "offset" -> offset.toString, "limit" -> limit.toString)))
          val response = service(state, mounts)(request).unsafePerformSync
          isExpectedResponse(sampleData.drop(offset).take(limit), response, MessageFormat.Default)
        }
      }
    }
  }
}
