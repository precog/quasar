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
import quasar._
import quasar.Fixture.unsafeToLP
import quasar.api._, ApiErrorEntityDecoder._, ToApiError.ops._
import quasar.api.matchers._
import quasar.api.services.VCacheFixture
import quasar.api.services.Fixture._
import quasar.common.{Map => _, _}
import quasar.contrib.pathy._, PathArbitrary._
import quasar.effect.ScopeExecution
import quasar.fp._
import quasar.fp.free.{foldMapNT, injectNT, liftFT}
import quasar.fp.ski._
import quasar.fp.numeric._
import quasar.fs._, InMemory._, mount._
import quasar.fs.mount.cache.ViewCache
import quasar.fs.mount.MountConfig.viewConfig0
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}
import quasar.main.CoreEffIO
import quasar.sql.{Positive => _, _}
import quasar.time.DateGenerators._

import java.time.{Instant, Duration}

import argonaut.{Json => AJson, _}, Argonaut._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.{NonNegative, Positive => RPositive}
import eu.timepit.refined.scalacheck.numeric._
import eu.timepit.refined.scalaz._
import matryoshka.data.Fix
import org.http4s
import org.http4s._
import org.http4s.argonaut._
import org.http4s.headers._
import org.specs2.matcher.MatchResult
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import pathy.scalacheck.AlphaCharacters
// TODO: Consider if possible to use argonaut backend and avoid printing followed by parsing
import rapture.json._, jsonBackends.json4s._, patternMatching.exactObjects._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import shapeless.tag.@@
import quasar.api.PathUtils._

class ExecuteServiceSpec extends quasar.Qspec with FileSystemFixture with Http4s {
  import queryFixture._
  import posixCodec.printPath
  import FileSystemError.executionFailed_
  import VCacheFixture.{Eff => _, _}

  val lpf = new LogicalPlanR[Fix[LogicalPlan]]

  implicit val scopeExecutionViewEff: ScopeExecution[Free[ViewEff, ?], Nothing] =
    ScopeExecution.ignore[Free[ViewEff, ?], Nothing]

  def executeServiceRef(
    mem: InMemState,
    f: QueryFile ~> Free[QueryFile, ?],
    mounts: Map[APath, MountConfig] = Map.empty
  ): (Service[Request, Response], Task[InMemState]) = {
    val (inter, ref) = inMemFSWebInspect(mem, MountingsConfig(mounts)).unsafePerformSync
    val finalInter = free.transformIn(f andThen foldMapNT(injectNT[QueryFile, CoreEffIO] andThen inter), inter)
    val svc = execute.service[CoreEffIO, Nothing](executionIdRef).toHttpService(finalInter).orNotFound

    (svc, ref.map{ case (mem, _) => mem })
  }

  def failingExecPlan(msg: String): QueryFile ~> Free[QueryFile, ?] =
    λ[QueryFile ~> Free[QueryFile, ?]] {
      case QueryFile.ExecutePlan(lp, _) =>
        (Vector[PhaseResult](), executionFailed_(lp, msg).left[Unit]).point[Free[QueryFile, ?]]

      case otherwise => Free.liftF(otherwise)
    }

  def post[A: EntityDecoder](
    eval: QueryFile ~> Free[QueryFile, ?])(
    path: ADir,
    query: Option[Query],
    destination: Option[FPath],
    state: InMemState,
    status: Status,
    response: A => MatchResult[_],
    stateCheck: Option[InMemState => MatchResult[_]] = None
  ) = {

    val baseURI = pathUri(path)
    val baseReq = Request(uri = baseURI, method = Method.POST)
    val req = query.map { query =>
      val uri = baseURI.+??("offset", query.offset.map(_.shows)).+??("limit", query.limit.map(_.shows))
      val uri1 = query.varNameAndValue.map{ case (name, value) => uri.+?("var."+name,value)}.getOrElse(uri)
      baseReq.withUri(uri1).withBody(query.q).unsafePerformSync
    }.getOrElse(baseReq)
    val req1 = destination.map(destination =>
      req.withHeaders(Headers(Header("Destination", UriPathCodec.printPath(destination))))
    ).getOrElse(req)
    val (service, ref) = executeServiceRef(state, eval)
    val actualResponse = service(req1).unsafePerformSync
    val stateCheck0 = stateCheck.getOrElse((_: InMemState) ==== state)
    response(actualResponse.as[A].unsafePerformSync) and (actualResponse.status must_== status) and stateCheck0(ref.unsafePerformSync)
  }

  "Execute" should {
    "execute a simple query" >> {
      "GET" >> prop { filesystem: SingleFileMemState =>
        val query = selectAll(file(filesystem.filename.value))
        get(executeService)(
          path = filesystem.parent,
          query = Some(Query(query)),
          state = filesystem.state,
          status = Status.Ok,
          response = (a: String) => a must_== jsonReadableLine.encode(Process.emitAll(filesystem.contents): Process[Task, Data]).runLog.unsafePerformSync.mkString(""))
      }
      "POST" >> prop { (filesystem: SingleFileMemState, destination: FPath) => {
        val expectedDestinationPath = refineTypeAbs(destination).fold(ι, filesystem.parent </> _)
        post[AJson](liftFT[QueryFile])(
          path = filesystem.parent,
          query = Some(Query(selectAll(file1(filesystem.filename)))),
          destination = Some(destination),
          state = filesystem.state,
          status = Status.Ok,
          response = json => Json.parse(json.nospaces) must beLike { case json""" { "out": $outValue, "phases": $outPhases }""" =>
            outValue.as[String] must_== printPath(expectedDestinationPath)
          },
          stateCheck = Some(s => s.contents.keys must contain(expectedDestinationPath)))
      }}
    }
    "execute a constant query" >> {
      "GET" >> {
        val query = "1 + 1"
        get(executeService)(
          path = rootDir,
          query = Some(Query(query)),
          state = emptyMem,
          status = Status.Ok,
          response = (a: String) => a must_=== "2\r\n")
      }
    }
    "response with view cache headers" >> {
      "fresh" >> prop {
          (now: Instant, lastUpdate: Instant, maxAgeSecs: Int @@ RPositive) => {
          val maxAge = Duration.ofSeconds(maxAgeSecs.toLong)
          lastUpdate.isBefore(Instant.MAX.minus(maxAge)) && now.isBefore(lastUpdate.plus(maxAge))
        } ==> {
          val f = rootDir </> file("f")
          val g = rootDir </> file("g")
          val expr = sqlB"""select { "α": 7 }"""
          val viewCache = ViewCache(
            MountConfig.ViewConfig(expr, Variables.empty), lastUpdate.some, None, 0, None, None,
            maxAgeSecs.toLong, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, g, None)
          val mounts = Map[APath, MountConfig](f -> viewConfig0(expr))

          val (resp, vc) = evalViewTest(now, mounts, InMemState.empty) { (it, ir) =>
            (for {
              _ <- vcache.put(f, viewCache)
              a <- execute.service[ViewEff, Nothing](executionIdRef).apply(
                     Request(uri = (pathUri(f) / "").copy(query =
                       http4s.Query.fromPairs(
                         "q" -> s"select * from `../${fileName(f).value}`"))))
              c <- vcache.get(f).run
            } yield (a.toHttpResponse(ir), c)).foldMap(it)
          }.unsafePerformSync

          resp.status must_= Status.Ok
          resp.headers.get(Expires.name) ∘ (_.value) must_= None
          vc ∘ (_.cacheReads) must_= (viewCache.cacheReads ⊹ 1).some
        }
      }

      "stale" >> prop {
          (now: Instant, lastUpdate: Instant, maxAgeSecs: Int @@ RPositive) => {
          val maxAge = Duration.ofSeconds(maxAgeSecs.toLong)
            lastUpdate.isBefore(Instant.MAX.minus(maxAge)) && now.isAfter(lastUpdate.plus(maxAge))
        } ==> {
          val f = rootDir </> file("f")
          val g = rootDir </> file("g")
          val expr = sqlB"""select { "α": 7 }"""
          val viewCache = ViewCache(
            MountConfig.ViewConfig(expr, Variables.empty), lastUpdate.some, None, 0, None, None,
            maxAgeSecs.toLong, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, g, None)
          val mounts = Map[APath, MountConfig](f -> viewConfig0(expr))

          val (resp, vc) = evalViewTest(now, mounts, InMemState.empty) { (it, ir) =>
            (for {
              _ <- vcache.put(f, viewCache)
              a <- execute.service[ViewEff, Nothing](executionIdRef).apply(
                Request(uri = (pathUri(f) / "").copy(query =
                  http4s.Query.fromPairs(
                    "q" -> s"select * from `../${fileName(f).value}`"))))
              c <- vcache.get(f).run
            } yield (a.toHttpResponse(ir), c)).foldMap(it)
          }.unsafePerformSync

          resp.status must_= Status.Ok
          resp.headers.get(Warning) ∘ (_.value) must_= None
          resp.headers.get(Expires) ∘ (_.value) must_= None
          vc ∘ (_.cacheReads) must_= (viewCache.cacheReads ⊹ 1).some
        }
      }
    }
    "execute a query with offset and limit and a variable" >> {
      def queryAndExpectedLP(aFile: AFile, varName: AlphaCharacters, var_ : Int): (String, Fix[LogicalPlan]) = {
        val query = pprint(selectAllWithVar(file1(fileName(aFile)), varName.value))
        val inlineQuery = selectAllWithVar(aFile, varName.value)
        val lp = unsafeToLP(inlineQuery, Variables.fromMap(Map(varName.value -> var_.toString)))
        (query,lp)
      }
      "GET" >> prop { (
        filesystem: SingleFileMemState,
        varName: AlphaCharacters,
        var_ : Int,
        offset: Int Refined NonNegative,
        limit: Int Refined RPositive) =>
          (filesystem.file != rootDir </> file("sk(..)a/nZaxo/\"`oq_Jy.g.r.V{\\l") && varName.value != "im" && limit.value != 1 && offset.value != 0 && var_ != 0) ==> {
            import quasar.std.StdLib.set._

            val (query, lp) = queryAndExpectedLP(filesystem.file, varName, var_)
            val limitedLp =
              Fix(Take(
                Fix(Drop(
                  lp,
                  lpf.constant(Data.Int(offset.value)))),
                lpf.constant(Data.Int(limit.value))))
            val limitedContents =
              filesystem.contents
                .drop(offset.value)
                .take(limit.value)

            get(executeService)(
              path = filesystem.parent,
              query = Some(Query(
                query,
                offset = Some(offset),
                limit = Some(Positive(limit.value.toLong).get),
                varNameAndValue = Some((varName.value, var_.toString)))),
              state = filesystem.state.copy(queryResps = Map(limitedLp -> limitedContents)),
              status = Status.Ok,
              response = (a: String) => a must_==
                jsonReadableLine.encode(Process.emitAll(filesystem.contents): Process[Task, Data]).runLog.unsafePerformSync
                  .drop(offset.value).take(limit.value).mkString(""))
        }
      }.flakyTest("See precondition for example of offending arguments")
      "POST" >> prop { (filesystem: SingleFileMemState, varName: AlphaCharacters, var_ : Int, offset: Natural, limit: Positive, destination: FPath) =>
        val (query, lp) = queryAndExpectedLP(filesystem.file, varName, var_)
        val expectedDestinationPath = refineTypeAbs(destination).fold(ι, filesystem.parent </> _)
        post[AJson](liftFT[QueryFile])(
          path = filesystem.parent,
          query = Some(Query(query, offset = Some(offset), limit = Some(limit), varNameAndValue = Some((varName.value, var_.toString)))),
          destination = Some(destination),
          state = filesystem.state.copy(queryResps = Map(lp -> filesystem.contents)),
          status = Status.Ok,
          response = json => Json.parse(json.nospaces) must beLike { case json""" { "out": $outValue, "phases": $outPhases }""" =>
            outValue.as[String] must_== printPath(expectedDestinationPath)
          },
          stateCheck = Some(s => s.contents.keys must contain(expectedDestinationPath)))
      }
    }
    "execute a query with function definitions" >> {
      val sampleFile = rootDir </> file("foo")
      val query =
        """
          |CREATE FUNCTION TRIVIAL(:table)
          |  BEGIN
          |    SELECT * FROM :table
          |  END;
          |TRIVIAL(`/foo`)
        """.stripMargin
      get(executeService)(
        path = rootDir,
        query = Some(Query(query)),
        state = InMemState.fromFiles(Map(sampleFile -> Vector(Data.Int(5)))),
        status = Status.Ok,
        response = (a: String) => a.trim must_= "5")
    }
    "execute a query with imported functions" >> {
      "imported with an absolute path" >> {
        val sampleFile = rootDir </> file("foo")
        val funcDec = FunctionDecl(CIName("Trivial"), List(CIName("from")), sqlE"select * from :from")
        val query =
          """
            |import `/mymodule/`;
            |TRIVIAL(`/foo`)
          """.stripMargin
        get(executeService)(
          path = rootDir,
          query = Some(Query(query)),
          state = InMemState.fromFiles(Map(sampleFile -> Vector(Data.Int(5)))),
          mounts = Map((rootDir </> dir("mymodule"): APath) -> MountConfig.moduleConfig(List(funcDec))),
          status = Status.Ok,
          response = (a: String) => a.trim must_= "5")
      }
      "imported with a relative path" >> {
        val sampleFile = rootDir </> file("foo")
        val funcDec = FunctionDecl(CIName("Trivial"), List(CIName("from")), sqlE"select * from :from")
        val query =
          """
            |import `mymodule/`;
            |TRIVIAL(`/foo`)
          """.stripMargin
        get(executeService)(
          path = rootDir </> dir("foo"),
          query = Some(Query(query)),
          state = InMemState.fromFiles(Map(sampleFile -> Vector(Data.Int(5)))),
          mounts = Map((rootDir </> dir("foo") </> dir("mymodule"): APath) -> MountConfig.moduleConfig(List(funcDec))),
          status = Status.Ok,
          response = (a: String) => a.trim must_= "5")
      }
      "imported with a backtracking relative path" >> {
        val sampleFile = rootDir </> file("foo")
        val funcDec = FunctionDecl(CIName("Trivial"), List(CIName("from")), sqlE"select * from :from")
        val query =
          """
            |import `../../mymodule/`;
            |TRIVIAL(`/foo`)
          """.stripMargin
        get(executeService)(
          path = rootDir </> dir("foo") </> dir("bar"),
          query = Some(Query(query)),
          state = InMemState.fromFiles(Map(sampleFile -> Vector(Data.Int(5)))),
          mounts = Map((rootDir </> dir("mymodule"): APath) -> MountConfig.moduleConfig(List(funcDec))),
          status = Status.Ok,
          response = (a: String) => a.trim must_= "5")
      }
      "if the argument to the function is a relative path, it should be relative to the calling context as opposed to the location of the module definning the function" >> {
        val sampleFile = rootDir </> dir("foo") </> file("bar")
        val funcDec = FunctionDecl(CIName("Trivial"), List(CIName("from")), sqlE"select * from :from")
        val query =
          """
            |import `/baz/mymodule/`;
            |TRIVIAL(`bar`)
          """.stripMargin
        get(executeService)(
          path = rootDir </> dir("foo"),
          query = Some(Query(query)),
          state = InMemState.fromFiles(Map(sampleFile -> Vector(Data.Int(5)))),
          mounts = Map((rootDir </> dir("baz") </> dir("mymodule"): APath) -> MountConfig.moduleConfig(List(funcDec))),
          status = Status.Ok,
          response = (a: String) => a.trim must_= "5")
      }
    }
    "fail on a query that has ambiguous imports" >> {
      val funcDec = {
        val selectAll = sqlE"select * from `foo`"
        FunctionDecl(CIName("Trivial"), List(CIName("user_id")), selectAll)
      }
      val query =
        """
          |import `/mymodule/`;
          |import `/otherModule/`;
          |TRIVIAL("bob")
        """.stripMargin
      get(executeService)(
        path = rootDir,
        query = Some(Query(query)),
        state = InMemState.empty,
        mounts = Map(
          (rootDir </> dir("mymodule"): APath)    -> MountConfig.moduleConfig(List(funcDec)),
          (rootDir </> dir("otherModule"): APath) -> MountConfig.moduleConfig(List(funcDec))),
        status = Status.BadRequest,
        response = (_: AJson) must_=== AJson(
          "error" := AJson(
            "status" := "Ambiguous function call",
            "detail" := AJson(
              "message" := "Function call `TRIVIAL` is ambiguous because the following functions: /mymodule/Trivial, /otherModule/Trivial could be applied here",
              "invoke"  := "TRIVIAL",
              "ambiguous functions"   := List("/mymodule/Trivial", "/otherModule/Trivial")))))
    }
    // TODO: Consider changing this behavior
    "if the query has a user defined functions that shadows a standard library function, it will choose the user defined function (consider changing this behavior)" >> {
      val funcDec = FunctionDecl(CIName("SUM"), List(CIName("a")), sqlE"1")
      val query =
        """
          |import `/mymodule/`;
          |SUM(`/foo`)
        """.stripMargin
      get(executeService)(
        path = rootDir,
        query = Some(Query(query)),
        state = InMemState.empty,
        mounts = Map((rootDir </> dir("mymodule"): APath) -> MountConfig.moduleConfig(List(funcDec))),
        status = Status.Ok,
        response = (a: String) => a.trim must_= "1")
    }
    "POST (error conditions)" >> {
      "be 404 for missing directory" >> prop { (dir: ADir, destination: AFile, filename: FileName) =>
        post[String](liftFT[QueryFile])(
          path = dir,
          query = Some(Query(selectAll(file(filename.value)))),
          destination = Some(destination),
          state = InMemState.empty,
          status = Status.NotFound,
          response = _ must_== "???")
      }.pendingUntilFixed("SD-773")
      "be 400 with missing query" >> prop { (filesystem: SingleFileMemState, destination: AFile) =>
        post[ApiError](liftFT[QueryFile])(
          path = filesystem.parent,
          query = None,
          destination = Some(destination),
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must equal(ApiError.fromStatus(
            Status.BadRequest withReason "No SQL^2 query found in message body.")))
      }
      "be 400 with missing Destination header" >> prop { filesystem: SingleFileMemState =>
        post[ApiError](liftFT[QueryFile])(
          path = filesystem.parent,
          query = Some(Query(selectAll(file(filesystem.filename.value)))),
          destination = None,
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must beHeaderMissingError("Destination"))
      }
      "be 400 for query error" >> prop { (filesystem: SingleFileMemState, destination: AFile) =>
        post[ApiError](liftFT[QueryFile])(
          path = filesystem.parent,
          query = Some(Query("select date where")),
          destination = Some(destination),
          state = filesystem.state,
          status = Status.BadRequest,
          response = _ must beApiErrorWithMessage(
            Status.BadRequest withReason "Malformed SQL^2 query."))
      }
      "be 400 for compile error" >> prop { (fs: SingleFileMemState, dst: AFile) =>
        val q = "select sum(1, 2, 3, 4)"

        val err: SemanticError =
          SemanticError.WrongArgumentCount(CIName("sum"), 1, 4)

        val expr: Fix[Sql] = sql.fixParser.parseExpr(q).valueOr(
          err => scala.sys.error("Parse failed: " + err.toString))

        val phases: PhaseResults =
          queryPlan(expr, Variables.empty, rootDir, 0L, None).run.written

        post[ApiError](liftFT[QueryFile])(
          path = fs.parent,
          query = Some(Query(q)),
          destination = Some(dst),
          state = fs.state,
          status = Status.BadRequest,
          response = _ must equal(NonEmptyList(err).toApiError :+ ("phases" := phases)))
      }
      "be 500 for execution error" >> {
        val q = "select * from `/foo`"
        val expr: Fix[Sql] = sql.fixParser.parseExpr(q).valueOr(
          err => scala.sys.error("Parse failed: " + err.toString))
        val lp = unsafeToLP(expr)
        val msg = "EXEC FAILED"
        val err = executionFailed_(lp, msg)

        val phases: PhaseResults =
          queryPlan(expr, Variables.empty, rootDir, 0L, None).run.written

        post[ApiError](failingExecPlan(msg))(
          path = rootDir,
          query = Some(Query(q)),
          destination = Some(rootDir </> file("outA")),
          state = InMemState.empty,
          status = Status.InternalServerError,
          response = _ must equal(err.toApiError :+ ("phases" := phases)))
      }
    }
  }
}
