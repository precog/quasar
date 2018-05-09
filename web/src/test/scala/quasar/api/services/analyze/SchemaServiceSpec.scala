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

package quasar.api.services.analyze

import slamdata.Predef._
import quasar.{Data, DataGenerators, DataCodec}
import quasar.api._, ApiErrorEntityDecoder._, PathUtils._
import quasar.api.matchers._
import quasar.contrib.matryoshka.arbitrary._
import quasar.contrib.pathy._
import quasar.effect._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}
import quasar.fs._, InMemory.InMemState
import quasar.fs.mount._
import quasar.main.analysis
import quasar.sst._
import quasar.std.IdentityLib

import argonaut._, Argonaut._, JsonScalaz._
import eu.timepit.refined.auto._
import eu.timepit.refined.scalacheck.numeric._
import eu.timepit.refined.scalaz._
import matryoshka._
import matryoshka.data.Fix
import org.http4s._
import org.http4s.argonaut._
import org.http4s.dsl._
import org.http4s.headers._
import org.http4s.util.{NonEmptyList => HNel}
import org.scalacheck._
import org.specs2.specification.core.Fragments
import pathy.Path.{dir, file, posixCodec, rootDir}
import pathy.argonaut.PosixCodecJson._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{process1, Process}
import spire.std.double._

final class SchemaServiceSpec extends quasar.Qspec with FileSystemFixture with Http4s {
  import SchemaServiceSpec._
  import DataGenerators._

  type J = Fix[EJson]

  val lpr = new LogicalPlanR[Fix[LogicalPlan]]

  def simpleRead(file: AFile): Fix[LogicalPlan] =
    lpr.invoke1(IdentityLib.Squash, lpr.read(file))

  def stateForFile(file: AFile, contents: Vector[Data]) =
    InMemState.empty.copy(queryResps = Map(
      analysis.sampled(simpleRead(file), schema.DefaultSampleSize) -> contents
    ))

  def baseUri(file: AFile) =
    pathUri(rootDir) +? ("q", s"select * from `${posixCodec.printPath(file)}`")

  def nonPos(i: Int): Int =
    if (i > 0) -i else i

  def nonNat(i: Int): Int =
    if (i ≟ 0) -1 else nonPos(i)

  def sstResponse(dataset: Vector[Data], cfg: analysis.CompressionSettings): Json = {
    type P[X] = StructuralType[J, X]

    Process.emitAll(dataset)
      .pipe(analysis.extractSchema[J, Double](cfg))
      .map(Population.subst[P, TypeStat[Double]] _)
      .map(psst => DataCodec.Precise.encode(analysis.schemaToData(psst.right)))
      .pipe(process1.stripNone)
      .toVector.headOption.getOrElse(Json.jNull)
  }

  case class SmallNatural(n: Natural)
  case class SmallPositive(p: Positive)

  implicit val smallNaturalArbitrary: Arbitrary[SmallNatural] =
    Arbitrary(chooseRefinedNum(0L: Natural, 10L: Natural) map (SmallNatural(_)))

  implicit val smallPositiveArbitrary: Arbitrary[SmallPositive] =
    Arbitrary(chooseRefinedNum(1L: Positive, 10L: Positive) map (SmallPositive(_)))

  "respond with bad request when" >> {
    "file path given" >> prop { file: AFile =>
      service(InMemState.empty)(Request(uri = pathUri(file) +? ("q", "select 1")))
        .flatMap(_.as[ApiError])
        .map(_ must beApiErrorWithMessage(
          BadRequest withReason "Directory path expected.",
          "path" := file))
        .unsafePerformSync
    }

    "query missing" >> prop { dir: ADir =>
      service(InMemState.empty)(Request(uri = pathUri(dir)))
        .flatMap(_.as[ApiError])
        .map(_ must equal(ApiError.fromStatus(
          BadRequest withReason "No SQL^2 query found in URL.")))
        .unsafePerformSync
    }

    "query malformed" >> prop { dir: ADir =>
      service(InMemState.empty)(Request(uri = pathUri(dir) +? ("q", "select foo from")))
        .flatMap(_.as[ApiError])
        .map(_ must beApiErrorWithMessage(
          BadRequest withReason "Malformed SQL^2 query."))
        .unsafePerformSync
    }

    "variable unbound" >> prop { dir: ADir =>
      service(InMemState.empty)(Request(uri = pathUri(dir) +? ("q", "select * from :foo")))
        .flatMap(_.as[ApiError])
        .map(_ must beApiErrorWithMessage(
          BadRequest withReason "Unbound variable.",
          "varName" := "foo"))
        .unsafePerformSync
    }

    val nonNaturalInvalid =
      List("arrayMaxLength", "mapMaxSize", "stringMaxLength") map { param =>
        s"non-natural $param given" >> prop { (file: AFile, i: Int) =>
          val ruri = pathUri(file) +? (param, nonNat(i).toString)

          service(InMemState.empty)(Request(uri = ruri))
            .flatMap(_.as[ApiError])
            .map(_ must beApiErrorWithMessage(
              BadRequest withReason "Invalid query parameter."))
            .unsafePerformSync
        }
      }

    val nonPositiveInvalid =
      List("unionMaxSize") map { param =>
        s"non-positive $param given" >> prop { (file: AFile, i: Int) =>
          val ruri = pathUri(file) +? (param, nonPos(i).toString)

          service(InMemState.empty)(Request(uri = ruri))
            .flatMap(_.as[ApiError])
            .map(_ must beApiErrorWithMessage(
              BadRequest withReason "Invalid query parameter."))
            .unsafePerformSync
        }
      }

    addFragments(Fragments(nonNaturalInvalid ::: nonPositiveInvalid : _*))
  }

  "successful response" >> {
    val testFile = rootDir </> dir("foo") </> file("bar")

    def shouldSucceed(x: Data, y: Data, cfg: analysis.CompressionSettings, req: Request) = {
      val dataset = Vector.fill(5)(x) ++ Vector.fill(5)(y)
      val mt = JsonFormat.LineDelimited.mediaType.withExtensions(Map("mode" -> "precise"))
      val accept = Accept(HNel(MediaRangeAndQValue.withDefaultQValue(mt)))

      service(stateForFile(testFile, dataset))(req.transformHeaders(_.put(accept)))
        .flatMap(_.as[Json])
        .map(_ must_= sstResponse(dataset, cfg))
        .unsafePerformSync
    }

    def testReq(f: Uri => Uri) =
      Request(uri = f(baseUri(testFile)))

    "includes the sst for the query results as JSON-encoded EJson" >> prop { (x: Data, y: Data) =>
      shouldSucceed(x, y,
        analysis.CompressionSettings.Default,
        testReq(a => a))
    }

    "applies arrayMaxLength when specified" >> prop { (x: Data, y: Data, len: SmallNatural) =>
      val cfg = analysis.CompressionSettings.Default.copy(arrayMaxLength = len.n)
      shouldSucceed(x, y, cfg, testReq(_ +? ("arrayMaxLength", len.n.shows)))
    }

    "applies mapMaxSize when specified" >> prop { (x: Data, y: Data, size: SmallNatural) =>
      val cfg = analysis.CompressionSettings.Default.copy(mapMaxSize = size.n)
      shouldSucceed(x, y, cfg, testReq(_ +? ("mapMaxSize", size.n.shows)))
    }

    "applies stringMaxLength when specified" >> prop { (x: Data, y: Data, len: SmallNatural) =>
      val cfg = analysis.CompressionSettings.Default.copy(stringMaxLength = len.n)
      shouldSucceed(x, y, cfg, testReq(_ +? ("stringMaxLength", len.n.shows)))
    }

    "applies unionMaxSize when specified" >> prop { (x: Data, y: Data, size: SmallPositive) =>
      val cfg = analysis.CompressionSettings.Default.copy(unionMaxSize = size.p)
      shouldSucceed(x, y, cfg, testReq(_ +? ("unionMaxSize", size.p.shows)))
    }

    "has an empty body when query has no results" >> {
      service(stateForFile(testFile, Vector()))(testReq(a => a))
        .flatMap(_.as[String])
        .map(_ must beEmpty)
        .unsafePerformSync
    }
  }
}

object SchemaServiceSpec {
  type SchemaEff[A] = (Mounting :\: QueryFile :\: FileSystemFailure :/: Task)#M[A]

  def runQueryFile(mem: InMemState): Task[QueryFile ~> FailedResponseOr] =
    InMemory.runStatefully(mem) map { eval =>
      liftMT[Task, FailedResponseT] compose eval compose InMemory.queryFile
    }

  val runMounting: Task[Mounting ~> FailedResponseOr] =
    KeyValueStore.impl.default[APath, MountConfig] map { eval =>
      liftMT[Task, FailedResponseT] compose foldMapNT(eval) compose Mounter.trivial[MountConfigs]
    }

  def service(mem: InMemState): Service[Request, Response] =
    HttpService.lift(req => runQueryFile(mem).tuple(runMounting) flatMap {
      case (runQF, runM) =>
        schema.service[SchemaEff].toHttpService(
          runM                                                     :+:
          runQF                                                    :+:
          FailedResponse.fromFailureMessage[Task, FileSystemError] :+:
          liftMT[Task, FailedResponseT]
        )(req)
    }).orNotFound
}
