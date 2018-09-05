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

package quasar.regression

import slamdata.Predef._
import quasar._
import quasar.api.datasource._
import quasar.build.BuildInfo
import quasar.common.PhaseResults
import quasar.common.data.Data
import quasar.contrib.fs2.convert
import quasar.contrib.nio.{file => contribFile}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.{MonadError_, MonadTell_}
import quasar.fp._
import quasar.frontend.data.DataCodec
import quasar.impl.datasource.local.LocalType
import quasar.impl.external.ExternalConfig
import quasar.mimir.Precog
import quasar.run.{Quasar, QuasarError, SqlQuery}
import quasar.run.implicits._
import quasar.sql.Query

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.matching.Regex

import java.math.{MathContext, RoundingMode}
import java.nio.file.{Files, Path => JPath, Paths}
import java.util.UUID

import argonaut._, Argonaut._
import cats.effect.{Effect, IO, Sync, Timer}
import eu.timepit.refined.auto._
import fs2.{io, text, Stream}
import fs2.async.Promise
import matryoshka._
import org.specs2.execute
import org.specs2.specification.core.Fragment
import pathy.Path, Path._
import scalaz._, Scalaz._
// import shims._ causes compilation to not terminate in any reasonable amount of time.
import shims.{monadErrorToScalaz, monadToScalaz}

final class Sql2QueryRegressionSpec extends Qspec {
  import Sql2QueryRegressionSpec._

  implicit val ignorePhaseResults: MonadTell_[IO, PhaseResults] =
    MonadTell_.ignore[IO, PhaseResults]

  implicit val ioQuasarError: MonadError_[IO, QuasarError] =
    MonadError_.facet[IO](QuasarError.throwableP)

  implicit val streamQuasarError: MonadError_[Stream[IO, ?], QuasarError] =
    MonadError_.facet[Stream[IO, ?]](QuasarError.throwableP)

  def DataDir(id: UUID) =
    rootDir[Sandboxed] </> dir("datasource") </> dir(id.toString)

  /** A name to identify the suite in test output. */
  val suiteName: String = "SQL^2 Regression Queries"

  val Q = for {
    tmpPath <-
      Stream.bracket(IO(Files.createTempDirectory("quasar-test-")))(
        Stream.emit(_),
        contribFile.deleteRecursively[IO](_))

    precog <- Precog.stream(tmpPath.toFile)

    q <- Quasar[IO](precog, ExternalConfig.Empty, 1L)

    localCfg =
      ("rootDir" := posixCodec.printPath(TestDataRoot)) ->:
      // 64 KB chunks, chosen mostly arbitrarily.
      ("readChunkSizeBytes" := 65535) ->:
      jEmptyObject

    localRef =
      DatasourceRef(LocalType, DatasourceName("local"), localCfg)

    r <- Stream.eval(q.datasources.addDatasource(localRef))

    i <- r.fold(
      e => MonadError_[Stream[IO, ?], DatasourceError.CreateError[Json]].raiseError(e),
      i => Stream.emit(i).covary[IO])

  } yield (q, i)

  ////

  val buildSuite =
    for {
      tdef <- Promise.empty[IO, (Quasar[IO], UUID)]
      sdown <- Promise.empty[IO, Unit]

      tests <- regressionTests[IO](TestsRoot, TestDataRoot)

      _ <- Q.evalMap(t => tdef.complete(t) *> sdown.get).compile.drain.start
      t <- tdef.get
      (q, i) = t

      f = (squery: UUID => SqlQuery) =>
        (Stream.force {
          q.queryEvaluator.evaluate(squery(i)).map(_.map(mimir.tableToData))
        }).flatMap(s => s)
    } yield {
      suiteName >> {
        tests.toList foreach { case (loc, test) =>
          regressionExample(loc, test, BackendName("lwc_local"), f)
        }

        step(sdown.complete(()).unsafeRunSync())
      }
    }

  buildSuite.unsafeRunSync()

  ////

  /** Returns an `Example` verifying the given `RegressionTest`. */
  def regressionExample(
      loc: RFile,
      test: RegressionTest,
      backendName: BackendName,
      results: (UUID => SqlQuery) => Stream[IO, Data])
      : Fragment = {
    def runTest: execute.Result = {
      val query = (id: UUID) =>
        SqlQuery(
          Query(test.query),
          Variables.fromMap(test.variables),
          test.data.nonEmpty.fold(DataDir(id) </> fileParent(loc), rootDir))

      verifyResults(test.expected, results(query), backendName)
        .unsafeRunTimed(2.minutes)
        .getOrElse(execute.StandardResults.failure("Timed out."))
    }

    s"${test.name} [${posixCodec.printPath(loc)}]" >> {
      collectFirstDirective(test.backends, backendName) {
        case TestDirective.Skip    => skipped
        case TestDirective.SkipCI  =>
          BuildInfo.isCIBuild.fold(execute.Skipped("(skipped during CI build)"), runTest)
        case TestDirective.Pending | TestDirective.PendingIgnoreFieldOrder =>
          runTest.pendingUntilFixed
      } getOrElse runTest
    }
  }

  /** Verify the given results according to the provided expectation. */
  def verifyResults[F[_]: Sync](
      exp: ExpectedResult,
      act: Stream[F, Data],
      backendName: BackendName)
      : F[execute.Result] = {

    val deleteFields: Json => Json =
      _.withObject(exp.ignoredFields.foldLeft(_)(_ - _))

    val fieldOrderSignificance =
      if (exp.ignoreFieldOrder)
        OrderIgnored
      else
        collectFirstDirective[OrderSignificance](exp.backends, backendName) {
          case TestDirective.IgnoreAllOrder |
               TestDirective.IgnoreFieldOrder |
               TestDirective.PendingIgnoreFieldOrder =>
            OrderIgnored
        } | OrderPreserved

    val resultOrderSignificance =
      if (exp.ignoreResultOrder)
        OrderIgnored
      else
        collectFirstDirective[OrderSignificance](exp.backends, backendName) {
          case TestDirective.IgnoreAllOrder |
               TestDirective.IgnoreResultOrder |
               TestDirective.PendingIgnoreFieldOrder =>
            OrderIgnored
        } | OrderPreserved

    // TODO{fs2}: Chunkiness
    val actNormal =
      act.mapChunks(
        _.map(deleteFields <<< (_.asJson))
          .toSegment)

    exp.predicate(
      exp.rows,
      actNormal,
      fieldOrderSignificance,
      resultOrderSignificance)
  }

  /** Returns all the `RegressionTest`s found in the given directory, keyed by
    * file path.
    */
  def regressionTests[F[_]: Effect: Timer](
      testDir: RDir,
      dataDir: RDir)
      : F[Map[RFile, RegressionTest]] =
    descendantsMatching[F](testDir, TestPattern)
      .flatMap(f => loadRegressionTest[F](f).map((f.relativeTo(dataDir).get, _)))
      .compile
      .fold(Map[RFile, RegressionTest]())(_ + _)

  /** Loads a `RegressionTest` from the given file. */
  def loadRegressionTest[F[_]: Effect: Timer](file: RFile): Stream[F, RegressionTest] =
    // all test files right now fit into 8K, which is a reasonable chunk size anyway
    io.file.readAllAsync[F](jPath(file), 8192)
      .through(text.utf8Decode)
      .reduce(_ + _)
      .flatMap(txt =>
        decodeJson[RegressionTest](txt).fold(
          err => Stream.raiseError(new RuntimeException(posixCodec.printPath(file) + ": " + err)),
          Stream.emit).covary[F])

  /** Returns all descendant files in the given dir matching `pattern`. */
  def descendantsMatching[F[_]: Sync](d: RDir, pattern: Regex): Stream[F, RFile] =
    convert.fromJavaStream(Sync[F].delay(Files.walk(jPath(d))))
      .filter(p => pattern.findFirstIn(p.getFileName.toString).isDefined)
      .map(p => posixCodec.parseRelFile(p.toString).flatMap(_.relativeTo(d)))
      .unNone
      .map(d </> _)

  /** Whether the backend has the specified directive in the given test. */
  def hasDirective(td: TestDirective, dirs: Directives, name: BackendName): Boolean =
    Foldable[Option].compose[NonEmptyList].any(dirs get name)(_ ≟ td)

  /** Returns the result of the given partial function applied to the first directive
    * for which it is defined, or `None` if it is undefined for all directives.
    */
  def collectFirstDirective[A](
      dirs: Directives,
      name: BackendName)(
      pf: PartialFunction[TestDirective, A])
      : Option[A] =
    Foldable[Option].compose[NonEmptyList]
      .findMapM[Id, TestDirective, A](dirs get name)(pf.lift)(idInstance)

  def jPath(p: Path[_, _, Sandboxed]): JPath =
    Paths.get(posixCodec.printPath(p))
}

object Sql2QueryRegressionSpec {
  val TestDataRoot: RDir =
    currentDir[Sandboxed] </> dir("it") </> dir("src") </> dir("main") </> dir("resources") </> dir("tests")

  val TestsRoot: RDir =
    TestDataRoot

  val TestPattern: Regex =
    """^([^.].*)\.test""".r

  val TestContext: MathContext =
    new MathContext(13, RoundingMode.DOWN)

  implicit val dataEncodeJson: EncodeJson[Data] =
    EncodeJson(DataCodec.Precise.encode(_).getOrElse(jString("Undefined")))
}
