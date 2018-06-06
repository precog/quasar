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
import quasar.build.BuildInfo
import quasar.common._
import quasar.compile.{SemanticError, SemanticErrsT}
import quasar.contrib.argonaut._
import quasar.contrib.pathy.Helpers._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.writerT._
import quasar.ejson
import quasar.ejson.Common.{Optics => CO}
import quasar.contrib.pathy._
import quasar.fp._, free._
import quasar.contrib.iota._
import quasar.fp.ski._
import quasar.fs._
import quasar.main.{physicalFileSystems, CompExec, FilesystemQueries}
import quasar.fs.mount.{Mounts, hierarchical}
import quasar.sql, sql.Sql

import java.math.{MathContext, RoundingMode}
import scala.concurrent.duration._
import scala.util.matching.Regex

import argonaut._, Argonaut._
import matryoshka._
import matryoshka.data.Fix
import org.specs2.execute
import org.specs2.specification.core.Fragment
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

abstract class QueryRegressionTest[S[_]](
  fileSystems: Task[IList[SupportedFs[S]]])(
  implicit S0: QueryFile :<: S, S1: ManageFile :<: S,
           S2: ReadFile  :<: S, S3: WriteFile  :<: S,
           S4: Task :<: S
) extends FileSystemTest[S](fileSystems) {

  import QueryRegressionTest._

  // NB: forcing the examples to not overlap seems to avoid some errors in Travis
  sequential

  type FsErr[A] = FileSystemErrT[F, A]

  val CE = CompExec[F]
  import CE._

  val injectTask: Task ~> F =
    liftFT[S].compose(injectNT[Task, S])

  val TestDataRoot: RDir =
    currentDir[Sandboxed] </> dir("it") </> dir("src") </> dir("main") </> dir("resources") </> dir("tests")

  lazy val TestsRoot: RDir =
    TestsDir.fold(TestDataRoot)(TestDataRoot </> _)

  val DataDir: ADir = rootDir </> dir("regression")

  /** Location on the (host) file system of the data file referred to from a
    * test, if it's a relative path pointing into the test directory. */
  def resolveData(test: RFile, data: RelFile[Unsandboxed]): String \/ RFile =
    sandbox(currentDir, unsandbox(fileParent(test)) </> data) \/>
      s"Can't sandbox data file to test dir: ${data.shows}"

  /** Location in the (Quasar) filesystem where a test dataset will be stored. */
  def dataFile(file: RFile): AFile =
    renameFile(DataDir </> file, _.dropExtension)

  val query  = QueryFile.Ops[S]
  val write  = WriteFile.Ops[S]
  val manage = ManageFile.Ops[S]
  val fsQ    = new FilesystemQueries[S]

  /** The location of the test files. */
  def TestsDir: Option[Path[Rel, Dir, Sandboxed]]

  /** A name to identify the suite in test output. */
  def suiteName: String

  /** Return the results of evaluating the given query as a stream. */
  def queryResults(expr: Fix[Sql], vars: Variables, basePath: ADir): Process[CompExecM, Data]

  ////

  lazy val tests = regressionTests(TestDataRoot, TestsRoot, knownFileSystems).unsafePerformSync

  // NB: The printing is just to indicate progress (especially for travis-ci) as
  //     these tests have the potential to be slow for a backend.
  //
  //     Ideally, we'd have specs2 log each example in the suite as it finishes, but
  //     all attempts at doing this have been unsuccessful, if we succeed eventually
  //     this printing can be removed.
  fileSystemShould { (fs, fsNonChrooted) =>
    suiteName should {
      step(print(s"Running $suiteName ["))

      tests.toList foreach { case (f, t) =>
        val fsʹ = t.data.nonEmpty.fold(fs, fsNonChrooted)
        regressionExample(f, t, fsʹ.ref.name, fsʹ.setupInterpM, fsʹ.testInterpM)
        step(print("."))
      }

      step(println("]"))
      step(doDelete(fs.setupInterpM, DataDir).runVoid)
    }
  }

  ////

  /** Returns an `Example` verifying the given `RegressionTest`. */
  def regressionExample(
    loc: RFile,
    test: RegressionTest,
    backendName: BackendName,
    setup: Run,
    run: Run
  ): Fragment = {
    def runTest: execute.Result = {
      val data = testQuery(
        test.data.nonEmpty.fold(DataDir </> fileParent(loc), rootDir),
        test.query,
        test.variables)

      (test.data.nonEmpty.whenM(ensureTestData(loc, test, setup)) *>
       verifyResults(test.expected, data, run, backendName))
        .timed(5.minutes)
        .unsafePerformSync
    }

    s"${test.name} [${posixCodec.printPath(loc)}]" >> {
      collectFirstDirective(test.backends, backendName) {
        case TestDirective.Skip    => skipped
        case TestDirective.SkipCI  =>
          BuildInfo.isCIBuild.fold(execute.Skipped("(skipped during CI build)"), runTest)
        case TestDirective.Timeout =>
          // NB: To locally skip tests that time out, make `Skipped` unconditional.
          BuildInfo.isCIBuild.fold(
            execute.Skipped("(skipped because it times out)"),
            runTest)
        case TestDirective.Pending | TestDirective.PendingIgnoreFieldOrder =>
          runTest.pendingUntilFixed
      } getOrElse runTest
    }
  }

  /** Ensures the data for the given test exists in the Quasar filesystem, inserting
    * it if not. Returns whether any data was inserted.
    */
  def ensureTestData(testLoc: RFile, test: RegressionTest, run: Run): Task[Boolean] = {
    def ensureTestFile(loc: RFile): Task[Boolean] =
      run(query.fileExists(dataFile(loc))).ifM(
        false.point[Task],
        loadTestData(loc, run).as(true))

    val locs: Set[RFile] = test.data.flatMap(resolveData(testLoc, _).toOption).toSet

    Task.gatherUnordered(locs.toList map ensureTestFile) map (_ any ι)
  }

  val TestContext = new MathContext(13, RoundingMode.DOWN)

  /** This helps us get identical results on different connectors, even though
    * they have different precisions for their floating point values.
    */
  val reducePrecision =
    λ[EndoK[ejson.Common]](CO.dec.modify(_.round(TestContext))(_))

  val normalizeJson: Json => Json =
    j => Recursive[Json, ejson.Json].transCata(j)(liftFFCopK(reducePrecision[Json]))

  /** Verify the given results according to the provided expectation. */
  def verifyResults(
    exp: ExpectedResult,
    act: Process[CompExecM, Data],
    run: Run,
    backendName: BackendName
  ): Task[execute.Result] = {

    type H1[A] = PhaseResultT[Task, A]
    type H2[A] = SemanticErrsT[H1, A]
    type H3[A] = FileSystemErrT[H2, A]

    val h1: G ~> H1 = Hoist[PhaseResultT].hoist(run)
    val h2: H ~> H2 = Hoist[SemanticErrsT].hoist(h1)
    val h3: CompExecM ~> H3 = Hoist[FileSystemErrT].hoist(h2)

    val liftRun = λ[CompExecM ~> Task](fa => {
      val w = rethrow[H1, NonEmptyList[SemanticError]].apply(rethrow[H2, FileSystemError].apply(h3(fa)))
      // TODO: Make it so this only prints out for _failed_ tests.
      // TODO: Make this configurable via an option to `it/test`
      // NB: Uncomment this line to print out the PhaseResults of each test
      //     (really only useful with `testOnly`).
      // (w.written ∘ (w => println(w.shows))) >>
      w.value
    })

    val deleteFields: Json => Json =
      _.withObject(exp.ignoredFields.foldLeft(_)(_ - _))

    val result =
      exp.predicate(
        exp.rows.toVector,
        act.map(normalizeJson <<< deleteFields <<< (_.asJson)).translate[Task](liftRun),

        // TODO: Error if a backend ignores field order when the query already does.
        if (exp.ignoreFieldOrder)
          OrderIgnored
        else
          collectFirstDirective[OrderSignificance](exp.backends, backendName) {
            case TestDirective.IgnoreAllOrder | TestDirective.IgnoreFieldOrder | TestDirective.PendingIgnoreFieldOrder =>
              OrderIgnored
          } | OrderPreserved,

        if (exp.ignoreResultOrder)
          OrderIgnored
        else
          collectFirstDirective[OrderSignificance](exp.backends, backendName) {
            case TestDirective.IgnoreAllOrder | TestDirective.IgnoreResultOrder | TestDirective.PendingIgnoreFieldOrder =>
              OrderIgnored
          } | OrderPreserved)

    collectFirstDirective(exp.backends, backendName) {
      case TestDirective.Timeout =>
        result.map {
          case execute.Success(_, _) =>
            execute.Failure(s"Fixed now, you should remove the “timeout” status.")
          case execute.Failure(m, _, _, _) =>
            execute.Failure(s"Failed with “$m”, you should change the “timeout” status.")
          case x => x
        }.handle {
          case e: java.util.concurrent.TimeoutException => execute.Pending(s"times out: ${e.getMessage}")
          case e => execute.Failure(s"Errored with “${e.getMessage}”, you should change the “timeout” status to “pending”.")
        }
    } getOrElse result.handle {
      case e: java.util.concurrent.TimeoutException =>
        execute.Failure(s"Times out (${e.getMessage}), you should use the “timeout” status.")
    }
  }

  /** Parse and execute the given query, returning a stream of results. */
  def testQuery(
    loc: ADir,
    qry: String,
    vars: Map[String, String]
  ): Process[CompExecM, Data] = {
    val f: Task ~> CompExecM =
      toCompExec compose injectTask

    val parseTask: Task[Fix[Sql]] =
      sql.fixParser.parseExpr(qry)
        .fold(e => Task.fail(new RuntimeException(e.message)),
        _.mkPathsAbsolute(loc).point[Task])

    Process.await(f(parseTask))(queryResults(_, Variables.fromMap(vars), loc))
  }

  /** Load the contents of the test data file into the filesytem under test at
    * the same path, relative to the test `DataDir`.
    *
    * Any failures during loading will result in a failed `Task`.
    */
  def loadTestData(dataLoc: RFile, run: Run): Task[Unit] = {
    val throwFsError = rethrow[Task, FileSystemError]

    val load = write.createChunked(
      dataFile(dataLoc),
      testData(dataLoc).chunk(500).translate(injectTask)
    ).translate[Task](throwFsError.compose[FsErr](runT(run)))

    load.run handleWith {
      case err =>
        val msg = s"Failed loading test data '${posixCodec.printPath(dataLoc)}': ${err.getMessage}"
        Task.fail(new RuntimeException(msg, err))
    }
  }

  /** Returns a stream of `Data` representing the lines of the given data file. */
  def testData(file: RFile): Process[Task, Data] = {
    def parse(line: String): Process0[Data] =
      DataCodec.parse(line)(DataCodec.Precise).fold(
        err => Process.fail(new RuntimeException(
                 s"Failed to parse `$line` as Data: ${err.message}"
               )),
        Process.emit(_))

    val jf = jFile(TestDataRoot </> file)

    Task.delay(jf.exists).liftM[Process].ifM(
      io.linesR(new FileInputStream(jf)) flatMap parse,
      Process.fail(new java.io.FileNotFoundException(jf.getPath)))
  }

  /** Returns all the `RegressionTest`s found in the given directory, keyed by
    * file path.
    */
  def regressionTests(
    dataDir: RDir,
    testDir: RDir,
    knownBackends: Set[BackendName]
  ): Task[Map[RFile, RegressionTest]] =
    descendantsMatching(testDir, """^([^.].*)\.test""".r) // don't match file names with a leading .
      .map(f =>
        (loadRegressionTest(f) >>= verifyBackends(knownBackends)) strengthL
          (f relativeTo dataDir).get)
      .gather(4)
      .runLog
      .map(_.toMap)

  /** Verifies there are no unknown backends specified in a `RegressionTest`. */
  def verifyBackends(knownBackends: Set[BackendName])
                    : RegressionTest => Task[RegressionTest] = { rt =>

    val unknown = rt.backends.keySet diff knownBackends
    def errMsg = s"Unrecognized backend(s) in '${rt.name}': ${unknown.mkString(", ")}"

    if (unknown.isEmpty) Task.now(rt)
    else Task.fail(new RuntimeException(errMsg))
  }

  /** Loads a `RegressionTest` from the given file. */
  def loadRegressionTest(file: RFile): Task[RegressionTest] =
    textContents(file) flatMap { text =>
      decodeJson[RegressionTest](text) fold (
        err => Task.fail(new RuntimeException(file.shows + ": " + err)),
        Task.now(_))
    }

  /** Returns all descendant files in the given dir matching `pattern`. */
  def descendantsMatching(d: RDir, pattern: Regex): Process[Task, RFile] =
    Process.eval(Task.delay(jFile(d).listFiles.toVector))
      .flatMap(Process.emitAll(_))
      .flatMap(f =>
        if (f.isDirectory)
          descendantsMatching(d </> dir(f.getName), pattern)
        else if (pattern.findFirstIn(f.getName).isDefined)
          Process.emit(d </> file(f.getName))
        else
          Process.halt)

  /** Whether the backend has the specified directive in the given test. */
  private def hasDirective(td: TestDirective, dirs: Directives, name: BackendName): Boolean =
    Foldable[Option].compose[NonEmptyList].any(dirs get name)(_ ≟ td)

  /** Returns the result of the given partial function applied to the first directive
    * for which it is defined, or `None` if it is undefined for all directives.
    */
  private def collectFirstDirective[A]
      (dirs: Directives, name: BackendName)
      (pf: PartialFunction[TestDirective, A])
      : Option[A] =
    Foldable[Option].compose[NonEmptyList]
      .findMapM[Id, TestDirective, A](dirs get name)(pf.lift)
}

object QueryRegressionTest {
  lazy val knownFileSystems = TestConfig.backendRefs.map(_.name).toSet

  val externalFS: Task[IList[SupportedFs[BackendEffectIO]]] =
    for {
      loadConfig <- TestConfig.testBackendConfig
      mounts <- physicalFileSystems(loadConfig)
      uts    <- (Functor[Task] compose Functor[IList]).map(FileSystemTest.externalFsUT(mounts))(_.liftIO)
      mntDir =  rootDir </> dir("hfs-mnt")
      hfsUts <- uts.traverse(sb => sb.impl.map(ut =>
                  hierarchicalFSIO(mntDir, ut.testInterp).map { f: BackendEffectIO ~> Task =>
                    SupportedFs(
                      sb.ref,
                      ut.copy(testInterp = f)
                        .contramapF(chroot.fileSystem[BackendEffectIO](ut.testDir))
                        .some,
                      ut.some)
                  }
                ).getOrElse(sb.point[Task]))
    } yield hfsUts

  private def hierarchicalFSIO(mnt: ADir, f: BackendEffectIO ~> Task): Task[BackendEffectIO ~> Task] =
    interpretHfsIO map { hfs =>
      val interpFS = f compose injectNT[BackendEffect, BackendEffectIO]

      val g: BackendEffect ~> Free[HfsIO, ?] =
        flatMapSNT(hierarchical.backendEffect[Task, HfsIO](Mounts.singleton(mnt, interpFS)))
          .compose(chroot.backendEffect[BackendEffect](mnt))

      NaturalTransformation.refl[Task] :+: (free.foldMapNT(hfs) compose g)
    }

  implicit val dataEncodeJson: EncodeJson[Data] =
    EncodeJson(DataCodec.Precise.encode(_).getOrElse(jString("Undefined")))
}
