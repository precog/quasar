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

package quasar.regression

import quasar.Predef._
import quasar._
import quasar.build.BuildInfo
import quasar.common._
import quasar.contrib.argonaut._
import quasar.ejson
import quasar.frontend._
import quasar.contrib.pathy._
import quasar.fp._, eitherT._, free._
import quasar.fp.ski._
import quasar.fs._
import quasar.main.FilesystemQueries
import quasar.fs.mount.{Mounts, hierarchical}
import quasar.sql, sql.{Query, Sql}

import java.io.{File => JFile, FileInputStream}
import java.math.{MathContext, RoundingMode}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.matching.Regex

import argonaut._, Argonaut._
import matryoshka._
import matryoshka.data.Fix
import org.specs2.execute._
import org.specs2.specification.core.Fragment
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

abstract class QueryRegressionTest[S[_]](
  fileSystems: Task[IList[SupportedFs[S]]])(
  implicit S0: QueryFile :<: S, S1: ManageFile :<: S,
           S2: WriteFile :<: S, S3: Task :<: S
) extends FileSystemTest[S](fileSystems) {

  import QueryRegressionTest._

  // NB: forcing the examples to not overlap seems to avoid some errors in Travis
  sequential

  type FsErr[A] = FileSystemErrT[F, A]

  val qfTransforms = QueryFile.Transforms[F]
  import qfTransforms._

  val injectTask: Task ~> F =
    liftFT[S].compose(injectNT[Task, S])

  val TestsRoot = currentDir[Sandboxed] </> dir("it") </> dir("src") </> dir("main") </> dir("resources") </> dir("tests")
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

  /** A name to identify the suite in test output. */
  def suiteName: String

  /** Return the results of evaluating the given query as a stream. */
  def queryResults(expr: Fix[Sql], vars: Variables, basePath: ADir): Process[CompExecM, Data]

  ////

  lazy val tests = regressionTests(TestsRoot, knownFileSystems).unsafePerformSync

  // NB: The printing is just to indicate progress (especially for travis-ci) as
  //     these tests have the potential to be slow for a backend.
  //
  //     Ideally, we'd have specs2 log each example in the suite as it finishes, but
  //     all attempts at doing this have been unsuccessful, if we succeed eventually
  //     this printing can be removed.
  fileSystemShould { fs =>
    suiteName should {
      step(print(s"Running $suiteName ["))

      tests.toList foreach { case (f, t) =>
        regressionExample(f, t, fs.ref.name, fs.setupInterpM, fs.testInterpM)
        step(print("."))
      }

      step(println("]"))
      step(runT(fs.setupInterpM)(manage.delete(DataDir)).runVoid)
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
    def runTest: Result = {
      val data = testQuery(DataDir </> fileParent(loc), test.query, test.variables)

      (ensureTestData(loc, test, setup) *> verifyResults(test.expected, data, run, backendName))
        .timed(5.minutes)
        .unsafePerformSync
    }

    s"${test.name} [${posixCodec.printPath(loc)}]" >> {
      test.backends.get(backendName) match {
        case Some(SkipDirective.Skip)    => skipped
        case Some(SkipDirective.SkipCI)  =>
          BuildInfo.isCIBuild.fold(Skipped("(skipped during CI build)"), runTest)
        case Some(SkipDirective.Pending) =>
          if (BuildInfo.coverageEnabled)
            Skipped("(pending example skipped during coverage run)")
          else if (BuildInfo.isCIBuild)
            Skipped("(pending example skipped during CI build)")
          else
            runTest.pendingUntilFixed
        case None                        => runTest
      }
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

  /** This is a workaround for issue where Mongo can only return objects. The
    * fallback evaluator will allow us to fix this in the right way, but for now
    * we just work around it in the tests.
    */
  val promoteValue: Json => Option[Json] =
    json => json.obj.fold(
      json.some)(
      _.toList match {
        case Nil                      => None
        case ("value", result) :: Nil => result.some
        case _                        => json.some
      })

  val TestContext = new MathContext(13, RoundingMode.DOWN)

  /** This helps us get identical results on different connectors, even though
    * they have different precisions for their floating point values.
    */
  val reducePrecision =
    λ[EndoK[ejson.Common]](ejson.dec.modify(_.round(TestContext))(_))

  val normalizeJson: Json => Option[Json] =
    j => promoteValue(Recursive[Json, ejson.Json].transCata(j)(liftFF(reducePrecision[Json])))

  /** Verify the given results according to the provided expectation. */
  def verifyResults(
    exp: ExpectedResult,
    act: Process[CompExecM, Data],
    run: Run,
    backendName: BackendName
  ): Task[Result] = {

    type H1[A] = PhaseResultT[Task, A]
    type H2[A] = SemanticErrsT[H1, A]
    type H3[A] = FileSystemErrT[H2, A]

    val h1: G ~> H1 = Hoist[PhaseResultT].hoist(run)
    val h2: H ~> H2 = Hoist[SemanticErrsT].hoist(h1)
    val h3: CompExecM ~> H3 = Hoist[FileSystemErrT].hoist(h2)

    val liftRun = λ[CompExecM ~> Task](fa =>
      rethrow[H1, NonEmptyList[SemanticError]].apply(rethrow[H2, FileSystemError].apply(h3(fa))).value
    )

    def deleteFields: Json => Json =
      _.withObject(obj => exp.ignoredFields.foldLeft(obj)(_ - _))

    exp.predicate(
      exp.rows.toVector,
      act.map(d => normalizeJson(d.asJson) ∘ deleteFields).unite.translate[Task](liftRun),
      exp.ignoreFieldOrderBackend match {
        case IgnoreFieldOrderAllBackends            =>
          FieldOrderIgnored
        case IgnoreFieldOrderBackends(backendNames) =>
          backendNames.exists(_ ≟ backendName).fold(FieldOrderIgnored, FieldOrderPreserved)
      })
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
      sql.fixParser.parse(Query(qry))
        .fold(e => Task.fail(new RuntimeException(e.message)),
        _.mkPathsAbsolute(loc).point[Task])

    f(parseTask).liftM[Process] flatMap (queryResults(_, Variables.fromMap(vars), loc))
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

    throwFsError(EitherT(load.take(1).runLast map (_ <\/ (())))) handleWith {
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

    val jf = jFile(TestsRoot </> file)
    Task.delay(jf.exists).liftM[Process].ifM(
      io.linesR(new FileInputStream(jf)) flatMap parse,
      Process.fail(new java.io.FileNotFoundException(jf.getPath)))
  }

  /** Returns all the `RegressionTest`s found in the given directory, keyed by
    * file path.
    */
  def regressionTests(
    testDir: RDir,
    knownBackends: Set[BackendName]
  ): Task[Map[RFile, RegressionTest]] =
    descendantsMatching(testDir, """^([^.].*)\.test""".r) // don't match file names with a leading .
      .map(f =>
        (loadRegressionTest(f) >>= verifyBackends(knownBackends)) strengthL
          (f relativeTo testDir).get)
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

  /** Returns the contents of the file as a `String`. */
  def textContents(file: RFile): Task[String] =
    Task.delay(Source.fromInputStream(new FileInputStream(jFile(file))).mkString)

  private def jFile(path: Path[_, _, Sandboxed]): JFile =
    new JFile(posixCodec.printPath(path))
}

object QueryRegressionTest {
  lazy val knownFileSystems = TestConfig.backendRefs.map(_.name).toSet

  val externalFS: Task[IList[SupportedFs[FileSystemIO]]] =
    for {
      uts    <- (Functor[Task] compose Functor[IList]).map(FileSystemTest.externalFsUT)(_.liftIO)
      mntDir =  rootDir </> dir("hfs-mnt")
      hfsUts <- uts.traverse(sb => sb.impl.map(ut => hierarchicalFSIO(mntDir, ut.testInterp) map { f: FileSystemIO ~> Task =>
                  SupportedFs(sb.ref, ut.copy(testInterp = f).contramapF(chroot.fileSystem[FileSystemIO](ut.testDir)).some)
                }).getOrElse(sb.point[Task]))
    } yield hfsUts

  private def hierarchicalFSIO(mnt: ADir, f: FileSystemIO ~> Task): Task[FileSystemIO ~> Task] =
    interpretHfsIO map { hfs =>
      val interpFS = f compose injectNT[FileSystem, FileSystemIO]

      val g: FileSystem ~> Free[HfsIO, ?] =
        flatMapSNT(hierarchical.fileSystem[Task, HfsIO](Mounts.singleton(mnt, interpFS)))
          .compose(chroot.fileSystem[FileSystem](mnt))

      NaturalTransformation.refl[Task] :+: (free.foldMapNT(hfs) compose g)
    }

  implicit val dataEncodeJson: EncodeJson[Data] =
    EncodeJson(DataCodec.Precise.encode(_).getOrElse(jString("Undefined")))
}
