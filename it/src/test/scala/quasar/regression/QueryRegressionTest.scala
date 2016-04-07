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

import quasar._
import quasar.Predef._
import quasar.fp._
import quasar.fs._
import quasar.fs.mount.{MountConfig, Mounts, hierarchical}
import quasar.physical.mongodb.fs.MongoDBFsType
import quasar.sql, sql.{Expr, Query}

import java.io.{File => JFile, FileInputStream}
import scala.io.Source
import scala.util.matching.Regex

import argonaut._, Argonaut._
import org.specs2.specification._
import org.specs2.execute._
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{merge => pmerge, _}

abstract class QueryRegressionTest[S[_]: Functor](
  fileSystems: Task[IList[FileSystemUT[S]]])(
  implicit S0: QueryFileF :<: S, S1: ManageFileF :<: S,
           S2: WriteFileF :<: S, S3: Task :<: S
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

  /** A name to identify the suite in test output. */
  def suiteName: String

  /** Return the results of evaluating the given query as a stream. */
  def queryResults(expr: Expr, vars: Variables): Process[CompExecM, Data]

  ////

  lazy val tests = regressionTests(TestsRoot, knownFileSystems).run

  fileSystemShould { fs =>
    suiteName should {
      step(prepareTestData(tests, fs.setupInterpM).run)

      tests.toList foreach { case (f, t) =>
        regressionExample(f, t, fs.name, fs.testInterpM)
      }

      step(runT(fs.setupInterpM)(manage.delete(DataDir)).runVoid)
    }; ()
  }

  ////

  /** Returns an `Example` verifying the given `RegressionTest`. */
  def regressionExample(
    loc: RFile,
    test: RegressionTest,
    backendName: BackendName,
    run: Run
  ): Example = {
    def runTest = (for {
      _    <- Task.delay(println(test.query))
      _    <- run(test.data.traverse[F,Result](
                relFile => injectTask(resolveData(loc, relFile).fold(
                          err => Task.fail(new RuntimeException(err)),
                          Task.now)) >>=
                        (p => verifyDataExists(dataFile(p)))))
      data =  testQuery(DataDir </> fileParent(loc), test.query, test.variables)
      res  <- verifyResults(test.expected, data, run)
    } yield res).run

    s"${test.name} [${posixCodec.printPath(loc)}]" >> {
      test.backends.get(backendName) match {
        case Some(SkipDirective.Skip)    => skipped
        case Some(SkipDirective.Pending) => runTest.pendingUntilFixed
        case None                        => runTest
      }
    }
  }

  /** Verify that the given data file exists in the filesystem. */
  def verifyDataExists(file: AFile): F[Result] =
    query.fileExists(file).map(exists =>
      if (exists) success(s"data file exists: ${fileName(file).value}")
      else failure(s"data file does not exist: ${fileName(file).value}"))

  /** Verify the given results according to the provided expectation. */
  def verifyResults(
    exp: ExpectedResult,
    act: Process[CompExecM, Data],
    run: Run
  ): Task[Result] = {
    val liftRun: CompExecM ~> Task = {
      type H1[A] = PhaseResultT[Task, A]
      type H2[A] = SemanticErrsT[H1, A]
      type H3[A] = FileSystemErrT[H2, A]

      val h1: G ~> H1 = Hoist[PhaseResultT].hoist(run)
      val h2: H ~> H2 = Hoist[SemanticErrsT].hoist(h1)
      val h3: CompExecM ~> H3 = Hoist[FileSystemErrT].hoist(h2)

      new (CompExecM ~> Task) {
        def apply[A](fa: CompExecM[A]) =
          rethrow[H1, NonEmptyList[SemanticError]].apply(
            rethrow[H2, FileSystemError].apply(
              h3(fa))).value
      }
    }

    def deleteFields: Json => Json =
      _.withObject(obj => exp.ignoredFields.foldLeft(obj)(_ - _))

    exp.predicate(
      exp.rows.toVector,
      act.map(deleteFields.compose[Data](_.asJson)).translate[Task](liftRun))
  }

  /** Parse and execute the given query, returning a stream of results. */
  def testQuery(
    loc: ADir,
    qry: String,
    vars: Map[String, String]
  ): Process[CompExecM, Data] = {
    val f: Task ~> CompExecM =
      toCompExec compose injectTask

    val parseTask: Task[Expr] =
      sql.parseInContext(Query(qry), loc)
        .fold(e => Task.fail(new RuntimeException(e.message)), _.point[Task])

    f(parseTask).liftM[Process] flatMap (queryResults(_, Variables.fromMap(vars)))
  }

  /** Loads all the test data needed by the given tests into the filesystem. */
  def prepareTestData(tests: Map[RFile, RegressionTest], run: Run): Task[Unit] = {
    val throwFsError = rethrow[Task, FileSystemError]

    val dataLoc: ((RFile, RegressionTest)) => Set[RFile] = {
      case (f, t) =>
        t.data.flatMap(resolveData(f, _).toOption).toSet
    }

    val loads: Process[Task, Process[Task, FileSystemError]] =
      Process.emitAll(tests.toList.foldMap(dataLoc).toVector) map { file =>
        write.createChunked(
          dataFile(file),
          testData(file).chunk(500).translate(injectTask)
        ).translate[Task](throwFsError.compose[FsErr](runT(run)))
      }

    throwFsError(EitherT(pmerge.mergeN(loads).take(1).runLast.map(_ <\/ (()))))
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
    descendantsMatching(testDir, """.*\.test"""r)
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
    def errMsg = s"Unrecognized backend(s): ${unknown.mkString(", ")}"

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
  import quasar.physical.mongodb.{filesystems => mongofs}

  lazy val knownFileSystems = TestConfig.backendNames.toSet

  val externalFS: Task[IList[FileSystemUT[FileSystemIO]]] = {
    val extFs = TestConfig.externalFileSystems {
      case (MountConfig.FileSystemConfig(MongoDBFsType, uri), dir) =>
        lazy val f = mongofs.testFileSystemIO(uri, dir).run
        Task.delay(f)
    }

    for {
      uts    <- extFs
      mntDir =  rootDir </> dir("hfs-mnt")
      hfsUts <- uts.traverse(ut => hierarchicalFSIO(mntDir, ut.testInterp) map { f =>
                  ut.copy(testInterp = f).contramap(chroot.fileSystem[FileSystemIO](ut.testDir))
                })
    } yield hfsUts
  }

  private def hierarchicalFSIO(mnt: ADir, f: FileSystemIO ~> Task): Task[FileSystemIO ~> Task] =
    interpretHfsIO map { hfs =>
      val interpFS = f compose injectNT[FileSystem, FileSystemIO]

      val g: FileSystem ~> Free[HfsIO, ?] =
        hierarchical.fileSystem[Task, HfsIO](Mounts.singleton(mnt, interpFS))
          .compose(chroot.fileSystem[FileSystem](mnt))

      free.interpret2(
        NaturalTransformation.refl[Task],
        free.foldMapNT(hfs) compose g)
    }

  implicit val dataEncodeJson: EncodeJson[Data] =
    EncodeJson(d =>
      DataCodec.Precise
        .encode(d)
        .fold(err => scala.sys.error(err.message), ι))
}
