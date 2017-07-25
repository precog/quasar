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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar._, DataArbitrary._
import quasar.common._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.foldable._
import quasar.contrib.scalaz.writerT._
import quasar.fp._
import quasar.fp.ski._
import quasar.frontend._
import quasar.fs._, FileSystemError._, FileSystemTest._
import quasar.main.FilesystemQueries
import quasar.physical.mongodb.Collection
import quasar.physical.mongodb.fs.MongoDbFileSystemSpec.mongoFsUT
import quasar.regression._
import quasar.sql, sql.Sql

import scala.Predef.$conforms

import com.mongodb.MongoException
import matryoshka.data.Fix
import monocle.Prism
import monocle.function.Field1
import monocle.std.{disjunction => D}
import org.specs2.execute.SkipException
import org.specs2.specification.core._
import pathy.Path._
import scalaz.{Optional => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

/** Unit tests for the MongoDB filesystem implementation. */
class MongoDbFileSystemSpec
  extends FileSystemTest[BackendEffectIO](mongoFsUT map (_ filter (_.ref supports BackendCapability.write())))
  with quasar.ExclusiveQuasarSpecification {

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  val query  = QueryFile.Ops[BackendEffectIO]
  val write  = WriteFile.Ops[BackendEffectIO]
  val manage = ManageFile.Ops[BackendEffectIO]
  val fsQ    = new FilesystemQueries[BackendEffectIO]

  type X[A] = Process[manage.M, A]

  /** The test prefix from the config.
    *
    * NB: This is a bit brittle as we're assuming this is the correct source
    *     of configuration compatible with the supplied interpreters.
    */
  val testPrefix: Task[ADir] =
    TestConfig.testDataPrefix

  /** This is necessary b/c the mongo server is shared global state and we
    * delete it all in this test, including the user-provided directory that
    * other tests expect to exist, which can cause failures depending on which
    * order the tests are run in =(
    *
    * The purpose of this function is to restore the testDir (i.e. database)
    * so that other tests aren't affected.
    */
  def restoreTestDir(run: Run): Task[Unit] = {
    val tmpFile: Task[AFile] =
      (testPrefix |@| NameGenerator.salt.map(file))(_ </> _)

    tmpFile flatMap { f =>
      val p = write.save(f, oneDoc.toProcess).terminated *>
              manage.delete(f).liftM[Process]

      rethrow[Task, FileSystemError].apply(execT(run, p))
    }
  }

  val tmpDir: Task[ADir] =
    NameGenerator.salt map (s => rootDir </> dir(s))

  fileSystemShould { (fs, _) =>
    val run = fs.testInterpM

    "MongoDB" should {
      "Writing" >> {
        val invalidData = testPrefix.map(_ </> dir("invaliddata"))
                            .liftM[FileSystemErrT]

        "fail with `InvalidData` when attempting to save non-documents" >> prop {
          (data: Data, fname: Int) => isNotObj(data) ==> {
            val path = invalidData map (_ </> file(fname.toHexString))

            path.flatMap(p => runLogT(run, write.append(p, Process(data)))).map { errs =>
              vectorFirst[FileSystemError]
                .composePrism(writeFailed)
                .composeLens(Field1.first)
                .nonEmpty(errs.toVector)
            }.run.unsafePerformSync.toEither must beRight(true)
          }
        }

        "fail to save data to DB path" in {
          val path = rootDir </> file("foo")

          // NB: We receive the expected shape of error, but it is actually for
          //     the temp path that is written to first, not the destination path.
          //
          //     This sort of thing shouldn't happen once we have primitive
          //     support for save, see SD-1296.
          runLogT(run, write.save(path, Process(Data.Obj(ListMap("a" -> Data.Int(1)))))).run.unsafePerformSync must beLike {
            case -\/(FileSystemError.PathErr(PathError.InvalidPath(_, msg))) => msg must_== "path names a database, but no collection"
          }
        }

        "fail to append data to DB path" in {
          val path = rootDir </> file("foo")

          runLogT(run, write.append(path, Process(Data.Obj(ListMap("a" -> Data.Int(1)))))).run.unsafePerformSync must_==
            -\/(FileSystemError.pathErr(PathError.invalidPath(path, "path names a database, but no collection")))
        }

        step(invalidData.flatMap(p => runT(run)(manage.delete(p))).runVoid)
      }

      /** NB: These tests effectively require "root" level permissions on the
        *     MongoDB server, but so does the functionality they exercise, so
        *     we're ok skipping them on an authorization error.
        */
      "Deletion" >> {
        "top-level directory should delete database" >> {
          def check(d: ADir)(implicit X: Apply[X]) = {
            val f = d </> file("deldb")

            (
              query.ls(rootDir).liftM[Process]           |@|
              write.save(f, oneDoc.toProcess).terminated |@|
              query.ls(rootDir).liftM[Process]           |@|
              manage.delete(d).liftM[Process]            |@|
              query.ls(rootDir).liftM[Process]
            ) { (before, _, create, _, delete) =>
              val pn = d.relativeTo(rootDir).flatMap(firstSegmentName).toSet
              (before.intersect(pn) must beEmpty) and
              (create.intersect(pn) must_== pn) and
              (delete must_== before)
            }
          }

          tmpDir.flatMap(d =>
            rethrow[Task, FileSystemError]
              .apply(runLogT(run, check(d)))
              .handleWith(skipIfUnauthorized)
              .map(_.headOption getOrElse ko)
          ).unsafePerformSync
        }

        "root dir should delete all databases" >> {
          def check(d1: ADir, d2: ADir)
                   (implicit X: Apply[X]) = {

            val f1 = d1 </> file("delall1")
            val f2 = d2 </> file("delall2")

            (
              write.save(f1, oneDoc.toProcess).terminated |@|
              write.save(f2, oneDoc.toProcess).terminated |@|
              query.ls(rootDir).liftM[Process]            |@|
              manage.delete(rootDir).liftM[Process]       |@|
              query.ls(rootDir).liftM[Process]
            ) { (_, _, before, _, after) =>
              val dA = d1.relativeTo(rootDir).flatMap(firstSegmentName).toSet
              val dB = d2.relativeTo(rootDir).flatMap(firstSegmentName).toSet

              (before.intersect(dA) must_== dA) and
              (before.intersect(dB) must_== dB) and
              (after must beEmpty)
            }
          }

          (tmpDir |@| tmpDir)((d1, d2) =>
            rethrow[Task, FileSystemError]
              .apply(runLogT(run, check(d1, d2)))
              .handleWith(skipIfUnauthorized)
              .map(_.headOption getOrElse ko)
          ).join.unsafePerformSync
        }.skippedOnUserEnv("Would destroy user data.")

        step(restoreTestDir(run).unsafePerformSync)
      }

      /** TODO: Testing this here closes the tests to the existence of
        *       `WorkflowExecutor`, but opens them to brittleness by assuming
        *       what is compiled to MR vs Aggregation. i.e. just because a
        *       query is compiled to MR now, doesn't mean it always will be.
        *
        *       Also, is this check something we should handle in `FileSystem`
        *       combinators? i.e. look through the LP provided to `ExecutePlan`
        *       and check that all the referenced files exist?
        *
        *       We also may want to change FileSystemUT[S[_]] to
        *       FileSystemUT[S[_], F[_]] to allow test suites to specify more
        *       granular constraints than just `Task`.
        */
      "Querying" >> {
        def shouldFailWithPathNotFound(f: String => String) = {
          val dne = testPrefix map (_ </> file("__DNE__"))
          val q = dne map (p => f(posixCodec.printPath(p)))
          val xform = QueryFile.Transforms[query.FreeS]

          import xform._

          val runExec: CompExecM ~> FileSystemErrT[PhaseResultT[Task, ?], ?] = {
            type X0[A] = PhaseResultT[Task, A]

            val x0: G ~> X0 =
              Hoist[PhaseResultT].hoist(run)

            val x1: H ~> X0 =
              rethrow[X0, SemanticErrors].compose[H](Hoist[SemanticErrsT].hoist(x0))

            Hoist[FileSystemErrT].hoist(x1)
          }

          def check(file: AFile) = {
            val errP: Prism[FileSystemError \/ AFile, APath] =
              D.left                    composePrism
              FileSystemError.pathErr composePrism
              PathError.pathNotFound

            val out = renameFile(file, κ(FileName("out")))

            def check0(expr: Fix[Sql]) =
              (run(query.fileExists(file)).unsafePerformSync ==== false) and
              (errP.getOption(
                runExec(fsQ.executeQuery(expr, Variables.empty, rootDir, out))
                  .run.value.unsafePerformSync
              ) must beSome(file))

            sql.fixParser.parseExpr(sql.Query(f(posixCodec.printPath(file)))) fold (
              err => ko(s"Parsing failed: ${err.shows}"),
              check0)
          }

          dne.map(check).unsafePerformSync
        }

        "mapReduce query should fail when file DNE" >> {
          shouldFailWithPathNotFound { path =>
            s"""SELECT name FROM `$path` WHERE LENGTH(name) > 10"""
          }
        }

        "aggregation query should fail when file DNE" >> {
          shouldFailWithPathNotFound { path =>
            s"""SELECT name FROM `$path` WHERE name.field1 > 10"""
          }
        }
      }

      "List dirs" >> {
        "listing the root dir should succeed" >> {
          runT(run)(query.ls(rootDir)).runEither must beRight
        }

        "listing a non-empty top dir (i.e. a database) should succeed" >> {
          val tdir = rootDir </> dir("__topdir__")
          val tfile = tdir </> file("foobar")

          val p = write.save(tfile, oneDoc.toProcess).drain ++
                  query.ls(tdir).liftM[Process]
                    .flatMap(ns => Process.emitAll(ns.toVector))

          (runLogT(run, p) <* runT(run)(manage.delete(tdir)))
            .runEither must beRight(contain(FileName("foobar").right[DirName]))
        }
      }

      "File exists" >> {
        "for missing file at root (i.e. a database path) should succeed" >> {
          val tfile = rootDir </> file("foo")

          val p = query.fileExists(tfile)

          run(p).unsafePerformSync must_== false
        }

        "for missing file not at the root (i.e. a collection path) should succeed" >> {
          val tfile = rootDir </> dir("foo") </> file("bar")

          val p = query.fileExists(tfile)

          run(p).unsafePerformSync must_== false
        }
      }

      "Moving" >> {
        "top-level directory should move database" >> {
          def check(src: ADir, dst: ADir)(implicit X: Apply[X]) = {
            val f1 = src </> file("movdb1")
            val f2 = src </> file("movdb2")
            val ovr = MoveSemantics.Overwrite

            (
              write.save(f1, oneDoc.toProcess).terminated |@|
              write.save(f2, oneDoc.toProcess).terminated |@|
              query.ls(src).liftM[Process]                |@|
              manage.moveDir(src, dst, ovr).liftM[Process] |@|
              query.ls(dst).liftM[Process]
            ) { (_, _, create, _, moved) =>
              val pn: Set[PathSegment] = Set(FileName("movdb1").right, FileName("movdb2").right)
              (create must contain(allOf(pn))) and (moved must contain(allOf(pn)))
            }
          }

          (tmpDir |@| tmpDir)((s, d) =>
            rethrow[Task, FileSystemError]
              .apply(
                runLogT(run, check(s, d)) <*
                runT(run)(manage.delete(s) *> manage.delete(d)))
              .handleWith(skipIfUnauthorized)
              .map(_.headOption getOrElse ko)
          ).join.unsafePerformSync
        }
      }

      "Temp files" >> {
        Fragments.foreach(Collection.DatabaseNameEscapes) { case (esc, _) => Fragments(
          s"be in the same database when db name contains '$esc'" >> {
            val pdir = rootDir </> dir(s"db${esc}name")

            runT(run)(for {
              tfile  <- manage.tempFile(pdir)
              dbName <- EitherT.fromDisjunction[manage.FreeS](
                          Collection.dbNameFromPath(tfile).leftMap(pathErr(_)))
            } yield dbName).runEither must_== Collection.dbNameFromPath(pdir).toEither
          })
        }
        ok
      }
    }
  }

  ////

  private def skipIfUnauthorized[A]: PartialFunction[Throwable, Task[A]] = {
    case ex: MongoException if ex.getMessage.contains("Command failed with error 13: 'not authorized on ") =>
      Task.fail(SkipException(skipped("No db-level permissions.")))
  }

  private def isNotObj: Data => Boolean = {
    case Data.Obj(_) => false
    case _           => true
  }
}

object MongoDbFileSystemSpec {
  // NB: No `chroot` here as we want to test deleting top-level
  //     dirs (i.e. databases).
  val mongoFsUT: Task[IList[SupportedFs[BackendEffectIO]]] =
    (Functor[Task] compose Functor[IList])
      .map(
        TestConfig.externalFileSystems(
          FileSystemTest.fsTestConfig(FsType, definition)
        ).handleWith[IList[SupportedFs[BackendEffect]]] {
          case _: TestConfig.UnsupportedFileSystemConfig => Task.now(IList.empty)
        }
      )(_.liftIO)
}

object MongoDbQScriptFileSystemSpec {
  // NB: No `chroot` here as we want to test deleting top-level
  //     dirs (i.e. databases).
  val mongoFsUT: Task[IList[SupportedFs[BackendEffectIO]]] =
    (Functor[Task] compose Functor[IList])
      .map(
        TestConfig.externalFileSystems(
          FileSystemTest.fsTestConfig(QScriptFsType, qscriptDefinition)
        ).handleWith[IList[SupportedFs[BackendEffect]]] {
          case _: TestConfig.UnsupportedFileSystemConfig => Task.now(IList.empty)
        }
      )(_.liftIO)
}
