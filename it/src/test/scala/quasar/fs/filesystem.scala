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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.fp._
import quasar.specs2.SkippedOnUserEnv
import quasar._
import quasar.fs._, Path._

import org.specs2.execute.{Result, SkipException}
import org.specs2.scalaz.DisjunctionMatchers
import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

class FileSystemSpecs extends BackendTest with DisjunctionMatchers with SkippedOnUserEnv {

  import Backend._
  import Errors._
  import quasar.fs._

  val oneDoc: Process[Task, Data] =
    Process.emit(Data.Obj(ListMap("a" -> Data.Int(1))))
  val anotherDoc: Process[Task, Data] =
    Process.emit(Data.Obj(ListMap("b" -> Data.Int(2))))
  def manyDocs(n: Int): Process[Task, Data] =
    Process.range(0, n).map(n => Data.Obj(ListMap("a" -> Data.Int(n))))

  def skipUnlessAuthorized(err: ProcessingError) = err match {
    case PWriteError(WriteError(_, Some(reason)))
        if reason.startsWith("Command failed with error 13: 'not authorized on ") =>
      throw SkipException(skipped("No db-level permissions."))
    case x => x must beNull
  }

  backendShould(interactive.zips.run) { (prefix, insertFs, fs, name, files) =>
    val relPrefix = prefix.asRelative
    val TestDir = relPrefix ++ testRootDir ++ genTempDir.run
    val ZipsDir = files.head

    implicit class SkippedOnReadOnly[T: org.specs2.execute.AsResult](t: => T) {
      def skippedOnReadOnly: org.specs2.execute.Result =
        if (name == TestConfig.MONGO_READ_ONLY) org.specs2.execute.Skipped("test of write behavior")
        else org.specs2.execute.AsResult(t)
    }

    "FileSystem" should {
      // Run the task to create a single FileSystem instance for each run (I guess)

      "list root" in {
        fs.ls(Path(".")).map(_ must contain(FilesystemNode(relPrefix, None))).run.run must beRightDisjunction
      }

      "count" in {
        fs.count(ZipsDir).run.run must beRightDisjunction(29353L)
      }

      "read zips with skip and limit" in {
        (for {
          cursor  <- fs.scan(ZipsDir, 100, Some(5)).runLog
          process <- fs.scan(ZipsDir, 0, None).drop(100).take(5).runLog
        } yield {
          cursor must_== process
        }).fold(_ must beNull, ι).run
      }

      "fail when reading zips with negative skip and zero limit" in {
        fs.scan(ZipsDir, -1, None).run.fold(_ must beNull, ι).attemptRun must beLeftDisjunction
        fs.scan(ZipsDir, 0, Some(0)).run.fold(_ must beNull, ι).attemptRun must beLeftDisjunction
      }

      "save one" in {
        (for {
          tmp    <- liftE[ProcessingError](genTempFile)
          before <- fs.ls(TestDir).leftMap(PPathError(_))
          _      <- fs.save(TestDir ++ tmp, oneDoc)
          after  <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          before must not(contain(FilesystemNode(tmp, None)))
          after must contain(FilesystemNode(tmp, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "allow duplicate saves" in {
        (for {
          tmp    <- liftE[ProcessingError](genTempFile)
          _      <- fs.save(TestDir ++ tmp, oneDoc)
          before <- fs.ls(TestDir).leftMap(PPathError(_))
          _      <- fs.save(TestDir ++ tmp, oneDoc)
          after  <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          before must contain(FilesystemNode(tmp, None))
          after must contain(FilesystemNode(tmp, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "fail duplicate creates" in {
        (for {
          tmp    <- liftE[ProcessingError](genTempFile)
          _      <- fs.create(TestDir ++ tmp, oneDoc)
          before <- fs.ls(TestDir).leftMap(PPathError(_))
          rez    <- liftE[ProcessingError](fs.create(TestDir ++ tmp, anotherDoc).run)
          after  <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must_== before
          rez must beLeftDisjunction(PPathError(ExistingPathError(TestDir ++ tmp, Some("can’t be created, because it already exists"))))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "fail initial replace" in {
        (for {
          tmp    <- liftP(genTempFile)
          before <- fs.ls(TestDir)
          rez    <- liftP(fs.replace(TestDir ++ tmp, anotherDoc).run)
          after  <- fs.ls(TestDir)
        } yield {
          after must_== before
          rez must beLeftDisjunction(PPathError(NonexistentPathError(TestDir ++ tmp, Some("can’t be replaced, because it doesn’t exist"))))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "replace one" in {
        (for {
          tmp    <- liftE[ProcessingError](genTempFile)
          _      <- fs.create(TestDir ++ tmp, oneDoc)
          before <- fs.ls(TestDir).leftMap(PPathError(_))
          _      <- fs.replace(TestDir ++ tmp, anotherDoc)
          after  <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          before must contain(FilesystemNode(tmp, None))
          after must contain(FilesystemNode(tmp, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "save one (subdir)" in {
        (for {
          tmpDir <- liftE[ProcessingError](genTempDir)
          tmp = Path("file1")
          before <- fs.ls(TestDir ++ tmpDir).leftMap(PPathError(_))
          _      <- fs.save(TestDir ++ tmpDir ++ tmp, oneDoc)
          after  <- fs.ls(TestDir ++ tmpDir).leftMap(PPathError(_))
        } yield {
          before must not(contain(FilesystemNode(tmp, None)))
          after must contain(FilesystemNode(tmp, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "save one with error" in {
        val badJson = Data.Int(1)
        val data: Process[Task, Data] = Process.emit(badJson)
        (for {
          tmpDir <- liftP(genTempDir)
          file = tmpDir ++ Path("file1")
          before <- fs.ls(TestDir ++ tmpDir)
          rez    <- liftP(fs.save(TestDir ++ file, data).run)
          after  <- fs.ls(TestDir ++ tmpDir)
        } yield {
          rez must beLeftDisjunction
          after must_== before
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "save many (approx. 10 MB in 1K docs)" in {
        val sizeInMB = 10.0

        // About 0.5K each of data, and 0.25K of index, etc.:
        def jsonTree(depth: Int): Data =
          if (depth == 0) Data.Arr(Data.Str("abc") :: Data.Int(123) :: Data.Str("do, re, mi") :: Nil)
          else            Data.Obj(ListMap("left" -> jsonTree(depth-1), "right" -> jsonTree(depth-1)))
        def json(i: Int) = Data.Obj(ListMap("seq" -> Data.Int(i), "filler" -> jsonTree(3)))

        // This is _very_ approximate:
        val bytesPerDoc = 750
        val count = (sizeInMB*1024*1024/bytesPerDoc).toInt

        val data: Process[Task, Data] = Process.emitAll(0 until count).map(json(_))

        (for {
          tmp   <- liftE[ProcessingError](genTempFile)
          _     <- fs.save(TestDir ++ tmp, data)
          after <- fs.ls(TestDir).leftMap(PPathError(_))
          _     <- fs.delete(TestDir ++ tmp).leftMap(PPathError(_)) // clean up this one eagerly, since it's a large file
        } yield {
          after must contain(FilesystemNode(tmp, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "append one" in {
        val json = Data.Obj(ListMap("a" ->Data.Int(1)))
        val data: Process[Task, Data] = Process.emit(json)
        (for {
          tmp   <- liftE[ProcessingError](genTempFile)
          rez   <- fs.append(TestDir ++ tmp, data).runLog.leftMap(PPathError(_))
          saved <- fs.scan(TestDir ++ tmp, 0, None).runLog.leftMap(PResultError(_))
        } yield {
          rez.size must_== 0
          saved.size must_== 1
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "append with one ok and one error" in {
        val json1 = Data.Obj(ListMap("a" ->Data.Int(1)))
        val json2 = Data.Int(1)
        val data: Process[Task, Data] = Process.emitAll(json1 :: json2 :: Nil)
        (for {
          tmp   <- liftE[ProcessingError](genTempFile)
          rez   <- fs.append(TestDir ++ tmp, data).runLog.leftMap(PPathError(_))
          saved <- fs.scan(TestDir ++ tmp, 0, None).runLog.leftMap(PResultError(_))
        } yield {
          rez.size must_== 1
          saved.size must_== 1
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "move file" in {
        (for {
          tmp1  <- liftE[ProcessingError](genTempFile)
          tmp2  <- liftE(genTempFile)
          _     <- fs.save(TestDir ++ tmp1, oneDoc)
          _     <- fs.move(TestDir ++ tmp1, TestDir ++ tmp2, FailIfExists).leftMap(PPathError(_))
          after <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must not(contain(FilesystemNode(tmp1, None)))
          after must contain(FilesystemNode(tmp2, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "error: move file to existing path" in {
        (for {
          tmp1  <- liftE[ProcessingError](genTempFile)
          tmp2  <- liftE(genTempFile)
          _     <- fs.save(TestDir ++ tmp1, oneDoc)
          _     <- fs.save(TestDir ++ tmp2, oneDoc)
          rez   <- liftE(fs.move(TestDir ++ tmp1, TestDir ++ tmp2, FailIfExists).leftMap(PPathError(_)).run.attempt)
          after <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          rez must beLeftDisjunction
          after must contain(FilesystemNode(tmp1, None))
          after must contain(FilesystemNode(tmp2, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "move file to existing path with Overwrite semantics" in {
        (for {
          tmp1  <- liftE[ProcessingError](genTempFile)
          tmp2  <- liftE(genTempFile)
          _     <- fs.save(TestDir ++ tmp1, oneDoc)
          _     <- fs.save(TestDir ++ tmp2, oneDoc)
          _     <- fs.move(TestDir ++ tmp1, TestDir ++ tmp2, Overwrite).leftMap(PPathError(_))
          after <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must not(contain(FilesystemNode(tmp1, None)))
          after must contain(FilesystemNode(tmp2, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "move file to itself (NOP)" in {
        (for {
          tmp1  <- liftE[ProcessingError](genTempFile)
          _     <- fs.save(TestDir ++ tmp1, oneDoc)
          _     <- fs.move(TestDir ++ tmp1, TestDir ++ tmp1, FailIfExists).leftMap(PPathError(_))
          after <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must contain(FilesystemNode(tmp1, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "move dir" in {
        (for {
          tmpDir1 <- liftE[ProcessingError](genTempDir)
          tmp1 = tmpDir1 ++ Path("file1")
          tmp2 = tmpDir1 ++ Path("file2")
          _       <- fs.save(TestDir ++ tmp1, oneDoc)
          _       <- fs.save(TestDir ++ tmp2, oneDoc)
          tmpDir2 <- liftE(genTempDir)
          _       <- fs.move(TestDir ++ tmpDir1, TestDir ++ tmpDir2, FailIfExists).leftMap(PPathError(_))
          after   <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must not(contain(FilesystemNode(tmpDir1, None)))
          after must contain(FilesystemNode(tmpDir2, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "move dir with destination given as file path" in {
        (for {
          tmpDir1 <- liftE[ProcessingError](genTempDir)
          tmp1 = tmpDir1 ++ Path("file1")
          tmp2 = tmpDir1 ++ Path("file2")
          _       <- fs.save(TestDir ++ tmp1, oneDoc)
          _       <- fs.save(TestDir ++ tmp2, oneDoc)
          tmpDir2 <- liftE(genTempFile)
          _       <- fs.move(TestDir ++ tmpDir1, TestDir ++ tmpDir2, FailIfExists).leftMap(PPathError(_))
          after   <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must not(contain(FilesystemNode(tmpDir1, None)))
          after must contain(FilesystemNode(tmpDir2.asDir, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "move missing dir to new (also missing) location (NOP)" in {
        (for {
          tmpDir1 <- liftE[ProcessingError](genTempDir)
          tmpDir2 <- liftE(genTempDir)
          _       <- fs.move(TestDir ++ tmpDir1, TestDir ++ tmpDir2, FailIfExists).leftMap(PPathError(_))
          after   <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must not(contain(FilesystemNode(tmpDir1, None)))
          after must not(contain(FilesystemNode(tmpDir2, None)))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "delete file" in {
        (for {
          tmp   <- liftE[ProcessingError](genTempFile)
          _     <- fs.save(TestDir ++ tmp, oneDoc)
          _     <- fs.delete(TestDir ++ tmp).leftMap(PPathError(_))
          after <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must not(contain(FilesystemNode(tmp, None)))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "delete file but not sibling" in {
        val tmp1 = Path("file1")
        val tmp2 = Path("file2")
        (for {
          tmpDir <- liftE[ProcessingError](genTempDir)
          _      <- fs.save(TestDir ++ tmpDir ++ tmp1, oneDoc)
          _      <- fs.save(TestDir ++ tmpDir ++ tmp2, oneDoc)
          before <- fs.ls(TestDir ++ tmpDir).leftMap(PPathError(_))
          _      <- fs.delete(TestDir ++ tmpDir ++ tmp1).leftMap(PPathError(_))
          after  <- fs.ls(TestDir ++ tmpDir).leftMap(PPathError(_))
        } yield {
          before must contain(FilesystemNode(tmp1, None))
          after must not(contain(FilesystemNode(tmp1, None)))
          after must contain(FilesystemNode(tmp2, None))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "delete dir" in {
        (for {
          tmpDir <- liftE[ProcessingError](genTempDir)
          tmp1 = tmpDir ++ Path("file1")
          tmp2 = tmpDir ++ Path("file2")
          _      <- fs.save(TestDir ++ tmp1, oneDoc)
          _      <- fs.save(TestDir ++ tmp2, oneDoc)
          _      <- fs.delete(TestDir ++ tmpDir).leftMap(PPathError(_))
          after  <- fs.ls(TestDir).leftMap(PPathError(_))
        } yield {
          after must not(contain(FilesystemNode(tmpDir, None)))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "delete database" in {
        // NB: Because this operates on a DB, it can’t run within `TestDir`.
        //     Failure during `delete` requires manual clean-up.
        (for {
          before <- fs.ls(Path.Root).leftMap(PPathError(_))
          db     <- liftE(genTempDir)
          col    <- liftE(genTempFile)
          _      <- fs.save(db ++ col, oneDoc)
          create <- fs.ls(Path.Root).leftMap(PPathError(_))
          _      <- fs.delete(db).leftMap(PPathError(_))
          delete <- fs.ls(Path.Root).leftMap(PPathError(_))
        } yield {
          before must not contain(FilesystemNode(db, None))
          create must contain(FilesystemNode(db, None))
          delete must_== before
        }).fold(skipUnlessAuthorized, ι).run
      }.skippedOnReadOnly

      "delete all databases" in {
        // NB: Because this operates on a DB, it can’t run within `TestDir`.
        //     Failure during `delete` requires manual clean-up.
        (for {
          db1    <- liftE(genTempDir)
          db2    <- liftE(genTempDir)
          col    <- liftE(genTempFile)
          _      <- fs.save(db1 ++ col, oneDoc)
          _      <- fs.save(db2 ++ col, oneDoc)
          before <- fs.ls(Path.Root).leftMap(PPathError(_))
          _      <- fs.delete(Path.Root).leftMap(PPathError(_))
          after  <- fs.ls(Path.Root).leftMap(PPathError(_))
        } yield {
          before must contain(FilesystemNode(db1, None))
          before must contain(FilesystemNode(db2, None))
          after must_== Set()
        }).fold(skipUnlessAuthorized, ι).run
      }.skippedOnUserEnv("This could destroy user data.").skippedOnReadOnly

      "delete missing file (not an error)" in {
        (for {
          tmp <- genTempFile
          rez <- fs.delete(TestDir ++ tmp).run.attempt
        } yield {
          rez must beRightDisjunction
        }).run
      }.skippedOnReadOnly

      "read large data and count" in {
        val COUNT = 10000
        val data = Process.range(0, COUNT).map(n => Data.Obj(ListMap("a" -> Data.Int(n))))

        (for {
          tmp <- genTempFile
          _   <- insertFs.save(TestDir ++ tmp, data).run

          ds  <- fs.scan(TestDir ++ tmp, 0, None).map(_ => 1).sum.runLast.run
        } yield {
          ds must beRightDisjunction(Some(COUNT))
        }).run
      }

      "read large data and run effects twice" in {
        val COUNT = 10000
        val data = Process.range(0, COUNT).map(n => Data.Obj(ListMap("a" -> Data.Int(n))))

        (for {
          tmp <- genTempFile
          _   <- insertFs.save(TestDir ++ tmp, data).run

          t   = fs.scan(TestDir ++ tmp, 0, None).map(_ => 1).sum.runLast
          ds1  <- t.run
          ds2  <- t.run
        } yield {
          ds1 must beRightDisjunction(Some(COUNT))
          ds2 must beRightDisjunction(Some(COUNT))
        }).run
      }

      "read very large data, encode, and zip" in {
        // NB: test as many layers as we can without the API, including
        // reading from the backend, encoding the results, and gzipping the
        // resulting bytes. The main point here is to prove that all of this
        // is stack safe.

        import argonaut._
        import scodec.bits._

        val COUNT = 100*1000
        val codec = DataCodec.Precise
        val EE: EncodeJson[DataEncodingError] = implicitly

        // emulating what happens in FileSystemApi/http4s:
        def encode(d: Data): ByteVector = {
          val s = DataCodec.render(d)(codec).fold(EE.encode(_).toString, ι)
          ByteVector.view(s.getBytes)
        }

        // Lifted from http4s's GZip code:
        val level = java.util.zip.Deflater.DEFAULT_COMPRESSION
        val bufferSize = 32 * 1024
        def zipped[F[_]](p: Process[F, ByteVector]) =
          p.pipe(scalaz.stream.compress.deflate(
            level = level,
            nowrap = true,
            bufferSize = bufferSize))

        (for {
          tmp <- genTempFile
          _   <- insertFs.save(TestDir ++ tmp, manyDocs(COUNT)).run

          ds = fs.scan(TestDir ++ tmp, 0, None).map(encode)
          ds1 = ds.translate[ProcessingTask](convertError[Task](PResultError(_)))
          ds  <- zipped(ds1).map(v => v.length).sum.runLast.run
        } yield {
          ds.toOption.join must beSome.which(_ must beBetween(200*1000, 250*1000))
        }).run
      }
    }

    "query evaluation" should {
      import quasar.sql.{Expr, Query, SQLParser}

      def parse(query: String) =
        liftE[ProcessingError](SQLParser.parseInContext(Query(query), TestDir).fold(
          e => Task.fail(new RuntimeException(e.message)),
          Task.now))
      def eval(fs: Backend, query: Expr): ProcessingTask[IndexedSeq[Data]] =
        fs.eval(QueryRequest(query, Variables(Map()))).run._2.fold(
          e => liftE(Task.fail(new RuntimeException(e.message))),
          _.runLog)
      def run(fs: Backend, query: Expr, out: Path): ProcessingTask[ResultPath] =
        fs.run(QueryRequest(query, Variables(Map())), out).run._2.fold(
          e => liftE(Task.fail(new RuntimeException(e.message))),
          _.leftMap(PEvalError(_)))
      def stripId(ds: IndexedSeq[Data]) = ds.map {
        case Data.Obj(values) =>
          val values1: Map[String, Data] = values.map { case ("value", Data.Obj(values2)) => "value" -> Data.Obj(values2 - "_id"); case d => d }
          Data.Obj(values1)
        case d => d
      }

      "leave no temps behind" in {
        (for {
          tmp    <- liftE[ProcessingError](genTempFile)
          _      <- fs.save(TestDir ++ tmp, oneDoc)

          before <- fs.lsAll(Path.Root).leftMap(PPathError(_))

          // NB: this query *does* use temps (not streamable)
          expr   <- parse("select *, a+1 from " + tmp.simplePathname + " order by a")
          rez    <- eval(fs, expr)

          after  <- fs.lsAll(Path.Root).leftMap(PPathError(_))
        } yield {
          stripId(rez) must_== Vector(
            Data.Obj(ListMap("value" -> Data.Obj(ListMap(
              "a" -> Data.Int(1), "1" -> Data.Int(2))))))
          after must contain(exactly(before.toList: _*))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly

      "leave only the output behind" in {
        (for {
          tmp    <- liftE[ProcessingError](genTempFile)
          _      <- fs.save(TestDir ++ tmp, oneDoc)

          before <- fs.lsAll(Path.Root).leftMap(PPathError(_))

          outReq <- liftE(genTempFile)
          // NB: this query *does* use temps (not streamable)
          expr   <- parse("select *, a+1 from " + tmp.simplePathname + " order by a")
          outAct <- run(fs, expr, TestDir ++ outReq)
          rez    <- fs.scan(outAct.path, 0, None).runLog.leftMap(PResultError(_))

          after  <- fs.lsAll(Path.Root).leftMap(PPathError(_))
        } yield {
          outAct.path.asRelative must_== TestDir ++ outReq
          stripId(rez) must_== Vector(
            Data.Obj(ListMap("value" -> Data.Obj(ListMap(
              "a" -> Data.Int(1), "1" -> Data.Int(2))))))
          after must contain(exactly(FilesystemNode(TestDir ++ outReq, None) :: before.toList: _*))
        }).fold(_ must beNull, ι).run
      }.skippedOnReadOnly
    }

    val cleanup = step {
      deleteTempFiles(fs, TestDir).run
    }
  }
}
