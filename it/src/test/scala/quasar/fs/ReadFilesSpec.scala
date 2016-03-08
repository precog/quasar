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

package quasar.fs

import quasar.Predef._
import quasar.Data
import quasar.fp._
import quasar.fp.numeric._
import quasar.fp.numeric.NumericArbitrary._

import java.lang.RuntimeException
import scala.annotation.tailrec

import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.{Positive => RPositive,_}
import eu.timepit.refined.scalacheck.numeric.chooseRefinedNum
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import monocle.std.{disjunction => D}
import org.scalacheck.Arbitrary
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import pathy.Path._
import scalaz.{EphemeralStream => EStream, _}, Scalaz._
import scalaz.stream._
import shapeless.Nat

class ReadFilesSpec extends FileSystemTest[FileSystem](FileSystemTest.allFsUT) with ScalaCheck {
  import ReadFilesSpec._, FileSystemError._, PathError2._
  import ReadFile._

  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  def loadForReading(run: Run): FsTask[Unit] = {
    type P[A] = Process[write.M, A]

    def loadDatum(td: TestDatum) = {
      val src = chunkStream(td.data, 1000)
        .foldRight(Process.halt: Process0[Vector[Data]])(xs => p => Process.emit(xs) ++ p)
      write.appendChunked(td.file, src)
    }

    List(emptyFile, smallFile, largeFile, veryLongFile)
      .foldMap(loadDatum)(PlusEmpty[P].monoid)
      .flatMap(err => Process.fail(new RuntimeException(err.shows)))
      .translate[FsTask](runT(run))
      .run
  }

  def deleteForReading(run: Run): FsTask[Unit] =
    runT(run)(manage.delete(readsPrefix))

  fileSystemShould { fs =>
    implicit val run = fs.testInterpM

    "Reading Files" should {
      // Load read-only data
      step((deleteForReading(fs.setupInterpM).run.void *> loadForReading(fs.setupInterpM).run.void).run)

      "open returns PathNotFound when file DNE" >>* {
        val dne = rootDir </> dir("doesnt") </> file("exist")
        read.unsafe.open(dne, 0L, None).run map { r =>
          r.toEither must beLeft(pathError(pathNotFound(dne)))
        }
      }

      "read unopened file handle returns UnknownReadHandle" >>* {
        val h = ReadHandle(rootDir </> file("f1"), 42)
        read.unsafe.read(h).run map { r =>
          r.toEither must beLeft(unknownReadHandle(h))
        }
      }

      "read closed file handle returns UnknownReadHandle" >>* {
        val r = for {
          h  <- read.unsafe.open(smallFile.file, 0L, None)
          _  <- read.unsafe.close(h).liftM[FileSystemErrT]
          xs <- read.unsafe.read(h)
        } yield xs

        r.run map { x =>
          (D.left composePrism unknownReadHandle).isMatching(x) must beTrue
        }
      }

      "scan an empty file succeeds, yielding no data" >> {
        val r = runLogT(run, read.scanAll(emptyFile.file))
        r.runEither must beRight((xs: scala.collection.IndexedSeq[Data]) => xs must beEmpty)
      }

      "scan with offset zero and no limit reads entire file" >> {
        val r = runLogT(run, read.scan(smallFile.file, 0L, None))
        r.runEither must beRight(smallFile.data.toIndexedSeq)
      }

      "scan with offset = |file| and no limit yields no data" >> {
        val r = runLogT(run, read.scan(smallFile.file, smallFileSize, None))
        r.runEither must beRight((xs: scala.collection.IndexedSeq[Data]) => xs must beEmpty)
      }

      /** TODO: This just specifies the default MongoDB behavior as that was
        *       the easiest to implement, however an argument could be made
        *       for erroring instead of returning nothing.
        */
      "scan with offset k, where k > |file|, and no limit succeeds with empty result" >> {
        val r = runLogT(run, read.scan(smallFile.file, smallFileSize |+| 1L, None))
        r.runEither must beRight((xs: scala.collection.IndexedSeq[Data]) => xs must beEmpty)
      }

      "scan with offset k > 0 and no limit skips first k data" ! prop { k: Int Refined RPositive =>
        val r = runLogT(run, read.scan(smallFile.file, widenPositive(k), None))
        val d = smallFile.data.zip(EStream.iterate(0)(_ + 1))
                  .dropWhile(_._2 < k.get).map(_._1)

        r.runEither must beRight(d.toIndexedSeq)
      }.set(minTestsOk = 10)

      "scan with offset zero and limit j stops after j data" ! prop { j: Int Refined Interval.Open[W.`1`.T, SmallFileSize] =>
        val limit = Positive(j.get.toLong).get // Not ideal, but simplest solution for now
        val r = runLogT(run, read.scan(smallFile.file, 0L, Some(limit)))

        r.runEither must beRight(smallFile.data.take(j.get).toIndexedSeq)
      }.set(minTestsOk = 10)

      "scan with offset k and limit j takes j data, starting from k" ! Prop.forAll(
        chooseRefinedNum[Refined, Int, RPositive](1, 200),
        chooseRefinedNum[Refined, Int, NonNegative](0, 200)
      ) { (j: Int Refined RPositive, k: Int Refined NonNegative) =>
        val r = runLogT(run, read.scan(largeFile.file, k, Some(j)))
        val d = largeFile.data.zip(EStream.iterate(0)(_ + 1))
                  .dropWhile(_._2 < k.get).map(_._1)
                  .take(j.get)

        r.runEither must beRight(d.toIndexedSeq)
      }.set(minTestsOk = 5)

      "scan with offset zero and limit j, where j > |file|, stops at end of file" ! prop { j: Int Refined Greater[SmallFileSize] =>
          val limit = Some(Positive(j.get.toLong).get) // Not ideal, but simplest solution for now
          val r = runLogT(run, read.scan(smallFile.file, 0L, limit))

          (j.get must beGreaterThan(smallFile.data.length)) and
            (r.runEither must beRight(smallFile.data.toIndexedSeq))
      }.set(minTestsOk = 10)

      "scan very long file is stack-safe" >> {
        runLogT(run, read.scanAll(veryLongFile.file).foldMap(_ => 1))
          .runEither must beRight(List(veryLongFile.data.length).toIndexedSeq)
      }

      // TODO: This was copied from existing tests, but what is being tested?
      "scan very long file twice" >> {
        val r = runLogT(run, read.scanAll(veryLongFile.file).foldMap(_ => 1))
        val l = List(veryLongFile.data.length).toIndexedSeq

        (r.runEither must beRight(l)) and (r.runEither must beRight(l))
      }

      step(deleteForReading(fs.setupInterpM).runVoid)
    }; ()
  }
}

object ReadFilesSpec {
  import FileSystemTest._

  final case class TestDatum(file: AFile, data: EStream[Data])

  val readsPrefix: ADir = rootDir </> dir("forreading")

  val emptyFile = TestDatum(
    readsPrefix </> file("empty"),
    EStream())

  type SmallFileSize = W.`100`.T
  val smallFileSize: Natural = 100L

  val smallFile = TestDatum(
    readsPrefix </> file("small"),
    manyDocs(smallFileSize.toInt))

  val largeFile = {
    val sizeInMb = 10.0
    val bytesPerDoc = 750
    val numDocs = (sizeInMb * 1024 * 1024 / bytesPerDoc).toInt

    def jsonTree(depth: Int): Data =
      if (depth == 0)
        Data.Arr(Data.Str("abc") :: Data.Int(123) :: Data.Str("do, re, mi") :: Nil)
      else
        Data.Obj(ListMap("left" -> jsonTree(depth-1), "right" -> jsonTree(depth-1)))

    def json(i: Int) =
      Data.Obj(ListMap("seq" -> Data.Int(i), "filler" -> jsonTree(3)))

    TestDatum(
      readsPrefix </> file("large"),
      EStream.range(1, numDocs) map (json))
  }

  val veryLongFile = TestDatum(
    readsPrefix </> dir("length") </> file("very.long"),
    manyDocs(100000))

  ////

  private def chunkStream[A](s: EStream[A], size: Int): EStream[Vector[A]] = {
    @tailrec
    def chunk0(xs: EStream[A], v: Vector[A], i: Int): (EStream[A], Vector[A]) =
      if (i == 0)
        (xs, v)
      else
        (xs.headOption, xs.tailOption) match {
          case (Some(a), Some(xss)) =>
            chunk0(xss, v :+ a, i - 1)
          case (Some(a), None) =>
            (EStream(), v :+ a)
          case _ =>
            (EStream(), v)
        }

    EStream.unfold(chunk0(s, Vector(), size)) { case (ys, v) =>
      if (v.isEmpty) None else Some((v, chunk0(ys, Vector(), size)))
    }
  }
}
