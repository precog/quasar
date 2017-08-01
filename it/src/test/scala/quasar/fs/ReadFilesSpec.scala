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

package quasar.fs

import slamdata.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.fp._
import quasar.fp.numeric._

import java.lang.RuntimeException
import scala.annotation.tailrec

import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.{Positive => RPositive,_}
import eu.timepit.refined.scalacheck.numeric._
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import monocle.std.{disjunction => D}
import org.scalacheck.Prop
import pathy.Path._
import scalaz.{EphemeralStream => EStream, _}, Scalaz._
import scalaz.stream._

class ReadFilesSpec extends FileSystemTest[BackendEffect](FileSystemTest.allFsUT) {
  import ReadFilesSpec._, FileSystemError._
  import ReadFile._

  val read   = ReadFile.Ops[BackendEffect]
  val write  = WriteFile.Ops[BackendEffect]
  val manage = ManageFile.Ops[BackendEffect]

  def loadForReading(run: Run): FsTask[Unit] = {
    type P[A] = Process[write.M, A]

    def loadDatum(td: TestDatum) = {
      val src = chunkStream(td.data, 1000)
        .foldRight(Process.halt: Process0[Vector[Data]])(xs => p => Process.emit(xs) ++ p)
      write.appendChunked(td.file, src)
    }

    List(emptyFile, smallFile, largeFile)
      .foldMap(loadDatum)(PlusEmpty[P].monoid)
      .flatMap(err => Process.fail(new RuntimeException(err.shows)))
      .translate[FsTask](runT(run))
      .run
  }

  def deleteForReading(run: Run): FsTask[Unit] =
    runT(run)(manage.delete(readsPrefix))

  implicit val setDataEq = Equal.equalA[Set[Data]]

  fileSystemShould { (fs, _) =>
    implicit val run = fs.testInterpM

    "Reading Files" should {
      // Load read-only data
      step((deleteForReading(fs.setupInterpM).run.void *> loadForReading(fs.setupInterpM).run.void).unsafePerformSync)

      "read unopened file handle returns UnknownReadHandle" >>* {
        val h = ReadHandle(rootDir </> file("f1"), 42)
        read.unsafe.read(h).run map { r =>
          r must_= unknownReadHandle(h).left
        }
      }

      "read closed file handle returns UnknownReadHandle" >>* {
        val r = for {
          h  <- read.unsafe.open(smallFile.file, 0L, None)
          _  <- read.unsafe.close(h).liftM[FileSystemErrT]
          xs <- read.unsafe.read(h)
        } yield xs

        r.run map { x =>
          (D.left composePrism unknownReadHandle).nonEmpty(x) must beTrue
        }
      }

      "scan an empty file succeeds, yielding no data" >> {
        val r = runLogT(run, read.scanAll(emptyFile.file))
        r.run_\/ must_= Vector.empty.right
      }

      "scan with offset zero and no limit reads entire file" >> {
        val r = runLogT(run, read.scan(smallFile.file, 0L, None))

        r.run_\/.map(_.toSet) must_= smallFile.data.toSet.right
      }

      "scan with offset = |file| and no limit yields no data" >> {
        val r = runLogT(run, read.scan(smallFile.file, smallFileSize, None))
        r.run_\/ must_= Vector.empty.right
      }

      /** TODO: This just specifies the default MongoDB behavior as that was
        *       the easiest to implement, however an argument could be made
        *       for erroring instead of returning nothing.
        */
      "scan with offset k, where k > |file|, and no limit succeeds with empty result" >> {
        val r = runLogT(run, read.scan(smallFile.file, smallFileSize |+| 1L, None))
        r.run_\/ must_= Vector.empty.right
      }

      // disabled for mimir, not because it doesn't work, but because it's absurdly slow
      "scan with offset k > 0 and no limit skips first k data" >> pendingFor(fs)(Set("mimir")) {
        prop { k: Int Refined RPositive =>
          val rFull = runLogT(run, read.scan(smallFile.file, 0L, None)).run_\/
          val r     = runLogT(run, read.scan(smallFile.file, widenPositive(k), None)).run_\/

          val d = rFull.map(_.drop(k))

          (rFull.map(_.toSet) must_= smallFile.data.toSet.right) and
          (r must_= d)
        }.set(minTestsOk = 10)
      }

      "scan with offset zero and limit j stops after j data" >> {
        prop { j: Int Refined Interval.Open[W.`1`.T, SmallFileSize] =>
          val limit = Positive(j.value.toLong).get // Not ideal, but simplest solution for now

          val rFull = runLogT(run, read.scan(smallFile.file, 0L, None)).run_\/
          val r     = runLogT(run, read.scan(smallFile.file, 0L, Some(limit))).run_\/

          val d = rFull.map(_.take(j.value))

          (rFull.map(_.toSet) must_= smallFile.data.toSet.right) and
          (r must_= d)
        }.set(minTestsOk = 10)
      }

      "scan with offset k and limit j takes j data, starting from k" >> {
        Prop.forAll(
          chooseRefinedNum[Refined, Int, RPositive](1, 50),
          chooseRefinedNum[Refined, Int, NonNegative](0, 50))
        { (j: Int Refined RPositive, k: Int Refined NonNegative) =>
          val rFull = runLogT(run, read.scan(smallFile.file, 0L, None)).run_\/
          val r     = runLogT(run, read.scan(smallFile.file, k, Some(j))).run_\/

          val d = rFull.map(_.drop(k).take(j.value))

          (rFull.map(_.toSet) must_= smallFile.data.toSet.right) and
          (r must_= d)
        }.set(minTestsOk = 5)
      }

      "scan with offset zero and limit j, where j > |file|, stops at end of file" >> {
        prop { j: Int Refined Greater[SmallFileSize] =>
          val limit = Some(Positive(j.value.toLong).get) // Not ideal, but simplest solution for now

          val rFull = runLogT(run, read.scan(smallFile.file, 0L, None)).run_\/
          val r     = runLogT(run, read.scan(smallFile.file, 0L, limit)).run_\/

          val d = rFull.map(_.take(j.value))

          (j.value must beGreaterThan(smallFile.data.length))    and
          (rFull.map(_.toSet) must_= smallFile.data.toSet.right) and
          (r must_= d)
        }.set(minTestsOk = 10)
      }

      "scan very long file is stack-safe" >> {
        runLogT(run, read.scanAll(largeFile.file).foldMap(_ => 1))
          .runEither must beRight(List(largeFile.data.length).toIndexedSeq)
      }

      // TODO: This was copied from existing tests, but what is being tested?
      "scan very long file twice" >> {
        val r = runLogT(run, read.scanAll(largeFile.file).foldMap(_ => 1))
        val l = Vector(largeFile.data.length)

        (r.run_\/ must_= l.right) and
        (r.run_\/ must_= l.right)
      }

      step(deleteForReading(fs.setupInterpM).runVoid)
    }
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

  val largeFile = TestDatum(
    readsPrefix </> dir("length") </> file("large"),
    manyDocs(10000))

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
