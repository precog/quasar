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
import quasar.{Data, DataArbitrary}
import quasar.contrib.pathy._
import quasar.fp._, eitherT._

import org.specs2.specification.core._
import pathy.scalacheck.PathyArbitrary._
import scalaz._
import scalaz.std.vector._
import scalaz.stream._
import scalaz.syntax.monad._

/** FIXME: couldn't make this one work with Qspec. */
class WriteFileSpec extends org.specs2.mutable.Specification with org.specs2.ScalaCheck with FileSystemFixture {
  import DataArbitrary._, FileSystemError._, PathError._

  type DataWriter = (AFile, Process0[Data]) => Process[write.M, FileSystemError]

  private def withDataWriters(
    streaming: (String, (AFile, Process[write.F, Data]) => Process[write.M, FileSystemError]),
    nonStreaming: (String, (AFile, Vector[Data]) => write.M[Vector[FileSystemError]])
  )(f: (String, DataWriter) => Fragments): Fragments = {
    val st: DataWriter = (f, xs) =>
      streaming._2(f, xs)

    val ns: DataWriter = (f, xs) =>
      nonStreaming._2(f, xs.toVector).liftM[Process].flatMap(Process.emitAll)

    Fragments.foreach(List((streaming._1, st), (nonStreaming._1, ns)))(f.tupled)
  }

  "WriteFile" should {

    withDataWriters(("append", write.append), ("appendThese", write.appendThese)) { (n, wt) =>
      s"$n should consume input and close write handle when finished" >> prop {
        (f: AFile, xs: Vector[Data]) =>

        val p = wt(f, xs.toProcess).drain ++ read.scanAll(f)

        type Result[A] = FileSystemErrT[MemStateTask,A]

        p.translate[Result](MemTask.interpretT).runLog.run
          .leftMap(_.wm)
          .run(emptyMem)
          .unsafePerformSync must_=== ((Map.empty, \/.right(xs)))
      }
    }

    "append should aggregate all `PartialWrite` errors and emit the sum" >> prop {
      (f: AFile, xs: Vector[Data]) => (xs.length > 1) ==> {
        val wf = writeFailed(Data.Str("foo"), "b/c reasons")
        val ws = Vector(wf) +: xs.tail.as(Vector(partialWrite(1)))

        MemFixTask.runLogWithWrites(ws.toList, write.append(f, xs.toProcess))
          .run.eval(emptyMem)
          .unsafePerformSync.toEither must beRight(Vector(wf, partialWrite(xs.length - 1)))
      }
    }

    "append should fail, but persist all data emitted prior to failure, when source fails" >> prop {
      (f: AFile, xs: Vector[Data], ys: Vector[Data]) =>

      val src = xs.toProcess ++
                Process.fail(new RuntimeException("SRCFAIL")) ++
                ys.toProcess

      val p = write.append(f, src).drain onFailure {
        case err if err.getMessage == "SRCFAIL" => read.scanAll(f)
        case err                                => Process.fail(err)
      }

      MemTask.runLogEmpty(p).unsafePerformSync.toEither must beRight(xs)
    }

    withDataWriters(("save", write.save), ("saveThese", write.saveThese)) { (n, wt) =>
      s"$n should replace existing file" >> prop {
        (f: AFile, xs: Vector[Data], ys: Vector[Data]) =>

        val p = (write.append(f, xs.toProcess) ++ wt(f, ys.toProcess)).drain ++ read.scanAll(f)

        MemTask.runLogEmpty(p).unsafePerformSync must_=== \/-(ys)
      }

      s"$n with empty input should create an empty file" >> prop { f: AFile =>
        val p = wt(f, Process.empty) ++ query.fileExistsM(f).liftM[Process]

        MemTask.runLogEmpty(p).unsafePerformSync must_=== \/-(Vector(true))
      }

      s"$n should leave existing file untouched on failure" >> prop {
        (f: AFile, xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
          val err = writeFailed(Data.Str("bar"), "")
          val ws = xs.as(Vector()) :+ Vector(err)
          val p = (write.append(f, xs.toProcess) ++ wt(f, ys.toProcess)).drain ++ read.scanAll(f)

          MemFixTask.runLogWithWrites(ws.toList, p).run
            .leftMap(_.contents.keySet)
            .run(emptyMem)
            .unsafePerformSync must_=== ((Set(f), \/.right(xs)))
        }
      }
    }

    "save should fail and write nothing when source fails" >> prop {
      (f: AFile, xs: Vector[Data], ys: Vector[Data], zs: Vector[Data]) =>

      val src = xs.toProcess ++
                Process.fail(new RuntimeException("SRCFAIL")) ++
                ys.toProcess

      val init = write.save(f, zs.toProcess).drain

      val p = write.save(f, src).drain onFailure {
        case err if err.getMessage == "SRCFAIL" => read.scanAll(f)
        case err                                => Process.fail(err)
      }

      MemTask.runLogEmpty(init ++ p).unsafePerformSync.toEither must beRight(zs)
    }

    withDataWriters(("create", write.create), ("createThese", write.createThese)) { (n, wt) =>
      s"$n should fail if file exists" >> prop {
        (f: AFile, xs: Vector[Data], ys: Vector[Data]) =>

        val p = write.append(f, xs.toProcess) ++ wt(f, ys.toProcess)

        MemTask.runLogEmpty(p).unsafePerformSync.toEither must beLeft(pathErr(pathExists(f)))
      }

      s"$n should consume all input into a new file" >> prop {
        (f: AFile, xs: Vector[Data]) =>

        val p = wt(f, xs.toProcess) ++ read.scanAll(f)

        MemTask.runLogEmpty(p).unsafePerformSync must_=== \/-(xs)
      }
    }

    withDataWriters(("replace", write.replace), ("replaceWithThese", write.replaceWithThese)) { (n, wt) =>
      s"$n should fail if the file does not exist" >> prop {
        (f: AFile, xs: Vector[Data]) =>

        MemTask.runLogEmpty(wt(f, xs.toProcess))
          .unsafePerformSync.toEither must beLeft(pathErr(pathNotFound(f)))
      }

      s"$n should leave the existing file untouched on failure" >> prop {
        (f: AFile, xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
          val err = writeFailed(Data.Int(42), "")
          val ws = xs.as(Vector()) :+ Vector(err)
          val p = (write.append(f, xs.toProcess) ++ wt(f, ys.toProcess)).drain ++ read.scanAll(f)

          MemFixTask.runLogWithWrites(ws.toList, p).run
            .leftMap(_.contents.keySet)
            .run(emptyMem)
            .unsafePerformSync must_=== ((Set(f), \/.right(xs)))
        }
      }

      s"$n should overwrite the existing file with new data" >> prop {
        (f: AFile, xs: Vector[Data], ys: Vector[Data]) =>

        val p = write.save(f, xs.toProcess) ++ wt(f, ys.toProcess) ++ read.scanAll(f)

        MemTask.runLogEmpty(p).unsafePerformSync must_=== \/-(ys)
      }
    }
  }
}
