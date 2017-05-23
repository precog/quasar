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

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.{Data, DataArbitrary}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.foldable._
import quasar.contrib.scalaz.stream._

import org.specs2.specification.core._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.stream._

/** FIXME: couldn't make this one work with Qspec. */
class WriteFileSpec extends org.specs2.mutable.Specification with org.specs2.ScalaCheck with FileSystemFixture {
  import DataArbitrary._, FileSystemError._, PathError._, InMemory.InMemState

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
        (f: AFile, xs: NonEmptyList[Data]) =>

        val write = wt(f, xs.toProcess).runLogCatch.run

        Mem.interpret(write).exec(emptyMem).contents must_=== Map(f -> xs.toVector)
      }
    }

    "append should aggregate all `PartialWrite` errors and emit the sum" >> prop {
      (f: AFile, xs: Vector[Data]) => (xs.length > 1) ==> {
        val wf = writeFailed(Data.Str("foo"), "b/c reasons")
        val ws = Vector(wf) +: xs.tail.as(Vector(partialWrite(1)))

        val doWrite = write.append(f, xs.toProcess).runLogCatch.run

        Mem.interpretInjectingWriteErrors(doWrite, ws.toList).eval(emptyMem) must_===
          Vector(wf, partialWrite(xs.length - 1)).right.right
      }
    }

    "append should fail, but persist all data emitted prior to failure, when source fails" >> prop {
      (f: AFile, xs: NonEmptyList[Data], ys: NonEmptyList[Data]) =>

      val src = xs.toProcess ++
                Process.fail(new RuntimeException("SRCFAIL")) ++
                ys.toProcess

      val doWrite = write.append(f, src).runLogCatch.run

      Mem.interpret(doWrite).exec(emptyMem).contents must_=== Map(f -> xs.toVector)
    }

    withDataWriters(("save", write.save), ("saveThese", write.saveThese)) { (n, wt) =>
      s"$n should replace existing file" >> prop {
        (f: AFile, xs: NonEmptyList[Data], ys: NonEmptyList[Data]) =>

        val write = wt(f, ys.toProcess).runLogCatch.run

        val before = InMemState.fromFiles(Map(f -> xs.toVector))

        val after = InMemState.fromFiles(Map(f -> ys.toVector))

        Mem.interpret(write).run(before).leftMap(_.contents) must_=== ((after.contents, Vector.empty.right.right))
      }

      s"$n should leave existing file untouched on failure" >> prop {
        (f: AFile, xs: NonEmptyList[Data], ys: NonEmptyList[Data]) =>

        val err = writeFailed(Data.Str("bar"), "")
        val ws = Vector(err)
        val write = wt(f, ys.toProcess).runLogCatch.run

        val before = InMemState.fromFiles(Map(f -> xs.toVector))

        Mem.interpretInjectingWriteErrors(write, List(ws)).run(before).leftMap(_.contents) must_===
          ((before.contents, Vector(err).right.right))
      }

      s"$n should properly report errors when writing fails to create a file" >> prop { f: AFile =>
          val err = writeFailed(Data.Str("bar"), "")
          val ws = Vector(err)
          val ys = Vector(Data.Obj(ListMap("foo" -> Data.Int(1))))
          val write = wt(f, ys.toProcess).runLogCatch.run
          val before = InMemState.empty

          Mem.interpretInjectingWriteErrors(write, List(ws))
            .run(before)
            .leftMap(_.contents) must_=== ((before.contents, Vector(err).right.right))
      }
    }

    "save should fail and write nothing when source fails" >> prop {
      (f: AFile, xs: Vector[Data], ys: Vector[Data], zs: Vector[Data]) =>

      val src = xs.toProcess ++
                Process.fail(new RuntimeException("SRCFAIL")) ++
                ys.toProcess

      val doWrite = write.save(f, src).runLogCatch.run

      val before = InMemState.fromFiles(Map(f -> zs))

      Mem.interpret(doWrite).exec(before).contents must_=== before.contents
    }

    withDataWriters(("create", write.create), ("createThese", write.createThese)) { (n, wt) =>
      s"$n should fail if file exists" >> prop {
        (f: AFile, xs: NonEmptyList[Data], ys: NonEmptyList[Data]) =>

        val doWrite = wt(f, ys.toProcess).runLogCatch.run

        val before = InMemState.fromFiles(Map(f -> xs.toVector))

        Mem.interpret(doWrite).run(before) must_=== ((before, pathErr(pathExists(f)).left))
      }

      s"$n should consume all input into a new file" >> prop {
        (f: AFile, xs: NonEmptyList[Data]) =>

        val write = wt(f, xs.toProcess).runLogCatch.run

        Mem.interpret(write)
          .run(emptyMem)
          .leftMap(_.contents) must_=== ((Map(f -> xs.toVector), Vector.empty.right.right))
      }
    }

    withDataWriters(("replace", write.replace), ("replaceWithThese", write.replaceWithThese)) { (n, wt) =>
      s"$n should fail if the file does not exist" >> prop {
        (f: AFile, xs: Vector[Data]) =>

        Mem.interpretEmpty(wt(f, xs.toProcess).runLogCatch.run)
          .toEither must beLeft(pathErr(pathNotFound(f)))
      }

      s"$n should leave the existing file untouched on failure" >> prop {
        (f: AFile, xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
          val err = writeFailed(Data.Int(42), "")
          // We need to introduce an error on the first write since `replaceWithThese` only does one write
          val ws = Vector(err)
          val write = wt(f, ys.toProcess).runLogCatch.run

          val before = InMemState.fromFiles(Map(f -> xs))

          Mem.interpretInjectingWriteErrors(write, List(ws))
            .run(before).leftMap(_.contents) must_=== ((before.contents, Vector(err).right.right))
        }
      }

      s"$n should overwrite the existing file with new data" >> prop {
        (f: AFile, xs: NonEmptyList[Data], ys: NonEmptyList[Data]) =>

        val write = wt(f, ys.toProcess).runLogCatch.run

        val before = InMemState.fromFiles(Map(f -> xs.toVector))
        val after  = InMemState.fromFiles(Map(f -> ys.toVector))

        Mem.interpret(write).run(before).leftMap(_.contents) must_=== ((after.contents, Vector.empty.right.right))
      }
    }
  }
}
