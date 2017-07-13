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

package quasar.physical.sparkcore.fs.hdfs

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp.free._
import quasar.fs._, FileSystemError._, WriteFile._
import pathy.Path.posixCodec

import java.io.OutputStream;
import java.io.BufferedWriter
import java.io.OutputStreamWriter

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object writefile {

  final case class HdfsWriteCursor(hdfs: FileSystem, bw: BufferedWriter)

  private def toPath(apath: APath): Task[Path] = Task.delay {
    new Path(posixCodec.unsafePrintPath(apath))
  }

  def chrooted[S[_]](prefix: ADir, fileSystem: Task[FileSystem])(implicit
    s0: KeyValueStore[WriteHandle, HdfsWriteCursor, ?] :<: S,
    s1: MonotonicSeq :<: S,
    s2: Task :<: S
  ) : WriteFile ~> Free[S, ?] =
    flatMapSNT(interpret(fileSystem)) compose chroot.writeFile[WriteFile](prefix)

  def interpret[S[_]](fileSystem: Task[FileSystem])(implicit
    s0: KeyValueStore[WriteHandle, HdfsWriteCursor, ?] :<: S,
    s1: MonotonicSeq :<: S,
    s2: Task :<: S
  ) : WriteFile ~> Free[S, ?] =
    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wr: WriteFile[A]): Free[S, A] = wr  match {
        case Open(f) => open(f, fileSystem)
        case Write(h, ch) => write(h, ch)
        case Close(h) => close(h)
      }
    }

  def open[S[_]](f: AFile, fileSystem: Task[FileSystem]) (implicit
    writers: KeyValueStore.Ops[WriteHandle, HdfsWriteCursor, S],
    sequence: MonotonicSeq.Ops[S],
    s2: Task :<: S
  ): Free[S, FileSystemError \/ WriteHandle] = {

    def createCursor: Free[S, HdfsWriteCursor] = lift(for {
      path <- toPath(f)
      hdfs <- fileSystem
    } yield {
      val os: OutputStream = hdfs.create(path, new Progressable() {
        override def progress(): Unit = {}
      })
      val bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) )
      HdfsWriteCursor(hdfs, bw)
    }).into[S]

    for {
      hwc <- createCursor
      id <- sequence.next
      h = WriteHandle(f, id)
      _ <- writers.put(h, hwc)
    } yield h.right[FileSystemError]
  }

  def write[S[_]](h: WriteHandle, chunks: Vector[Data])(implicit
    writers: KeyValueStore.Ops[WriteHandle, HdfsWriteCursor, S],
    s2: Task :<: S
  ): Free[S, Vector[FileSystemError]] = {

    implicit val codec: DataCodec = DataCodec.Precise

    def _write(bw: BufferedWriter): Free[S, Vector[FileSystemError]] = {

      val lines: Vector[(String, Data)] =
        chunks.map(data => DataCodec.render(data) strengthR data).unite

      lift(Task.delay(lines.flatMap {
        case (line, data) =>
          \/.fromTryCatchNonFatal{
            bw.write(line)
            bw.newLine()
          }.fold(
            ex => Vector(writeFailed(data, ex.getMessage)),
            u => Vector.empty[FileSystemError]
          )
      })).into[S]
    }

    val findAndWrite: OptionT[Free[S,?], Vector[FileSystemError]] = for {
      HdfsWriteCursor(_, bw) <- writers.get(h)
      errors <- _write(bw).liftM[OptionT]
    } yield errors

    findAndWrite.fold (
      errs => errs,
      Vector[FileSystemError](unknownWriteHandle(h))
    )
  }

  def close[S[_]](h: WriteHandle)(implicit
    writers: KeyValueStore.Ops[WriteHandle, HdfsWriteCursor, S]
  ): Free[S, Unit] = (for {
    HdfsWriteCursor(hdfs, br) <- writers.get(h)
    _ <- writers.delete(h).liftM[OptionT]
  } yield {
    br.close()
    hdfs.close()
  }).run.void
}
