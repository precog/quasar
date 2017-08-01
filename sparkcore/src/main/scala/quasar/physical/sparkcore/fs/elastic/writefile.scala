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

package quasar.physical.sparkcore.fs.elastic

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp.free._
import quasar.fs._, FileSystemError._, WriteFile._

import org.elasticsearch.spark._
import org.apache.spark._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object writefile {

  final case class WriteCursor(afile: AFile)

  def interpreter[S[_]](implicit
    s0: KeyValueStore[WriteHandle, WriteCursor, ?] :<: S,
    s1: MonotonicSeq :<: S,
    s2: Task :<: S,
    read: Read.Ops[SparkContext, S]
  ) : WriteFile ~> Free[S, ?] =
    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wr: WriteFile[A]): Free[S, A] = wr match {
        case Open(f) => open(f)
        case Write(h, ch) => write(h, ch)
        case Close(h) => close(h)
      }
    }

  def open[S[_]](f: AFile) (implicit
    writers: KeyValueStore.Ops[WriteHandle, WriteCursor, S],
    sequence: MonotonicSeq.Ops[S],
    s2: Task :<: S
  ): Free[S, FileSystemError \/ WriteHandle] = for {
    c <- WriteCursor(f).point[Free[S, ?]]
    id <- sequence.next
    h = WriteHandle(f, id)
    _ <- writers.put(h, c)
  } yield h.right[FileSystemError]

  def write[S[_]](h: WriteHandle, chunks: Vector[Data])(implicit
    writers: KeyValueStore.Ops[WriteHandle, WriteCursor, S],
    s2: Task :<: S,
    read: Read.Ops[SparkContext, S]
  ): Free[S, Vector[FileSystemError]] = {

    implicit val codec: DataCodec = DataCodec.Precise

    def writeToFile(f: AFile): Free[S, Vector[FileSystemError]] = read.asks { (sc: SparkContext) => {
      val lines = chunks.map(data => DataCodec.render(data)).toList.map(_.toList).join
      lift(Task.delay {
        sc.makeRDD(lines).saveJsonToEs(file2ES(f).shows)
        // TODO_ES handle errors
        Vector.empty[FileSystemError]
      }).into[S]
    }}.join

    val findAndWrite: OptionT[Free[S,?], Vector[FileSystemError]] = for {
      WriteCursor(f) <- writers.get(h)
      errors <- writeToFile(f).liftM[OptionT]
    } yield errors

    findAndWrite.fold (
      errs => errs,
      Vector[FileSystemError](unknownWriteHandle(h))
    )
  }

  def close[S[_]](h: WriteHandle)(implicit
    writers: KeyValueStore.Ops[WriteHandle, WriteCursor, S]
  ): Free[S, Unit] = for {
    _ <- writers.delete(h)
  } yield ()
}
