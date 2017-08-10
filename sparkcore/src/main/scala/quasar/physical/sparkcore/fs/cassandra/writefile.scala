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

package quasar.physical.sparkcore.fs.cassandra

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.{Data, DataCodec}
import quasar.effect._
import quasar.fs._, WriteFile._, FileSystemError._
import quasar.fp.free._

import org.apache.spark.SparkContext
import pathy.Path.fileParent
import scalaz._, Scalaz._

object writefile {

  import common._

  def chrooted[S[_]](prefix: ADir)(implicit
    s0: KeyValueStore.Ops[WriteHandle, AFile, S],
    s1: MonotonicSeq :<: S,
    s2: Read.Ops[SparkContext, S],
    s3: CassandraDDL.Ops[S]
  ) : WriteFile ~> Free[S, ?] =
    flatMapSNT(interpret) compose chroot.writeFile[WriteFile](prefix)

  def interpret[S[_]](implicit
    s0: KeyValueStore.Ops[WriteHandle, AFile, S],
    s1: MonotonicSeq :<: S,
    s2: Read.Ops[SparkContext, S],
    s3: CassandraDDL.Ops[S]
  ): WriteFile ~> Free[S, ?] = {
    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wr: WriteFile[A]): Free[S, A] = wr match {
        case Open(f) => open(f)
        case Write(h, ch) => write(h, ch)
        case Close(h) => close(h)
      }
    }
  }

  def open[S[_]](file: AFile)(implicit
    writers: KeyValueStore.Ops[WriteHandle, AFile, S],
    gen: MonotonicSeq.Ops[S],
    s0: Read.Ops[SparkContext, S],
    s1: CassandraDDL.Ops[S]
  ): Free[S, FileSystemError \/ WriteHandle] =
    for {
      id <- gen.next
      _ <- create(file)
      wh = WriteHandle(file, id)
      _ <- writers.put(wh, file)
    } yield (wh.right)

  def create[S[_]](file: AFile)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, Unit] = {
    val ks = keyspace(fileParent(file))
    val tb = tableName(file)

    for {
      keyspaceExists <- cass.keyspaceExists(ks)
      _              <- if(keyspaceExists) ().point[Free[S, ?]] else cass.createKeyspace(ks)
      tableExists    <- cass.tableExists(ks, tb)
      _              <- if(tableExists) ().point[Free[S, ?]] else cass.createTable(ks, tb)
    } yield ()
  }

  implicit val codec: DataCodec = DataCodec.Precise

  def write[S[_]](handle: WriteHandle, chunks: Vector[Data])(implicit
    writers: KeyValueStore.Ops[WriteHandle, AFile, S],
    read: Read.Ops[SparkContext, S],
    cass: CassandraDDL.Ops[S]
  ): Free[S, Vector[FileSystemError]] =
    for{
      maybeAFile <- writers.get(handle).run
      errors     <- 
        maybeAFile.cata(
          f => {
            implicit val codec: DataCodec = DataCodec.Precise

            val textChunk: Vector[(String, Data)] =
              chunks.map(d => DataCodec.render(d) strengthR d).unite

            val ks = keyspace(fileParent(f))
            val tn = tableName(f)

            textChunk.map {
              case (text, data) => cass.insertData(ks, tn, text)
            }.sequence.as(Vector.empty[FileSystemError])
          },
          Vector[FileSystemError](unknownWriteHandle(handle)).point[Free[S, ?]]
        )
    } yield errors

  def close[S[_]](handle: WriteHandle)(implicit
    writers: KeyValueStore.Ops[WriteHandle, AFile, S]
  ): Free[S, Unit] = 
    writers.delete(handle)
}
