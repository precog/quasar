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
import quasar.physical.sparkcore.fs.queryfile.Input
import quasar.contrib.pathy._
import quasar.fs.FileSystemError
import quasar.fs.FileSystemError._
import quasar.fs.FileSystemErrT
import quasar.fs.PathError._
import quasar.fp.free._
import quasar.contrib.pathy._

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.OutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.Progressable
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import org.apache.spark._
import org.apache.spark.rdd._

class queryfile(fileSystem: Task[FileSystem]) {

  private def toPath(apath: APath): Task[Path] = Task.delay {
    new Path(posixCodec.unsafePrintPath(apath))
  }

  def fromFile(sc: SparkContext, file: AFile): Task[RDD[Data]] = for {
    hdfs <- fileSystem
    pathStr <- Task.delay {
      val pathStr = posixCodec.unsafePrintPath(file)
      val host = hdfs.getUri().getHost()
      val port = hdfs.getUri().getPort()
      s"hdfs://$host:$port$pathStr"
    }
    rdd <- readfile.fetchRdd(sc, pathStr)
  } yield rdd

  def store[S[_]](rdd: RDD[Data], out: AFile)(implicit
    S: Task :<: S
  ): Free[S, Unit] = lift(for {
    path <- toPath(out)
    hdfs <- fileSystem
  } yield {
    val os: OutputStream = hdfs.create(path, new Progressable() {
      override def progress(): Unit = {}
    })
    val bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"))

    rdd.flatMap(DataCodec.render(_)(DataCodec.Precise).toList).collect().foreach(v => {
        bw.write(v)
        bw.newLine()
    })
    bw.close()
    hdfs.close()
  }).into[S]

  def fileExists[S[_]](f: AFile)(implicit
    S: Task :<: S
  ): Free[S, Boolean] = lift(for {
    path <- toPath(f)
    hdfs <- fileSystem
  } yield {
    val exists = hdfs.exists(path)
    hdfs.close()
    exists
  }).into[S]

  def listContents[S[_]](d: ADir)(implicit
    S: Task :<: S
  ): FileSystemErrT[Free[S, ?], Set[PathSegment]] = EitherT(lift(for {
    path <- toPath(d)
    hdfs <- fileSystem
  } yield {
    val result = if(hdfs.exists(path)) {
      hdfs.listStatus(path).toSet.map {
        case file if file.isFile() => FileName(file.getPath().getName()).right[DirName]
        case directory => DirName(directory.getPath().getName()).left[FileName]
      }.right[FileSystemError]
    } else pathErr(pathNotFound(d)).left[Set[PathSegment]]
    hdfs.close
    result
  }).into[S])

  def readChunkSize: Int = 5000

  def input[S[_]](implicit s0: Task :<: S): Input[S] =
    Input[S](fromFile _, store[S] _, fileExists[S] _, listContents[S] _, readChunkSize _)
}

object queryfile {
  def input[S[_]](fileSystem: Task[FileSystem])(implicit s0: Task :<: S): Input[S] = new queryfile(fileSystem).input
}
