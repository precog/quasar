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
import quasar.contrib.pathy._
import quasar.effect.Capture
import quasar.fp.ski._
import quasar.fs.FileSystemError
import quasar.fs.FileSystemError._
import quasar.fs.FileSystemErrT
import quasar.fs.PathError._
import quasar.physical.sparkcore.fs.SparkConnectorDetails, SparkConnectorDetails._
import quasar.physical.sparkcore.fs.hdfs.parquet.ParquetRDD

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.OutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.Progressable
import pathy.Path._
import scalaz._, Scalaz._
import org.apache.spark._
import org.apache.spark.rdd._

class queryfile[F[_]:Capture:Bind](fileSystem: F[FileSystem]) {

  private def toPath(apath: APath): F[Path] = Capture[F].capture {
    new Path(posixCodec.unsafePrintPath(apath))
  }

  private def fetchRdd[F[_]:Capture](sc: SparkContext, pathStr: String): F[RDD[Data]] = Capture[F].capture {
    import ParquetRDD._
    // TODO add magic number support to distinguish
    if(pathStr.endsWith(".parquet"))
      sc.parquet(pathStr)
    else
      sc.textFile(pathStr)
        .map(raw => DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι))
  }

  def rddFrom[F[_]:Capture](f: AFile)(hdfsPathStr: AFile => F[String])(implicit
    reader: MonadReader[F, SparkContext]
  ): F[RDD[Data]] = for {
    pathStr <- hdfsPathStr(f)
    sc <- reader.asks(ι)
    rdd <- fetchRdd[F](sc, pathStr)
  } yield rdd

  def store(rdd: RDD[Data], out: AFile): F[Unit] = for {
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
  }

  def fileExists(f: AFile): F[Boolean] = for {
    path <- toPath(f)
    hdfs <- fileSystem
  } yield {
    val exists = hdfs.exists(path)
    hdfs.close()
    exists
  }

  def listContents(d: ADir): FileSystemErrT[F, Set[PathSegment]] = EitherT(for {
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
  })
}

object queryfile {

  def detailsInterpreter[F[_]:Capture](
    fileSystem: F[FileSystem],
    hdfsPathStr: AFile => F[String]
  )(implicit
    reader: MonadReader[F, SparkContext]
  ): SparkConnectorDetails ~> F =
    new (SparkConnectorDetails ~> F) {
      val qf = new queryfile[F](fileSystem)

      def apply[A](from: SparkConnectorDetails[A]) = from match {
        case FileExists(f)       => qf.fileExists(f)
        case ReadChunkSize       => 5000.point[F]
        case StoreData(rdd, out) => qf.store(rdd, out)
        case ListContents(d)     => qf.listContents(d).run
        case RDDFrom(f)          => qf.rddFrom(f)(hdfsPathStr)
      }
    }
}
