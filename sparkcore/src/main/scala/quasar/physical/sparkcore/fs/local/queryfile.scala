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

package quasar.physical.sparkcore.fs.local

import quasar.Predef._
import quasar.{Data, DataCodec}
import quasar.fs.FileSystemError
import quasar.fs.PathError._
import quasar.physical.sparkcore.fs.queryfile.Input
import quasar.fs.FileSystemError._
import quasar.contrib.pathy._
import quasar.fp.ski._

import java.io.{File, PrintWriter, FileOutputStream}
import java.nio.file._

import org.apache.spark._
import org.apache.spark.rdd._
import pathy.Path._
import scalaz._, Scalaz._, scalaz.concurrent.Task

object queryfile {

  def fromFile(sc: SparkContext, file: AFile): Task[RDD[Data]] = Task.delay {
    sc.textFile(posixCodec.unsafePrintPath(file))
      .map(raw => DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι))
  }

  def store(rdd: RDD[Data], out: AFile): Task[Unit] = Task.delay {
    val ioFile = new File(posixCodec.printPath(out))
    val pw = new PrintWriter(new FileOutputStream(ioFile, true))
    rdd.flatMap(DataCodec.render(_)(DataCodec.Precise).toList).collect().foreach(v => pw.write(s"$v\n"))
    pw.close()
  }

  def fileExists(f: AFile): Task[Boolean] = Task.delay {
    Files.exists(Paths.get(posixCodec.unsafePrintPath(f)))
  }

  def listContents(d: ADir): EitherT[Task, FileSystemError, Set[PathSegment]] = EitherT(Task.delay {
    val directory = new File(posixCodec.unsafePrintPath(d))
    if(directory.exists()) {
      \/.fromTryCatchNonFatal{
        directory.listFiles.toSet[File].map {
          case file if file.isFile() => FileName(file.getName()).right[DirName]
          case directory => DirName(directory.getName()).left[FileName]
        }
      }
        .leftMap {
        case e =>
          pathErr(invalidPath(d, e.getMessage()))
      }
    } else pathErr(pathNotFound(d)).left[Set[PathSegment]]
  })

  def readChunkSize: Int = 5000

  def input: Input = Input(fromFile _, store _, fileExists _, listContents _, readChunkSize _)
}
