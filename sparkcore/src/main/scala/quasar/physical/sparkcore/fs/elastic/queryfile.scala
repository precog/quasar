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
import quasar.physical.sparkcore.fs.queryfile.Input
import quasar.contrib.pathy._
import quasar.fs.FileSystemError
import quasar.fs.FileSystemErrT
import quasar.fp.free._
import quasar.fp.ski._
import quasar.contrib.pathy._

import org.elasticsearch.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfile {

  private def parseIndex(adir: ADir) = posixCodec.unsafePrintPath(adir).replace("/", "") // TODO_ES handle invalid paths

  def fromFile(sc: SparkContext, file: AFile): Task[RDD[Data]] = Task.delay {
    sc
      .esJsonRDD(file2ES(file).shows)
      .map(_._2)
      .map(raw => DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι))
  }

  def store[S[_]](rdd: RDD[Data], out: AFile)(implicit
    S: Task :<: S
  ): Free[S, Unit] = lift(Task.delay {
    rdd.flatMap(DataCodec.render(_)(DataCodec.Precise).toList)
      .saveJsonToEs(file2ES(out).shows)
  }).into[S]

  def fileExists[S[_]](f: AFile)(implicit
    E: ElasticCall.Ops[S]
  ): Free[S, Boolean] = E.typeExists(file2ES(f))

  def listContents[S[_]](adir: ADir)(implicit
    E: ElasticCall.Ops[S]
  ): FileSystemErrT[Free[S, ?], Set[PathSegment]] = {
    val toDirName: String => PathSegment = t => DirName(t).left[FileName]
    val toFileName: String => PathSegment = t => FileName(t).right[DirName]
    val rootFolder: String => String = _.split(separator).head

    val segments = if(adir === rootDir)
      E.listIndices.map(_.map(rootFolder).toSet.map(toDirName))
    else {
      val prefix = dir2Index(adir)
      val folders = E.listIndices.map(indices =>
        indices
          .filter(_.startsWith(prefix))
          .map(s => s.substring(s.indexOf(prefix) + prefix.length))
          .map {
            case s if s.contains(separator) => s.substring(0, s.indexOf(separator))
            case s => s
          }
          .toSet
          .map(toDirName))
      val index = if(prefix.endsWith(separator)) prefix.substring(0, prefix.length - separator.length) else prefix
      val files = E.listTypes(index).map(_.map(toFileName).toSet)
      (folders |@| files)(_ ++ _)
    }
    EitherT(segments.map(_.right[FileSystemError]))
  }

  def readChunkSize: Int = 5000

  def input[S[_]](implicit
    s0: Task :<: S,
    elastic: ElasticCall :<: S
  ): Input[S] =
    Input[S](fromFile _, store[S] _, fileExists[S] _, listContents[S] _, readChunkSize _)
}
