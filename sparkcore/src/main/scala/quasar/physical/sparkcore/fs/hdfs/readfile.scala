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

package quasar.physical.sparkcore.fs.hdfs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.effect.Read
import quasar.fp.free._
import quasar.physical.sparkcore.fs.readfile.{Offset, Limit}
import quasar.physical.sparkcore.fs.readfile.Input

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext
import org.apache.spark.rdd._

import scalaz._
import scalaz.concurrent.Task

object readfile {

  def rddFrom[S[_]](f: AFile, offset: Offset, maybeLimit: Limit)(hdfsPathStr: AFile => Task[String])(implicit
    read: Read.Ops[SparkContext, S],
    s1: Task :<: S
  ): Free[S, RDD[String]] =
    (lift(hdfsPathStr(f)).into[S]) >>= (pathStr => read.asks { sc =>
      sc.textFile(pathStr)
        .zipWithIndex()
        .filter {
        case (value, index) =>
          maybeLimit.fold(
            index >= offset.value
          ) (
            limit => index >= offset.value && index < limit.value + offset.value
          )
      }.map{
        case (value, index) => value
      }
    })

  def fileExists[S[_]](f: AFile)(hdfsPathStr: AFile => Task[String], fileSystem: Task[FileSystem])(
    implicit s0: Task :<: S): Free[S, Boolean] =
    lift(hdfsPathStr(f).flatMap { pathStr =>
      fileSystem.map(fs => fs.exists(new Path(pathStr)))
    }).into[S]

  // TODO arbitrary value, more or less a good starting point
  // but we should consider some measuring
  def readChunkSize: Int = 5000

  def input[S[_]](hdfsPathStr: AFile => Task[String], fileSystem: Task[FileSystem])(implicit
    read: Read.Ops[SparkContext, S], s0: Task :<: S) =
    Input(
      (f,off, lim) => rddFrom(f, off, lim)(hdfsPathStr),
      f => fileExists(f)(hdfsPathStr, fileSystem),
      readChunkSize _
    )

}
