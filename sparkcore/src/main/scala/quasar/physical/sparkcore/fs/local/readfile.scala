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

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp.free._
import quasar.fp.ski._
import quasar.fs._
import quasar.physical.sparkcore.fs.readfile.{Offset, Limit}
import quasar.physical.sparkcore.fs.readfile.Input
import quasar.physical.sparkcore.fs.{genSc => coreGenSc}

import java.io.File

import org.apache.spark._
import org.apache.spark.rdd._
import pathy.Path.posixCodec
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

object readfile {

  def createSparkContext[S[_]](sparkConf: SparkConf)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S,
    FailOps: Failure.Ops[PhysicalError, S]
  ): Free[S, SparkContext] =
    lift(coreGenSc(sparkConf).run).into[S] >>= (_.fold(
      msg => FailOps.fail[SparkContext](UnhandledFSError(new RuntimeException(msg))),
      _.point[Free[S, ?]]
    ))
  
  def rddFrom[S[_]](sc: SparkContext, f: AFile, offset: Offset, maybeLimit: Limit)
    (implicit
      s0: Task :<: S
    ): Free[S, RDD[(Data, Long)]] =
    lift(Task.delay {
      sc.textFile(posixCodec.unsafePrintPath(f))
        .map(raw => DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι))
        .zipWithIndex()
        .filter {
          case (value, index) =>
            maybeLimit.fold(
              index >= offset.value
            ) (
              limit => index >= offset.value && index < limit.value + offset.value
            )
        }
    }).into[S]

  def fileExists[S[_]](f: AFile)(implicit s0: Task :<: S): Free[S, Boolean] =
    injectFT[Task, S].apply(Task.delay(new File(posixCodec.unsafePrintPath(f)).exists()))

  // TODO arbitrary value, more or less a good starting point
  // but we should consider some measuring
  def readChunkSize: Int = 5000

  def input[S[_]](implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Input[S] =
    Input[S](
      conf => createSparkContext(conf),
      (sc: SparkContext, f: AFile, off: Offset, lim: Limit) => rddFrom(sc, f, off, lim),
      f => fileExists(f),
      readChunkSize _
    )

}
