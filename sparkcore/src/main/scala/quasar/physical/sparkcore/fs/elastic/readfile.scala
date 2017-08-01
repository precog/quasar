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
import quasar.Data
import quasar.contrib.pathy._
import quasar.effect.Read
import quasar.fp.free._
import quasar.fp.ski.ι
import quasar.physical.sparkcore.fs.readfile.Input
import quasar.physical.sparkcore.fs.readfile.{Offset, Limit}

import org.apache.spark._
import org.apache.spark.rdd._
import scalaz._, concurrent.Task

object readfile {

  def rddFrom[S[_]](f: AFile, offset: Offset, maybeLimit: Limit)(implicit
    read: Read.Ops[SparkContext, S],
    E: ElasticCall :<: S,
    S: Task :<: S
  ): Free[S, (RDD[(Data, Long)])] = {
    for {
      sc <- read.asks(ι)
      rdd <- lift(queryfile.fromFile(sc, f)).into[S]
    } yield {
      rdd
        .zipWithIndex()
        .filter {
        case (value, index) =>
          maybeLimit.fold(
            index >= offset.value
          ) (
            limit => index >= offset.value && index < limit.value + offset.value
          )
      }
    }
  }

  def input[S[_]](implicit
    read: Read.Ops[SparkContext, S],
    E: ElasticCall :<: S,
    S: Task :<: S
  ): Input[S] = Input(rddFrom[S] _, queryfile.fileExists[S] _, queryfile.readChunkSize _)

}
