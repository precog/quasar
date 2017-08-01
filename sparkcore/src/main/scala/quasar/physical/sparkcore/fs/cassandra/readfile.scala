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
import quasar.Data
import quasar.contrib.pathy._
import quasar.physical.sparkcore.fs.readfile.{ Offset, Limit }
import quasar.physical.sparkcore.fs.readfile.Input

import org.apache.spark.rdd._
import scalaz._
import scalaz.concurrent.Task
import pathy.Path.fileParent

object readfile {

  import common._

  def rddFrom[S[_]](f: AFile, offset: Offset, maybeLimit: Limit)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, RDD[(Data, Long)]] =
    cass.readTable(keyspace(fileParent(f)), tableName(f)).map{ rdd =>
      rdd
        .zipWithIndex()
        .filter {
          case (value, index) =>
            maybeLimit.fold(
              index >= offset.value
            )(
              limit => index >= offset.value && index < limit.value + offset.value
            )
        }
    }

  def fileExists[S[_]](f: AFile)(implicit
    cass: CassandraDDL.Ops[S]
  ): Free[S, Boolean] =
    cass.tableExists(keyspace(fileParent(f)), tableName(f))

  // TODO arbitrary value, more or less a good starting point
  // but we should consider some measuring
  def readChunkSize: Int = 5000

  def input[S[_]](implicit cass: CassandraDDL.Ops[S], s0: Task :<: S) =
    Input(
      rddFrom(_, _, _),
      fileExists(_),
      readChunkSize _)

}
