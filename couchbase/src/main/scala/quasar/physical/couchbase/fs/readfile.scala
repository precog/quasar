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

package quasar.physical.couchbase.fs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.Data
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.fp.free._
import quasar.fs._
import quasar.fs.impl.ReadOpts
import quasar.physical.couchbase.common._

import scala.collection.JavaConverters._

import eu.timepit.refined.api.RefType.ops._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile {

  def interpret[S[_]](
    implicit
    S0: KeyValueStore[ReadFile.ReadHandle, Cursor, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Read[Context, ?] :<:  S,
    S3: Task :<: S
  ): ReadFile ~> Free[S, ?] =
    impl.read[S, Cursor](open, read, close)

  // TODO: Streaming
  def open[S[_]](
    file: AFile, readOpts: ReadOpts
  )(implicit
    S1: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Cursor] =
    (for {
      ctx     <- context.ask.liftM[FileSystemErrT]
      bktCol  <- EitherT(bucketCollectionFromPath(file).point[Free[S, ?]])
      bkt     <- EitherT(getBucket(bktCol.bucket))
      limit   =  readOpts.limit.map(lim => s"LIMIT ${lim.unwrap}").orZero
      qStr    =  s"""SELECT ifmissing(d.`value`, d).* FROM `${bktCol.bucket}` d
                     WHERE type="${bktCol.collection}"
                     $limit OFFSET ${readOpts.offset.unwrap.shows}"""
      qResult <- EitherT(lift(Task.delay(
                   bkt.query(n1qlQuery(qStr))
                     .allRows
                     .asScala
                     .toVector
                     .traverse(rowToData)
                 )).into)
    } yield Cursor(qResult)).run

  def read[S[_]](
    cursor: Cursor
  )(implicit
    S0: Task :<: S
  ): Free[S, FileSystemError \/ (Cursor, Vector[Data])] =
    resultsFromCursor(cursor).right.point[Free[S, ?]]

  def close[S[_]](
    cursor: Cursor
  )(implicit
    S0: Task :<: S
  ): Free[S, Unit] =
    ().point[Free[S, ?]]

}
