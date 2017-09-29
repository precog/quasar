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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp.free._
import quasar.fp.numeric.{Natural, Positive}
import quasar.fs._, FileSystemError._

import org.apache.spark.SparkContext
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile {

  type Offset = Natural
  type Limit = Option[Positive]

  import ReadFile.ReadHandle

  def interpret[S[_]](
      implicit
      s0: KeyValueStore[ReadHandle, SparkCursor, ?] :<: S,
      s1: Read[SparkContext, ?] :<: S,
      s2: MonotonicSeq :<: S,
      s3: Task :<: S,
      details: SparkConnectorDetails.Ops[S]): ReadFile ~> Free[S, ?] =
    new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case ReadFile.Open(f, offset, limit) => open[S](f, offset, limit)
        case ReadFile.Read(h) => read[S](h)
        case ReadFile.Close(h) => close[S](h)
      }
    }

  def open[S[_]](f: AFile, offset: Offset, limit: Limit)(
      implicit
      kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S],
      s1: Read[SparkContext, ?] :<: S,
      gen: MonotonicSeq.Ops[S],
      details: SparkConnectorDetails.Ops[S]): Free[S, FileSystemError \/ ReadHandle] = {

    def freshHandle: Free[S, ReadHandle] =
      gen.next map (ReadHandle(f, _))

    def _open: Free[S, ReadHandle] =
      for {
        rdd <- details.rddFrom(f)
        limitedRdd = rdd.zipWithIndex().filter {
          case (value, index) =>
            limit.fold(
              index >= offset.value
            )(
              limit => index >= offset.value && index < limit.value + offset.value
            )
        }
        cur = SparkCursor(limitedRdd.some, 0)
        h <- freshHandle
        _ <- kvs.put(h, cur)
      } yield h

    def _empty: Free[S, ReadHandle] =
      for {
        h <- freshHandle
        cur = SparkCursor(None, 0)
        _ <- kvs.put(h, cur)
      } yield h

    details
      .fileExists(f)
      .ifM(
        _open map (_.right[FileSystemError]),
        _empty map (_.right[FileSystemError])
      )
  }

  def read[S[_]](h: ReadHandle)(
      implicit
      kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S],
      s1: Task :<: S,
      details: SparkConnectorDetails.Ops[S]): Free[S, FileSystemError \/ Vector[Data]] =
    details.readChunkSize >>= (step =>
      kvs
        .get(h)
        .toRight(unknownReadHandle(h))
        .flatMap {
          case SparkCursor(None, _) =>
            Vector.empty[Data].pure[EitherT[Free[S, ?], FileSystemError, ?]]
          case SparkCursor(Some(rdd), p) =>
            val collect = lift(Task.delay {
              rdd.filter(d => d._2 >= p && d._2 < (p + step)).map(_._1).collect.toVector
            }).into[S].liftM[FileSystemErrT]

            for {
              collected <- collect
              cur = if (collected.isEmpty) SparkCursor(None, 0)
              else SparkCursor(some(rdd), p + step)
              _ <- kvs.put(h, cur).liftM[FileSystemErrT]
            } yield collected

        }
        .run)

  def close[S[_]](h: ReadHandle)(
      implicit
      kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S]): Free[S, Unit] = kvs.delete(h)
}
