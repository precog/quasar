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
import org.apache.spark.rdd._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final case class SparkCursor(rdd: Option[RDD[(Data, Long)]], pointer: Int)

object readfile {

  type Offset = Natural
  type Limit = Option[Positive]

  final case class Input[S[_]](
    rddFrom: (AFile, Offset, Limit)  => Free[S, RDD[(Data, Long)]],
    fileExists: AFile => Free[S, Boolean],
    readChunkSize: () => Int
  )

  import ReadFile.ReadHandle

  def chrooted[S[_]](input: Input[S], prefix: ADir)(implicit
    s0: KeyValueStore[ReadHandle, SparkCursor, ?] :<: S,
    s1: Read[SparkContext, ?] :<: S,
    s2: MonotonicSeq :<: S,
    s3: Task :<: S
  ): ReadFile ~> Free[S, ?] =
    flatMapSNT(interpret(input)) compose chroot.readFile[ReadFile](prefix)

  def interpret[S[_]](input: Input[S])(implicit
    s0: KeyValueStore[ReadHandle, SparkCursor, ?] :<: S,
    s1: Read[SparkContext, ?] :<: S,
    s2: MonotonicSeq :<: S,
    s3: Task :<: S
  ): ReadFile ~> Free[S, ?] =
    new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case ReadFile.Open(f, offset, limit) => open[S](f, offset, limit, input)
        case ReadFile.Read(h) => read[S](h, input.readChunkSize())
        case ReadFile.Close(h) => close[S](h)
      }
  }

  private def open[S[_]](f: AFile, offset: Offset, limit: Limit, input: Input[S])(implicit
    kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S],
    s1: Read[SparkContext, ?] :<: S,
    gen: MonotonicSeq.Ops[S]
  ): Free[S, FileSystemError \/ ReadHandle] = {

    def freshHandle: Free[S, ReadHandle] =
      gen.next map (ReadHandle(f, _))

    def _open: Free[S, ReadHandle] = for {
      rdd <- input.rddFrom(f, offset, limit)
      cur = SparkCursor(rdd.some, 0)
      h <- freshHandle
      _ <- kvs.put(h, cur)
    } yield h

    def _empty: Free[S, ReadHandle] = for {
      h <- freshHandle
      cur = SparkCursor(None, 0)
      _ <- kvs.put(h, cur)
    } yield h

    input.fileExists(f).ifM(
      _open map (_.right[FileSystemError]),
     _empty map (_.right[FileSystemError])
    )
  }

  private def read[S[_]](h: ReadHandle, step: Int)(implicit
    kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S],
    s1: Task :<: S
  ): Free[S, FileSystemError \/ Vector[Data]] = {


    kvs.get(h).toRight(unknownReadHandle(h)).flatMap {
        case SparkCursor(None, _) =>
          Vector.empty[Data].pure[EitherT[Free[S, ?], FileSystemError, ?]]
        case SparkCursor(Some(rdd), p) =>

        val collect = lift(Task.delay {
          rdd
            .filter(d => d._2 >= p && d._2 < (p + step))
            .map(_._1).collect.toVector
        }).into[S].liftM[FileSystemErrT]

        for {
          collected <- collect
          cur = if(collected.isEmpty) SparkCursor(None, 0) else SparkCursor(some(rdd), p + step)
          _ <- kvs.put(h, cur).liftM[FileSystemErrT]
        } yield collected
        
    }.run
  }

  private def close[S[_]](h: ReadHandle)(implicit
    kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S]
  ): Free[S, Unit] = kvs.delete(h)
}
