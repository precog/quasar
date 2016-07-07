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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar.Data
import quasar.DataCodec
import quasar.fp.ι
import quasar.fs._
import quasar.fs.PathError._
import quasar.fs.FileSystemError._
import quasar.effect._

import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import scalaz._
import Scalaz._


final case class SparkCursor(rdd: Option[RDD[Data]])

final case class Input[S[_]](
  rddFrom: AFile => Free[S, RDD[String]],
  fileExists: AFile => Free[S, Boolean]
)

object readfile {

  import ReadFile.ReadHandle

  def interpret[S[_]](input: Input[S])(implicit
    s0: KeyValueStore[ReadHandle, SparkCursor, ?] :<: S,
    s1: Read[SparkContext, ?] :<: S,
    s2: MonotonicSeq :<: S
  ): ReadFile ~> Free[S, ?] =
    new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case ReadFile.Open(f, _, _) => open[S](f, input)
        case ReadFile.Read(h) => read[S](h)
        case ReadFile.Close(h) => close[S](h)
      }
  }

  private def open[S[_]](f: AFile, input: Input[S])(implicit
    kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S],
    s1: Read[SparkContext, ?] :<: S,
    gen: MonotonicSeq.Ops[S]
  ): Free[S, FileSystemError \/ ReadHandle] = {

    def freshHandle: Free[S, ReadHandle] =
      gen.next map (ReadHandle(f, _))

    def _open: Free[S, ReadHandle] = for {
      rdd <- input.rddFrom(f)
      cur = SparkCursor(rdd.map{ raw =>
        DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι)
      }.some)
      h <- freshHandle
      _ <- kvs.put(h, cur)
    } yield h

    input.fileExists(f).ifM(
      _open map (_.right[FileSystemError]),
      pathErr(pathNotFound(f)).left.point[Free[S, ?]]
    )
  }

  private def read[S[_]](h: ReadHandle)(implicit
    kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S]
  ): Free[S, FileSystemError \/ Vector[Data]] = {

    kvs.get(h).toRight(unknownReadHandle(h)).flatMap {
        case SparkCursor(None) =>
          Vector.empty[Data].pure[EitherT[Free[S, ?], FileSystemError, ?]]
        case SparkCursor(Some(rdd)) =>

          val collect = rdd.collect.toVector.pure[EitherT[Free[S, ?], FileSystemError, ?]]
          val clear = kvs.put(h, SparkCursor(None)).liftM[FileSystemErrT]

          collect <* clear
    }.run
  }

  private def close[S[_]](h: ReadHandle)(implicit
    kvs: KeyValueStore.Ops[ReadHandle, SparkCursor, S]
  ): Free[S, Unit] = kvs.delete(h)
}
