/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.fs

import slamdata.Predef._
import quasar.Data

import scalaz._, Scalaz._
import scalaz.stream.Process

/** Typeclass representing the interface to a effectful cursor of `Data`.
  *
  * Laws
  *   1. close(c) *> nextChunk(c) must return an empty `Vector`.
  */
trait DataCursor[F[_], C] {
  /** Returns the next chunk of data from the cursor. An empty `Vector` signals
    * no more data is available.
    */
  def nextChunk(cursor: C): F[Vector[Data]]

  /** Closes the cursor, freeing any resources it might be using. */
  def close(cursor: C): F[Unit]

  def process(cursor: C)(implicit F: Applicative[F]): Process[F, Data] = {
    def closeCursor(c: C): Process[F, Nothing] =
      Process.eval_[F, Unit](close(c))
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def readUntilEmpty(c: C): Process[F, Data] =
      Process.await(nextChunk(c)) { data =>
        if (data.isEmpty)
          Process.halt
        else
          Process.emitAll(data) ++ readUntilEmpty(c)
      }

    Process.bracket(cursor.point[F])(closeCursor)(readUntilEmpty)
  }
}

object DataCursor {
  def apply[F[_], C](implicit DC: DataCursor[F, C]): DataCursor[F, C] = DC

  implicit def eitherDataCursor[F[_], A, B](
    implicit A: DataCursor[F, A], B: DataCursor[F, B]
  ): DataCursor[F, A \/ B] =
    new DataCursor[F, A \/ B] {
      def nextChunk(ab: A \/ B) =
        ab fold (A.nextChunk, B.nextChunk)

      def close(ab: A \/ B) =
        ab fold (A.close, B.close)
    }

  implicit def optionDataCursor[F[_]: Applicative, A](implicit C: DataCursor[F, A]): DataCursor[F, Option[A]] =
    new DataCursor[F, Option[A]] {
      def nextChunk(oa: Option[A]) =
        oa.cata(C.nextChunk, Vector[Data]().point[F])

      def close(oa: Option[A]) =
        oa.traverse_(C.close)
    }
}
