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

package quasar

import quasar.common.data.Data
import quasar.contrib.fs2.convert
import quasar.precog.common.{CUndefined, RValue}
import quasar.yggdrasil.table.Slice

import cats.effect.IO
import fs2.{Chunk, Stream}
import scalaz.{Functor, StreamT}
import shims._

package object mimir {

  private def slicesToStream[F[_]: Functor](slices: StreamT[F, Slice]): Stream[F, RValue] =
    convert.fromChunkedStreamT(slices.map(s => Chunk.indexedSeq(SliceIndexedSeq(s))))

  // used in integration tests and the REPL
  def tableToData(repr: MimirRepr): Stream[IO, Data] =
    slicesToStream(repr.table.slices)
      // TODO{fs2}: Chunkiness
      .mapSegments(s =>
        s.filter(_ != CUndefined).map(RValue.toData).force.toChunk.toSegment)
}
