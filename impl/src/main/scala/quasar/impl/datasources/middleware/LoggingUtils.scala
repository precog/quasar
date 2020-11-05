/*
 * Copyright 2020 Precog Data
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

package quasar.impl
package datasources.middleware

import scala._, Predef._

import cats.effect.Sync

import fs2.{Chunk, Stream}

object LoggingUtils {
  def logFirstN[F[_]: Sync](s: Stream[F, Byte], max: Int, log: String => F[Unit])
      : Stream[F, Byte] = {
    def logChunk(bytes: Chunk[Byte]) =
      log(new String(bytes.toArray, "utf-8"))

    s.chunks.zipWithIndex flatMap {
      case (bytes, i) if i < max =>
        Stream.eval_(logChunk(bytes)) ++ Stream.chunk(bytes)
      case (bytes, _) =>
        Stream.chunk(bytes)
    }
  }

  def logFirstNDrain[F[_]: Sync](s: Stream[F, Byte], max: Int, log: String => F[Unit])
      : F[Unit] =
    logFirstN[F](s.take(max.toLong), max, log).compile.drain
}
