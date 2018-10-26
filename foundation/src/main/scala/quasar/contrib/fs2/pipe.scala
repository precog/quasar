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

package quasar.contrib.fs2

import slamdata.Predef._

import java.nio.CharBuffer
import java.nio.charset.CodingErrorAction

import fs2.{Chunk, Pipe, Stream}

object pipe {

  def charBufferToByte[F[_]](cs: Charset): Pipe[F, CharBuffer, Byte] = { str =>
    Stream suspend {
      val encoder = cs.newEncoder
        .onMalformedInput(CodingErrorAction.REPLACE)
        .onUnmappableCharacter(CodingErrorAction.REPLACE)

      str.flatMap(cb => Stream.chunk(Chunk.ByteBuffer(encoder.encode(cb))))
    }
  }
}
