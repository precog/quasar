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

package quasar.contrib.fs2

import slamdata.Predef._

import quasar.byte

import java.nio.CharBuffer
import java.nio.charset.StandardCharsets

import fs2.{Chunk, Pipe, Stream, Pull}

object pipe {
  private val Utf8Bom = Chunk.bytes(byte.Utf8Bom)
  private val Utf8 = StandardCharsets.UTF_8

  /**
   * A variant of the charBufferToByte pipe specialized on UTF-8 to allow us to
   * inject the byte-order mark when relevant. You should use this instead of
   * charBufferToByte whenever converting specifically to UTF-8.
   */
  def charBufferToByteUtf8[F[_]](bom: Boolean): Pipe[F, CharBuffer, Byte] = { str =>
    val prefix = if (bom) Stream.chunk(Utf8Bom) else Stream.empty
    prefix ++ charBufferToByte(Utf8)(str)
  }

  def charBufferToByte[F[_]](cs: Charset): Pipe[F, CharBuffer, Byte] = { str =>
    Stream suspend {
      val encoder = byte.charsetEncoder(cs)

      str.flatMap(cb =>
        Stream.chunk(Chunk.ByteBuffer(encoder.encode(cb))))
    }
  }

  def limitSize[F[_]](
      target: Int,
      onTargetExceeded: Pull[F, Nothing, Nothing])
      : Pipe[F, Byte, Byte] = { bytes =>

    def loop(acc: Int, remaining: Stream[F, Byte]): Pull[F, Byte, Int] =
      remaining.pull.uncons flatMap {
        case Some((hd, tl)) =>
          val size = acc + hd.size
          if (size > target)
            onTargetExceeded
          else
            Pull.output(hd) >> loop(size, tl)
        case None =>
          Pull.pure(acc)
      }

    loop(0, bytes).void.stream
  }
}
