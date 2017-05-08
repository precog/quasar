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

package quasar.niflheim

import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, WritableByteChannel}
import java.util.zip.Adler32

import scalaz._

/**
 * This class provides some nice method for writing/reading bytes to channels.
 * It does this by writing the data out in chunks. These chunks are fixed and
 * can use a checksum to ensure our data isn't corrupted.
 */
trait Chunker {
  def verify: Boolean
  val ChunkSize = 4096

  private def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size)

  private def takeChunk(buffer: ByteBuffer): ByteBuffer = {
    val bytes = new Array[Byte](ChunkSize)
    val remaining = buffer.remaining()
    val len = math.min(ChunkSize - 12, remaining)
    buffer.get(bytes, 4, len)

    val checksum = new Adler32()
    checksum.update(bytes, 4, ChunkSize - 12)

    val chunk = ByteBuffer.wrap(bytes)
    chunk.putInt(0, remaining)
    chunk.putLong(ChunkSize - 8, checksum.getValue())
    chunk
  }

  private def readChunk(chunk: ByteBuffer, buffer: ByteBuffer): Int = {
    chunk.mark()
    val remaining = chunk.getInt()
    val bytes = new Array[Byte](ChunkSize - 12)
    val len = math.min(remaining, bytes.length)
    chunk.get(bytes, 0, len)
    chunk.reset()

    if (verify) {
      val checksum = new Adler32()
      checksum.update(bytes)
      val sum0 = checksum.getValue()
      val sum1 = chunk.getLong(ChunkSize - 8)
      if (sum0 != sum1)
        throw new IOException("Corrupted chunk.")
    }

    buffer.put(bytes, 0, len)
    remaining - len
  }

  def write[A](channel: WritableByteChannel, maxSize: Int)(f: ByteBuffer => A): Validation[IOException, A] = {
    val buffer = allocate(maxSize)
    val result = f(buffer)
    buffer.flip()

    try {
      while (buffer.remaining() > 0) {
        val chunk = takeChunk(buffer)
        while (chunk.remaining() > 0) {
          channel.write(chunk)
        }
      }
      Success(result)
    } catch { case ex: IOException =>
      Failure(ex)
    }
  }

  def read(channel: ReadableByteChannel): Validation[IOException, ByteBuffer] = {
    try {
      val chunk = allocate(ChunkSize)
      while (chunk.remaining() > 0) {
        channel.read(chunk)
      }
      chunk.flip()

      val length = chunk.getInt(0)
      val buffer = allocate(length)
      while (readChunk(chunk, buffer) > 0) {
        chunk.clear()
        while (chunk.remaining() > 0) {
          channel.read(chunk)
        }
        chunk.flip()
      }
      buffer.flip()

      Success(buffer)
    } catch { case ioe: IOException =>
      Failure(ioe)
    }
  }
}
