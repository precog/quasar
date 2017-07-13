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

package quasar.yggdrasil.jdbm3

import org.apache.jdbm.Serializer

import java.io.{DataInput, DataOutput}

import scala.annotation.tailrec

object ByteArraySerializer extends Serializer[Array[Byte]] with Serializable {

  @tailrec
  private def writePackedInt(out: DataOutput, n: Int): Unit =
    if ((n & ~0x7F) != 0) {
      out.writeByte(n & 0x7F | 0x80)
      writePackedInt(out, n >> 7)
    } else {
      out.writeByte(n & 0x7F)
    }

  private def readPackedInt(in: DataInput): Int = {
    @tailrec def loop(n: Int, offset: Int): Int = {
      val b = in.readByte()
      if ((b & 0x80) != 0) {
        loop(n | ((b & 0x7F) << offset), offset + 7)
      } else {
        n | ((b & 0x7F) << offset)
      }
    }
    loop(0, 0)
  }

  def serialize(out: DataOutput, bytes: Array[Byte]) {
    writePackedInt(out, bytes.length)
    out.write(bytes)
  }

  def deserialize(in: DataInput): Array[Byte] = {
    val length = readPackedInt(in)
    val bytes  = new Array[Byte](length)
    in.readFully(bytes)
    bytes
  }
}
