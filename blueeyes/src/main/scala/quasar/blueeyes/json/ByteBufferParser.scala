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

package quasar.blueeyes.json

import quasar.blueeyes._
import java.nio.ByteBuffer

/**
  * Basic ByteBuffer parser.
  */
private[json] final class ByteBufferParser(src: ByteBuffer) extends SyncParser with ByteBasedParser {
  final val start = src.position
  final val limit = src.limit - start

  var line = 0
  protected[this] final def newline(i: Int) { line += 1 }
  protected[this] final def column(i: Int) = i

  final def close() { src.position(src.limit) }
  final def reset(i: Int): Int = i
  final def checkpoint(state: Int, i: Int, stack: List[Context]) {}
  final def byte(i: Int): Byte = src.get(i + start)
  final def at(i: Int): Char   = src.get(i + start).toChar

  final def at(i: Int, k: Int): String = {
    val len = k - i
    val arr = new Array[Byte](len)
    src.position(i + start)
    src.get(arr, 0, len)
    src.position(start)
    new String(arr, Utf8Charset)
  }

  final def atEof(i: Int) = i >= limit
}
