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

package quasar.blueeyes.json

import quasar.blueeyes._

import java.io.{File, FileInputStream}
import java.nio.channels.ReadableByteChannel

object ChannelParser {
  private[json] def fromFile(f: File) =
    new ChannelParser(new FileInputStream(f).getChannel)
}

/**
  * Basic file parser.
  *
  * Given a file name this parser opens it, chunks the data 1M at a time, and
  * parses it.
  */
private[json] final class ChannelParser(ch: ReadableByteChannel) extends SyncParser with ByteBasedParser {

  // 256K buffers: arrived at via a bit of testing
  @inline final def bufsize = 262144
  @inline final def mask    = bufsize - 1

  // these are the actual byte arrays we'll use
  private var curr = new Array[Byte](bufsize)
  private var next = new Array[Byte](bufsize)

  // these are the bytebuffers used to load the data
  private var bcurr = ByteBufferWrap(curr)
  private var bnext = ByteBufferWrap(next)

  // these are the bytecounts for each array
  private var ncurr = ch.read(bcurr)
  private var nnext = ch.read(bnext)

  var line = 0
  private var pos  = 0
  protected[this] final def newline(i: Int) { line += 1; pos = i }
  protected[this] final def column(i: Int) = i - pos

  final def close() = ch.close()

  /**
    * Swap the curr and next arrays/buffers/counts.
    *
    * We'll call this in response to certain reset() calls. Specifically, when
    * the index provided to reset is no longer in the 'curr' buffer, we want to
    * clear that data and swap the buffers.
    */
  final def swap() {
    val tmp  = curr; curr = next; next = tmp
    val btmp = bcurr; bcurr = bnext; bnext = btmp
    val ntmp = ncurr; ncurr = nnext; nnext = ntmp
  }

  /**
    * If the cursor 'i' is past the 'curr' buffer, we want to clear the current
    * byte buffer, do a swap, load some more data, and continue.
    */
  final def reset(i: Int): Int = {
    if (i >= bufsize) {
      bcurr.clear()
      swap()
      nnext = ch.read(bnext)
      pos -= bufsize
      i - bufsize
    } else {
      i
    }
  }

  final def checkpoint(state: Int, i: Int, stack: List[Context]) {}

  /**
    * This is a specialized accessor for the case where our underlying data are
    * bytes not chars.
    */
  final def byte(i: Int): Byte =
    if (i < bufsize)
      curr(i)
    else
      next(i & mask)

  /**
    * Rads
    */
  final def at(i: Int): Char =
    if (i < bufsize)
      curr(i).toChar
    else
      next(i & mask).toChar

  /**
    * Access a byte range as a string.
    *
    * Since the underlying data are UTF-8 encoded, i and k must occur on unicode
    * boundaries. Also, the resulting String is not guaranteed to have length
    * (k - i).
    */
  final def at(i: Int, k: Int): String = {
    val len = k - i

    if (k <= bufsize) {
      new String(curr, i, len, Utf8Charset)
    } else {
      val arr = new Array[Byte](len)
      val mid = bufsize - i
      System.arraycopy(curr, i, arr, 0, mid)
      System.arraycopy(next, 0, arr, mid, k - bufsize)
      new String(arr, Utf8Charset)
    }
  }

  final def atEof(i: Int) = if (i < bufsize) i >= ncurr else (i - bufsize) >= nnext
}
