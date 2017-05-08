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

import scala.math.max

import java.nio.ByteBuffer

case class AsyncParse(errors: Seq[ParseException], values: Seq[JValue])

/**
  * This class is used internally by AsyncParser to signal that we've reached
  * the end of the particular input we were given.
  */
private[json] class AsyncException extends Exception

private[json] class FailureException extends Exception

object AsyncParser {
  sealed trait Input
  case class More(buf: ByteBuffer) extends Input
  case object Done extends Input

  @deprecated("Use AsyncParser.stream() to maintain the current behavior", "1.0")
  def apply(): AsyncParser = stream()

  /**
    * Asynchronous parser for a stream of (whitespace-delimited) JSON values.
    */
  def stream(): AsyncParser =
    new AsyncParser(state = -1, curr = 0, stack = Nil, data = new Array[Byte](131072), len = 0, allocated = 131072, offset = 0, done = false, streamMode = 0)

  /**
    * Asynchronous parser for a single JSON value.
    */
  def json(): AsyncParser =
    new AsyncParser(state = -1, curr = 0, stack = Nil, data = new Array[Byte](131072), len = 0, allocated = 131072, offset = 0, done = false, streamMode = -1)

  /**
    * Asynchronous parser which can unwrap a single JSON array into a stream of
    * values (or return a single value otherwise).
    */
  def unwrap(): AsyncParser =
    new AsyncParser(state = -5, curr = 0, stack = Nil, data = new Array[Byte](131072), len = 0, allocated = 131072, offset = 0, done = false, streamMode = 1)
}

/*
 * AsyncParser is able to parse chunks of data (encoded as
 * Option[ByteBuffer] instances) and parse asynchronously.  You can
 * use the factory methods in the companion object to instantiate an
 * async parser.
 *
 * The async parser's fields are described below:
 *
 * The (state, curr, stack) triple is used to save and restore parser
 * state between async calls. State also helps encode extra
 * information when streaming or unwrapping an array.
 *
 * The (data, len, allocated) triple is used to manage the underlying
 * data the parser is keeping track of. As new data comes in, data may
 * be expanded if not enough space is available.
 *
 * The offset parameter is used to drive the outer async parsing. It
 * stores similar information to curr but is kept separate to avoid
 * "corrupting" our snapshot.
 *
 * The done parameter is used internally to help figure out when the
 * atEof() parser method should return true. This will be set when
 * apply(None) is called.
 *
 * The streamMode parameter controls how the asynchronous parser will
 * be handling multiple values. There are three states:
 *
 *    1: An array is being unwrapped. Normal JSON array rules apply
 *       (Note that if the outer value observed is not an array, this
 *       mode will toggle to the -1 mode).
 *
 *    0: A JSON stream is being parsed. JSON values will be separated
 *       by optional whitespace
 *
 *   -1: No streaming is occuring. Only a single JSON value is
 *       allowed.
 */
final class AsyncParser protected[json] (
    protected[json] var state: Int,
    protected[json] var curr: Int,
    protected[json] var stack: List[Context],
    protected[json] var data: Array[Byte],
    protected[json] var len: Int,
    protected[json] var allocated: Int,
    protected[json] var offset: Int,
    protected[json] var done: Boolean,
    protected[json] var streamMode: Int
) extends ByteBasedParser {
  import AsyncParser._

  protected[this] var line = 0
  protected[this] var pos  = 0
  protected[this] final def newline(i: Int) { line += 1; pos = i + 1 }
  protected[this] final def column(i: Int) = i - pos

  protected[this] final def copy() =
    new AsyncParser(state, curr, stack, data.clone, len, allocated, offset, done, streamMode)

  final def apply(input: Input): (AsyncParse, AsyncParser) = copy.feed(input)

  protected[this] final def absorb(buf: ByteBuffer): Unit = {
    done = false
    val buflen = buf.limit - buf.position
    val need   = len + buflen

    // if we don't have enough free space available we'll need to grow our
    // data array. we never shrink the data array, assuming users will call
    // feed with similarly-sized buffers.
    if (need > allocated) {
      val doubled = if (allocated < 0x40000000) allocated * 2 else Int.MaxValue
      val newsize = max(need, doubled)
      val newdata = new Array[Byte](newsize)
      System.arraycopy(data, 0, newdata, 0, len)
      data = newdata
      allocated = newsize
    }

    buf.get(data, len, buflen)
    len = need
  }

  // Explanation of the new synthetic states .The parser machinery
  // uses positive integers for states while parsing json values. We
  // use these negative states to keep track of the async parser's
  // status between json values.
  //
  // ASYNC_PRESTART: We haven't seen any non-whitespace yet. We
  // could be parsing an array, or not. We are waiting for valid
  // JSON.
  //
  // ASYNC_START: We've seen an array and have begun unwrapping
  // it. We could see a ] if the array is empty, or valid JSON.
  //
  // ASYNC_END: We've parsed an array and seen the final ]. At this
  // point we should only see whitespace or an EOF.
  //
  // ASYNC_POSTVAL: We just parsed a value from inside the array. We
  // expect to see whitespace, a comma, or an EOF.
  //
  // ASYNC_PREVAL: We are in an array and we just saw a comma. We
  // expect to see whitespace or a JSON value.
  @inline private[this] final def ASYNC_PRESTART = -5
  @inline private[this] final def ASYNC_START    = -4
  @inline private[this] final def ASYNC_END      = -3
  @inline private[this] final def ASYNC_POSTVAL  = -2
  @inline private[this] final def ASYNC_PREVAL   = -1

  protected[json] def feed(b: Input): (AsyncParse, AsyncParser) = {
    b match {
      case Done      => done = true
      case More(buf) => absorb(buf)
    }

    // accumulates errors and results
    val errors  = ArrayBuffer.empty[ParseException]
    val results = ArrayBuffer.empty[JValue]

    // we rely on exceptions to tell us when we run out of data
    try {
      while (true) {
        if (state < 0) {
          (at(offset): @switch) match {
            case '\n' =>
              newline(offset)
              offset += 1

            case ' ' | '\t' | '\r' =>
              offset += 1

            case '[' =>
              if (state == ASYNC_PRESTART) {
                offset += 1
                state = ASYNC_START
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else if (state == ASYNC_POSTVAL) {
                die(offset, "expected , or ]")
              } else {
                state = 0
              }

            case ',' =>
              if (state == ASYNC_POSTVAL) {
                offset += 1
                state = ASYNC_PREVAL
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else {
                die(offset, "expected json value")
              }

            case ']' =>
              if (state == ASYNC_POSTVAL || state == ASYNC_START) {
                if (streamMode > 0) {
                  offset += 1
                  state = ASYNC_END
                } else {
                  die(offset, "expected json value or eof")
                }
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else {
                die(offset, "expected json value")
              }

            case c =>
              if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else if (state == ASYNC_POSTVAL) {
                die(offset, "expected ] or ,")
              } else {
                if (state == ASYNC_PRESTART && streamMode > 0) streamMode = -1
                state = 0
              }
          }

        } else {
          // jump straight back into rparse
          offset = reset(offset)
          val (value, j) = if (state <= 0) {
            parse(offset)
          } else {
            rparse(state, curr, stack)
          }
          if (streamMode > 0) {
            state = ASYNC_POSTVAL
          } else if (streamMode == 0) {
            state = ASYNC_PREVAL
          } else {
            state = ASYNC_END
          }
          curr = j
          offset = j
          stack = Nil
          results.append(value)
        }
      }
    } catch {
      case e: AsyncException =>
      // we ran out of data, so return what we have so far

      case e: ParseException =>
        // we hit a parser error, so return that error and results so far
        errors.append(e)
    }
    (AsyncParse(errors, results), this)
  }

  // every 1M we shift our array back by 1M.
  protected[this] final def reset(i: Int): Int = {
    if (offset >= 1048576) {
      len -= 1048576
      offset -= 1048576
      pos -= 1048576
      System.arraycopy(data, 1048576, data, 0, len)
      i - 1048576
    } else {
      i
    }
  }

  /**
    * We use this to keep track of the last recoverable place we've
    * seen. If we hit an AsyncException, we can later resume from this
    * point.
    *
    * This method is called during every loop of rparse, and the
    * arguments are the exact arguments we can pass to rparse to
    * continue where we left off.
    */
  protected[this] final def checkpoint(state: Int, i: Int, stack: List[Context]) {
    this.state = state
    this.curr = i
    this.stack = stack
  }

  /**
    * This is a specialized accessor for the case where our underlying data are
    * bytes not chars.
    */
  protected[this] final def byte(i: Int): Byte =
    if (i >= len)
      throw new AsyncException
    else
      data(i)

  // we need to signal if we got out-of-bounds
  protected[this] final def at(i: Int): Char =
    if (i >= len)
      throw new AsyncException
    else
      data(i).toChar

  /**
    * Access a byte range as a string.
    *
    * Since the underlying data are UTF-8 encoded, i and k must occur on unicode
    * boundaries. Also, the resulting String is not guaranteed to have length
    * (k - i).
    */
  protected[this] final def at(i: Int, k: Int): String = {
    if (k > len) throw new AsyncException
    val size = k - i
    val arr  = new Array[Byte](size)
    System.arraycopy(data, i, arr, 0, size)
    new String(arr, Utf8Charset)
  }

  // the basic idea is that we don't signal EOF until done is true, which means
  // the client explicitly send us an EOF.
  protected[this] final def atEof(i: Int) = if (done) i >= len else false

  // we don't have to do anything special on close.
  protected[this] final def close() = ()
}
