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

import scala.annotation.{switch, tailrec}

import java.lang.Integer.parseInt

// underlying parser code adapted from jawn under MIT license.
// (https://github.com/non/jawn)

case class ParseException(msg: String, index: Int, line: Int, col: Int) extends Exception(msg)
case class IncompleteParseException(msg: String)                        extends Exception(msg)

/**
  * Parser contains the state machine that does all the work. The only
  */
private[json] trait Parser {
  /**
    * Read the byte/char at 'i' as a Char.
    *
    * Note that this should not be used on potential multi-byte sequences.
    */
  protected[this] def at(i: Int): Char

  /**
    * Read the bytes/chars from 'i' until 'j' as a String.
    */
  protected[this] def at(i: Int, j: Int): String

  /**
    * Return true iff 'i' is at or beyond the end of the input (EOF).
    */
  protected[this] def atEof(i: Int): Boolean

  /**
    * Return true iff the byte/char at 'i' is equal to 'c'.
    */
  protected[this] final def is(i: Int, c: Char): Boolean = at(i) == c

  /**
    * Return true iff the bytes/chars from 'i' until 'j' are equal to 'str'.
    */
  protected[this] final def is(i: Int, j: Int, str: String): Boolean = at(i, j) == str

  /**
    * The reset() method is used to signal that we're working from the given
    * position, and any previous data can be released. Some parsers (e.g.
    * StringParser) will ignore release, while others (e.g. PathParser) will
    * need to use this information to release and allocate different areas.
    */
  protected[this] def reset(i: Int): Int

  /**
    * The checkpoint() method is used to allow some parsers to store their
    * progress.
    */
  protected[this] def checkpoint(state: Int, i: Int, stack: List[Context]): Unit

  /**
    * Should be called when parsing is finished.
    */
  protected[this] def close(): Unit

  /**
    * Valid parser states.
    */
  @inline protected[this] final val ARRBEG = 6
  @inline protected[this] final val OBJBEG = 7
  @inline protected[this] final val DATA   = 1
  @inline protected[this] final val KEY    = 2
  @inline protected[this] final val SEP    = 3
  @inline protected[this] final val ARREND = 4
  @inline protected[this] final val OBJEND = 5

  protected[this] def newline(i: Int): Unit
  protected[this] def line(): Int
  protected[this] def column(i: Int): Int

  /**
    * Used to generate error messages with character info and byte addresses.
    */
  protected[this] def die(i: Int, msg: String) = {
    val y = line() + 1
    val x = column(i) + 1
    val s = "%s got %s (line %d, column %d)" format (msg, at(i), y, x)
    throw ParseException(s, i, y, x)
  }

  /**
    * Used to generate messages for internal errors.
    */
  protected[this] def error(msg: String) =
    sys.error(msg)

  /**
    * Parse the given number, and add it to the given context.
    *
    * We don't actually instantiate a number here, but rather save the string
    * for future use. This ends up being way faster and has the nice side-effect
    * that we know exactly how the user represented the number.
    *
    * It would probably be possible to keep track of the whether the number is
    * expected to be whole, decimal, etc. but we don't do that at the moment.
    */
  protected[this] final def parseNum(i: Int, ctxt: Context): Int = {
    var j = i
    var c = at(j)

    if (c == '-') {
      j += 1
      c = at(j)
    }
    while ('0' <= c && c <= '9') { j += 1; c = at(j) }

    if (c == '.') {
      j += 1
      c = at(j)
      while ('0' <= c && c <= '9') { j += 1; c = at(j) }
    }

    if (c == 'e' || c == 'E') {
      j += 1
      c = at(j)
      if (c == '+' || c == '-') {
        j += 1
        c = at(j)
      }
      while ('0' <= c && c <= '9') { j += 1; c = at(j) }
    }

    ctxt.add(JNum(at(i, j)))
    j
  }

  /**
    * This number parser is a bit slower because it has to be sure it doesn't
    * run off the end of the input. Normally (when operating in rparse in the
    * context of an outer array or objedct) we don't have to worry about this
    * and can just grab characters, because if we run out of characters that
    * would indicate bad input.
    *
    * This method has all the same caveats as the previous method.
    */
  protected[this] final def parseNumSlow(i: Int, ctxt: Context): Int = {
    var j = i
    var c = at(j)

    if (c == '-') {
      // any valid input will require at least one digit after -
      j += 1
      c = at(j)
    }
    while ('0' <= c && c <= '9') {
      j += 1
      if (atEof(j)) {
        ctxt.add(JNum(at(i, j)))
        return j
      }
      c = at(j)
    }

    if (c == '.') {
      // any valid input will require at least one digit after .
      j += 1
      c = at(j)
      while ('0' <= c && c <= '9') {
        j += 1
        if (atEof(j)) {
          ctxt.add(JNum(at(i, j)))
          return j
        }
        c = at(j)
      }
    }

    if (c == 'e' || c == 'E') {
      // any valid input will require at least one digit after e, e+, etc
      j += 1
      c = at(j)
      if (c == '+' || c == '-') {
        j += 1
        c = at(j)
      }
      while ('0' <= c && c <= '9') {
        j += 1
        if (atEof(j)) {
          ctxt.add(JNum(at(i, j)))
          return j
        }
        c = at(j)
      }
    }
    ctxt.add(JNum(at(i, j)))
    j
  }

  /**
    * Generate a Char from the hex digits of "\u1234" (i.e. "1234").
    *
    * NOTE: This is only capable of generating characters from the basic plane.
    * This is why it can only return Char instead of Int.
    */
  protected[this] final def descape(s: String) = parseInt(s, 16).toChar

  /**
    * Parse the JSON string starting at 'i' and save it into 'ctxt'.
    */
  protected[this] def parseString(i: Int, ctxt: Context): Int

  /**
    * Parse the JSON constant "true".
    */
  protected[this] final def parseTrue(i: Int) =
    if (is(i, i + 4, "true")) JTrue else die(i, "expected true")

  /**
    * Parse the JSON constant "false".
    */
  protected[this] final def parseFalse(i: Int) =
    if (is(i, i + 5, "false")) JFalse else die(i, "expected false")

  /**
    * Parse the JSON constant "null".
    */
  protected[this] final def parseNull(i: Int) =
    if (is(i, i + 4, "null")) JNull else die(i, "expected null")

  /**
    * Parse and return the "next" JSON value as well as the position beyond it.
    * This method is used by both parse() as well as parseMany().
    */
  protected[this] final def parse(i: Int): (JValue, Int) =
    try {
      (at(i): @switch) match {
        // ignore whitespace
        case ' '  => parse(i + 1)
        case '\t' => parse(i + 1)
        case '\r' => parse(i + 1)
        case '\n' => newline(i); parse(i + 1)

        // if we have a recursive top-level structure, we'll delegate the parsing
        // duties to our good friend rparse().
        case '[' => rparse(ARRBEG, i + 1, new ArrContext :: Nil)
        case '{' => rparse(OBJBEG, i + 1, new ObjContext :: Nil)

        // we have a single top-level number
        case '-' | '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
          val ctxt = new SingleContext
          val j    = parseNumSlow(i, ctxt)
          (ctxt.value, j)

        // we have a single top-level string
        case '"' =>
          val ctxt = new SingleContext
          val j    = parseString(i, ctxt)
          (ctxt.value, j)

        // we have a single top-level constant
        case 't' => (parseTrue(i), i + 4)
        case 'f' => (parseFalse(i), i + 5)
        case 'n' => (parseNull(i), i + 4)

        // invalid
        case _ => die(i, "expected json value")
      }
    } catch {
      case _: IndexOutOfBoundsException =>
        throw IncompleteParseException("exhausted input")
    }

  /**
    * Tail-recursive parsing method to do the bulk of JSON parsing.
    *
    * This single method manages parser states, data, etc. Except for parsing
    * non-recursive values (like strings, numbers, and constants) all important
    * work happens in this loop (or in methods it calls, like reset()).
    *
    * Currently the code is optimized to make use of switch statements. Future
    * work should consider whether this is better or worse than manually
    * constructed if/else statements or something else.
    */
  @tailrec
  protected[this] final def rparse(state: Int, j: Int, stack: List[Context]): (JValue, Int) = {
    val i = reset(j)
    checkpoint(state, i, stack)
    (state: @switch) match {
      // we are inside an object or array expecting to see data
      case DATA =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case '[' => rparse(ARRBEG, i + 1, new ArrContext :: stack)
          case '{' => rparse(OBJBEG, i + 1, new ObjContext :: stack)

          case '-' | '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
            val ctxt = stack.head
            val j    = parseNum(i, ctxt)
            rparse(if (ctxt.isObj) OBJEND else ARREND, j, stack)

          case '"' =>
            val ctxt = stack.head
            val j    = parseString(i, ctxt)
            rparse(if (ctxt.isObj) OBJEND else ARREND, j, stack)

          case 't' =>
            val ctxt = stack.head
            ctxt.add(parseTrue(i))
            rparse(if (ctxt.isObj) OBJEND else ARREND, i + 4, stack)

          case 'f' =>
            val ctxt = stack.head
            ctxt.add(parseFalse(i))
            rparse(if (ctxt.isObj) OBJEND else ARREND, i + 5, stack)

          case 'n' =>
            val ctxt = stack.head
            ctxt.add(parseNull(i))
            rparse(if (ctxt.isObj) OBJEND else ARREND, i + 4, stack)

          case _ =>
            die(i, "expected json value")
        }

      // we are in an object expecting to see a key
      case KEY =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case '"' =>
            val j = parseString(i, stack.head)
            rparse(SEP, j, stack)

          case _ => die(i, "expected \"")
        }

      // we are starting an array, expecting to see data or a closing bracket
      case ARRBEG =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case ']' =>
            stack match {
              case ctxt1 :: Nil =>
                (ctxt1.finish, i + 1)
              case ctxt1 :: ctxt2 :: tail =>
                ctxt2.add(ctxt1.finish)
                rparse(if (ctxt2.isObj) OBJEND else ARREND, i + 1, ctxt2 :: tail)
              case _ =>
                error("invalid stack")
            }

          case _ => rparse(DATA, i, stack)
        }

      // we are starting an object, expecting to see a key or a closing brace
      case OBJBEG =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case '}' =>
            stack match {
              case ctxt1 :: Nil =>
                (ctxt1.finish, i + 1)
              case ctxt1 :: ctxt2 :: tail =>
                ctxt2.add(ctxt1.finish)
                rparse(if (ctxt2.isObj) OBJEND else ARREND, i + 1, ctxt2 :: tail)
              case _ =>
                error("invalid stack")
            }

          case _ => rparse(KEY, i, stack)
        }

      // we are in an object just after a key, expecting to see a colon
      case SEP =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case ':' => rparse(DATA, i + 1, stack)

          case _ => die(i, "expected :")
        }

      // we are at a possible stopping point for an array, expecting to see
      // either a comma (before more data) or a closing bracket.
      case ARREND =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case ',' => rparse(DATA, i + 1, stack)

          case ']' =>
            stack match {
              case ctxt1 :: Nil =>
                (ctxt1.finish, i + 1)
              case ctxt1 :: ctxt2 :: tail =>
                ctxt2.add(ctxt1.finish)
                rparse(if (ctxt2.isObj) OBJEND else ARREND, i + 1, ctxt2 :: tail)
              case _ =>
                error("invalid stack")
            }

          case _ => die(i, "expected ] or ,")
        }

      // we are at a possible stopping point for an object, expecting to see
      // either a comma (before more data) or a closing brace.
      case OBJEND =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case ',' => rparse(KEY, i + 1, stack)

          case '}' =>
            stack match {
              case ctxt1 :: Nil =>
                (ctxt1.finish, i + 1)
              case ctxt1 :: ctxt2 :: tail =>
                ctxt2.add(ctxt1.finish)
                rparse(if (ctxt2.isObj) OBJEND else ARREND, i + 1, ctxt2 :: tail)
              case _ =>
                error("invalid stack")
            }

          case _ => die(i, "expected } or ,")
        }
    }
  }
}
