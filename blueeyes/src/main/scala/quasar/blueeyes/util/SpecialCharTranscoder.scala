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

package quasar.blueeyes
package util

/** Transcodes special characters using an escape character.
  */
case class SpecialCharTranscoder(val escape: Char, encoding: PartialFunction[Char, Char], decoding: PartialFunction[Char, Char]) {
  private val defaultEscapeMapping: PartialFunction[Char, Char] = {
    case `escape` => escape
  }

  private val encodingF: Char => Option[Char] = encoding.orElse(defaultEscapeMapping).lift

  private val decodingF: Char => Option[Char] = decoding.orElse(defaultEscapeMapping).lift

  /** Takes an decoded string, and encodes it.
    */
  def encode(s: String): String = {
    val encoded = new StringBuilder

    for (i <- 0 until s.length) {
      val c = s.charAt(i)

      encodingF(c) match {
        case Some(remapped) =>
          encoded.append(escape).append(remapped)

        case None =>
          encoded.append(c)
      }
    }

    encoded.toString
  }

  /** Takes an encoded string, and decodes it.
    */
  def decode(s: String): String = {
    val decoded = new StringBuilder
    var escaping = false

    for (i <- 0 until s.length) {
      val c = s.charAt(i)

      if (escaping) {
        val original = decodingF(c).getOrElse(sys.error("Expected to find encoded character but found: " + c))

        decoded.append(original)

        escaping = false
      } else
        c match {
          case `escape` => escaping = true
          case _        => decoded.append(c)
        }
    }

    decoded.toString
  }
}
object SpecialCharTranscoder {
  def fromMap(escape: Char, map: Map[Char, Char]) = new SpecialCharTranscoder(escape, map, map.map(t => (t._2, t._1)))
}
