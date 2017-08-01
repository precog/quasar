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

/** Transcodes special characters to characters.
  */
case class SpecialCharToStringTranscoder(encoding: PartialFunction[Char, String], decoding: PartialFunction[List[Char], Option[Char]]) {

  private val encodingF: Char => Option[String] = encoding.lift

  private val decodingF: List[Char] => Option[Option[Char]] = decoding.lift

  /** Takes an decoded string, and encodes it.
    */
  def encode(s: String): String = {
    val encoded = new StringBuilder

    for (i <- 0 until s.length) {
      val c = s.charAt(i)

      encodingF(c) match {
        case Some(remapped) =>
          encoded.append(remapped)

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
    var decodingPart: List[Char] = Nil

    for (i <- 0 until s.length) {
      decodingPart = decodingPart ::: List(s.charAt(i))

      decodingF(decodingPart) match {
        case Some(None) =>
        case Some(Some(decodedChar)) =>
          decoded.append(decodedChar)
          decodingPart = Nil
        case None =>
          decoded.append(decodingPart.mkString(""))
          decodingPart = Nil
      }
    }
    decoded.append(decodingPart.mkString(""))

    decoded.toString
  }
}
