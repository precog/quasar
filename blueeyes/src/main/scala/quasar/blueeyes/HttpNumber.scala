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

/* A method to ensure that numbers are dealt with as we would hope in Http
 * Headers -- i.e., paser should return an Option[Long] and not die on mangled
 * cases */
sealed trait HttpNumber {
  def number: Long
  def value = number.toString
  override def toString = value
}

object HttpNumbers {
  def parseHttpNumbers(inString: String): Option[HttpNumber] = {
    def NumParser = """[\d]+""".r
    NumParser.findFirstIn(inString.trim).map(x => LongNumber(x.toLong))
  }

  case class LongNumber(number: Long) extends HttpNumber
}


trait HttpNumberImplicits { 
  implicit def int2HttpNumber(num: Int): HttpNumber =
    HttpNumbers.LongNumber(num)

  implicit def long2HttpNumber(long: Long): HttpNumber =
    HttpNumbers.LongNumber(long)
}

object HttpNumberImplicits extends HttpNumberImplicits
