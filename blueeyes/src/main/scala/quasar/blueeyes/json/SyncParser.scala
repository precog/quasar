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

import scala.annotation.switch
import scala.collection.mutable

private[json] trait SyncParser extends Parser {

  /**
    * Parse the JSON document into a single JSON value.
    *
    * The parser considers documents like '333', 'true', and '"foo"' to be
    * valid, as well as more traditional documents like [1,2,3,4,5]. However,
    * multiple top-level objects are not allowed.
    */
  final def parse(): JValue = {
    val (value, i) = parse(0)
    var j = i
    while (!atEof(j)) {
      (at(j): @switch) match {
        case '\n'              => newline(j); j += 1
        case ' ' | '\t' | '\r' => j += 1
        case _                 => die(j, "expected whitespace or eof")
      }
    }
    if (!atEof(j)) die(j, "expected eof")
    close()
    value
  }

  /**
    * Parse the given document into a sequence of JSON values. These might be
    * containers like objects and arrays, or primtitives like numbers and
    * strings.
    *
    * JSON objects may only be separated by whitespace. Thus, "top-level" commas
    * and other characters will become parse errors.
    */
  final def parseMany(): Seq[JValue] = {
    val results = mutable.ArrayBuffer.empty[JValue]
    var i = 0
    while (!atEof(i)) {
      (at(i): @switch) match {
        case '\n'              => newline(i); i += 1
        case ' ' | '\t' | '\r' => i += 1
        case _ =>
          val (value, j) = parse(i)
          results.append(value)
          i = j
      }
    }
    close()
    results
  }
}
