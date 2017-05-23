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

/**
  * Basic in-memory string parsing.
  *
  * This parser is limited to the maximum string size (~2G). Obviously for large
  * JSON documents it's better to avoid using this parser and go straight from
  * disk, to avoid having to load the whole thing into memory at once.
  */
private[json] final class StringParser(s: String) extends SyncParser with CharBasedParser {
  var line                                                             = 0
  final def column(i: Int): Int                                        = i
  final def newline(i: Int): Unit                                      = line += 1
  final def reset(i: Int): Int                                         = i
  final def checkpoint(state: Int, i: Int, stack: List[Context]): Unit = ()
  final def at(i: Int): Char                                           = s.charAt(i)
  final def at(i: Int, j: Int): String                                 = s.substring(i, j)
  final def atEof(i: Int): Boolean                                     = i == s.length
  final def close(): Unit                                              = ()
}
