/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar

import slamdata.Predef.List

import org.specs2.matcher.Matcher

import java.lang.String

/**
 * Defines common machinery for testing functions that manipulate JSON
 * via differing backends.
 */
abstract class JsonSpec extends Qspec {

/**
 * The representation of a single json element in the underlying interpreter.
 *
 * You'll get nicer error messages if you keep this simple with nice toStrings.
 */
  type JsonElement

  type JsonStream = List[JsonElement]

  protected def ldjson(str: String): JsonStream

  protected def bestSemanticEqual(str: JsonStream): Matcher[JsonStream] =
    containTheSameElementsAs(str)
}
