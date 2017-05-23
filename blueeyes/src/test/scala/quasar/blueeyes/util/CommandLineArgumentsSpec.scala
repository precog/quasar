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

import quasar.precog.TestSupport._

class CommandLineArgumentsSpec extends Specification {
  "No parameters or values should be parsed properly" in {
    val c = CommandLineArguments()

    c.parameters.size mustEqual (0)
    c.values.length mustEqual (0)
  }

  "Several parameters should be parsed properly" in {
    val c = CommandLineArguments("--foo", "bar", "--bar", "baz")

    c.parameters mustEqual Map(
      "foo" -> "bar",
      "bar" -> "baz"
    )
  }

  "Values combined with parameters should be parsed properly" in {
    val c = CommandLineArguments("baz", "--foo", "bar", "bar")

    c.parameters mustEqual Map("foo" -> "bar")
    c.values mustEqual List("baz", "bar")
  }

  "Parameters without values should have empty value strings" in {
    val c = CommandLineArguments("--baz", "--foo")

    c.parameters mustEqual Map("baz" -> "", "foo" -> "")
  }

  "Values combined with parameters should be counted properly" in {
    val c = CommandLineArguments("baz", "--foo", "bar", "bar")

    c.size mustEqual 3
  }
}
