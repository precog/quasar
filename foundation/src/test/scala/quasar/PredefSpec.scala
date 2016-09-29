/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._

class PredefSpec extends Qspec {
  "Predef" should {
    "create tuples with ->" >> ((5 -> "a") must_=== ((5, "a")))
    "deconstruct tuples with ->" >> {
      def list[A](x: A -> A): List[A] = x match { case k -> v => List(k, v) }

      list(5 -> 15) must_=== List(5, 15)
    }
  }
}
