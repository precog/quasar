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

class QuasarSpecSpec extends QuasarSpecification {
  "flaky tests" should {
    "pass if they pass" in true.flakyTest
    "be skipped if they fail" in false.flakyTest
    "be skipped and explain themselves if they can" in {
      5 must_=== 6
    }.flakyTest("I'm a flaky test")
  }
}
