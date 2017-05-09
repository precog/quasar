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

import org.specs2.mutable.Specification
import HttpHeaders._

class CacheDirectiveSpec extends Specification{

  "Cache-Directive: Should parse a cache directive with a field correctly" in {
    val testString1 = "private=\"this\", no-cache, max-age=10, no-transform"
    HttpHeaders.`Cache-Control`(CacheDirectives.parseCacheDirectives(testString1): _*).value mustEqual testString1
  }  

  "Cache-Directive: Should parse a cache-directive with a delta " in {
    val testString2 = "private, no-cache, max-stale=590, no-transform"
    HttpHeaders.`Cache-Control`(CacheDirectives.parseCacheDirectives(testString2): _*).value mustEqual testString2
  }

  "Cache-Directive: Should return empty array on bad input" in {
    val testString3 = "amnamzimmeram"
    CacheDirectives.parseCacheDirectives(testString3).length mustEqual 0
  }
}
