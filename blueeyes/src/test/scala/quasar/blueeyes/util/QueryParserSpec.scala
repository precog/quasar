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

import QueryParser._
import java.net.URI
import java.net.URLEncoder._
import quasar.precog.TestSupport._

class QueryParserSpec extends Specification {
  val baseURI  = "http://www.socialmedia.com/test?"
  val encoding = "UTF-8"

  "Support 'normal' query params" in {
    val queryParams = "a=1&b=2"
    val query       = URI.create(baseURI + queryParams).getRawQuery()
    var params = parseQuery(query)
    params must (haveKey('a) and haveKey('b))
    params must (havePair(('a, "1")) and havePair(('b, "2")))
    unparseQuery(params) must_== (queryParams)
  }

  "Support value-less query params" in {
    val queryParams = "usermode"
    val query       = URI.create(baseURI + queryParams).getRawQuery()
    var params = parseQuery(query)
    params must haveKey('usermode)
    unparseQuery(params) must_== (queryParams)
  }

  "Support empty query string" in {
    val queryParams = ""
    val query       = URI.create(baseURI + queryParams).getRawQuery()
    var params = parseQuery(query)
    params must beEmpty
    unparseQuery(params) must_== (queryParams)
  }

  "Support query string with fragment appended" in {
    val queryParams = "flag=true"
    val query       = URI.create(baseURI + queryParams + "#fragment").getRawQuery()
    var params = parseQuery(query)
    params must havePairs(('flag, "true"))
    unparseQuery(params) must_== (queryParams)
  }

  "Support query string with <space>" in {
    val queryParams = "flag=true&path=" + encode("/hello world", encoding)
    val query       = URI.create(baseURI + queryParams).getRawQuery
    var params = parseQuery(query)
    params must havePair(('flag, "true"))
    unparseQuery(params) must_== (queryParams)
  }

  "Support query string with extra '?' in param name" in {
    val queryParams = "flag=true&" + encode("path?", encoding) + "=foo"
    val query       = URI.create(baseURI + queryParams).getRawQuery
    var params = parseQuery(query)
    params must (havePair(('flag, "true")) and havePair((Symbol("path?"), "foo")))
    unparseQuery(params) must_== (queryParams)
  }

  "Support query string with random '?'" in {
    val queryParams = "flag=true&" + encode("path??path2", encoding)
    val query       = URI.create(baseURI + queryParams).getRawQuery
    var params = parseQuery(query)
    params must (havePair(('flag, "true")) and havePair((Symbol("path??path2"), "")))
    unparseQuery(params) must_== (queryParams)
  }

  "Support empty parameter block '&&'" in {
    val queryParams = "flag=true&&foo=bar"
    val query       = URI.create(baseURI + queryParams).getRawQuery
    var params = parseQuery(query)
    params must (havePair(('flag, "true")) and havePair(('foo, "bar")))
    unparseQuery(params) must_== (queryParams.replace("&&", "&"))
  }

  "Support empty URI as param value" in {
    val queryParams = "site=" + encode("http://www.google.com?search=blah", encoding)
    val query       = URI.create(baseURI + queryParams).getRawQuery
    var params = parseQuery(query)
    params must havePair(('site, "http://www.google.com?search=blah"))
    unparseQuery(params) must_== (queryParams)
  }
}
