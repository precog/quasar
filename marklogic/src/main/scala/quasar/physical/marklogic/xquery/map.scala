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

package quasar.physical.marklogic.xquery

import quasar.Predef._

import scalaz.{Foldable, IList}

object map {
  def contains(map: XQuery, key: XQuery): XQuery =
    XQuery(s"map:contains($map, $key)")

  def delete(map: XQuery, key: XQuery): XQuery =
    XQuery(s"map:delete($map, $key)")

  def entry(key: XQuery, value: XQuery): XQuery =
    XQuery(s"map:entry($key, $value)")

  def get(map: XQuery, key: XQuery): XQuery =
    XQuery(s"map:get($map, $key)")

  def keys(map: XQuery): XQuery =
    XQuery(s"map:keys($map)")

  def map[F[_]: Foldable](maps: F[XQuery]): XQuery =
    XQuery(s"map:map${mkSeq(maps)}")

  def map(): XQuery = map(IList[XQuery]())

  def new_(entries: XQuery): XQuery =
    XQuery(s"map:new($entries)")

  def put(map: XQuery, key: XQuery, value: XQuery): XQuery =
    XQuery(s"map:put($map, $key, $value)")
}
