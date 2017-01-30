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

object json {
  def arrayPop(arr: XQuery): XQuery =
    XQuery(s"json:array-pop($arr)")

  def arrayPush(arr: XQuery, item: XQuery): XQuery =
    XQuery(s"json:array-push($arr, $item)")

  def arrayValues(arr: XQuery): XQuery =
    XQuery(s"json:array-values($arr)")

  def toArray(itemSeq: XQuery): XQuery =
    XQuery(s"json:to-array($itemSeq)")
}
