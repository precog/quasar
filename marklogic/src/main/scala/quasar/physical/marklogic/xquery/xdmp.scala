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

import java.lang.SuppressWarnings

import scalaz.std.iterable._

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object xdmp {
  def apply(function: XQuery, params: XQuery*): XQuery =
    XQuery(s"xdmp:apply($function, ${mkSeq(params)})")

  def directory(uri: XQuery, depth: XQuery): XQuery =
    XQuery(s"xdmp:directory($uri, $depth)")

  def directoryCreate(uri: XQuery): XQuery =
    XQuery(s"xdmp:directory-create($uri)")

  def directoryDelete(uri: XQuery): XQuery =
    XQuery(s"xdmp:directory-delete($uri)")

  def documentDelete(uri: XQuery): XQuery =
    XQuery(s"xdmp:document-delete($uri)")

  def documentInsert(uri: XQuery, root: XQuery): XQuery =
    XQuery(s"xdmp:document-insert($uri, $root)")

  def documentProperties(uris: XQuery*): XQuery =
    XQuery(s"xdmp:document-properties${mkSeq(uris)}")

  def formatNumber(seq: XQuery, picture: XQuery): XQuery =
    XQuery(s"xdmp:format-number($seq, $picture)")

  def function(function: XQuery): XQuery =
    XQuery(s"xdmp:function($function)")

  def fromJson(node: XQuery): XQuery =
    XQuery(s"xdmp:from-json($node)")

  def hmacSha1(password: XQuery, message: XQuery, encoding: Option[XQuery] = None): XQuery =
    XQuery(s"xdmp:hmac-sha1($password, ${message}${asArg(encoding)})")

  def integerToHex(int: XQuery): XQuery =
    XQuery(s"xdmp:integer-to-hex($int)")

  def nodeKind(node: XQuery): XQuery =
    XQuery(s"xdmp:node-kind($node)")

  def nodeUri(node: XQuery): XQuery =
    XQuery(s"xdmp:node-uri($node)")

  def parseDateTime(picture: XQuery, value: XQuery): XQuery =
    XQuery(s"xdmp:parse-dateTime($picture, $value)")

  // TODO: Works, but has a bug that reorders `where` and `order by` clausees
  //       in FLWOR expressions, causing them to be malformed.
  def prettyPrint(qstring: XQuery): XQuery =
    XQuery(s"xdmp:pretty-print($qstring)")

  def quarterFromDate(date: XQuery): XQuery =
    XQuery(s"xdmp:quarter-from-date($date)")

  def toJson(serialized: XQuery): XQuery =
    XQuery(s"xdmp:to-json($serialized)")

  def weekFromDate(date: XQuery): XQuery =
    XQuery(s"xdmp:week-from-date($date)")

  def weekdayFromDate(date: XQuery): XQuery =
    XQuery(s"xdmp:weekday-from-date($date)")

  def yeardayFromDate(date: XQuery): XQuery =
    XQuery(s"xdmp:yearday-from-date($date)")
}
