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

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object fn {
  def abs(num: XQuery): XQuery =
    XQuery(s"fn:abs($num)")

  def avg(seq: XQuery): XQuery =
    XQuery(s"fn:avg($seq)")

  def baseUri(node: XQuery): XQuery =
    XQuery(s"fn:base-uri($node)")

  def ceiling(n: XQuery): XQuery =
    XQuery(s"fn:ceiling($n)")

  def codepointsToString(cps: XQuery): XQuery =
    XQuery(s"fn:codepoints-to-string($cps)")

  def concat(x: XQuery, xs: XQuery*): XQuery =
    XQuery(s"fn:concat${mkSeq_(x, xs: _*)}")

  def count(xs: XQuery, max: Option[XQuery] = None): XQuery =
    XQuery(s"fn:count(${xs}${asArg(max)})")

  val currentDateTime: XQuery =
    XQuery("fn:current-dateTime()")

  def dayFromDateTime(dt: XQuery): XQuery =
    XQuery(s"fn:day-from-dateTime($dt)")

  def distinctValues(seq: XQuery): XQuery =
    XQuery(s"fn:distinct-values($seq)")

  def data(item: XQuery): XQuery =
    XQuery(s"fn:data($item)")

  def doc(): XQuery =
    XQuery("fn:doc()")

  def doc(uri: XQuery): XQuery =
    XQuery(s"fn:doc($uri)")

  def documentUri(node: XQuery): XQuery =
    XQuery(s"fn:document-uri($node)")

  def empty(seq: XQuery): XQuery =
    XQuery(s"fn:empty($seq)")

  def error(err: XQuery, desc: Option[XQuery] = None, errObj: Option[XQuery] = None): XQuery =
    XQuery(s"fn:error(${err}${asArg(desc)}${asArg(errObj)})")

  def exists(seq: XQuery): XQuery =
    XQuery(s"fn:exists($seq)")

  val False: XQuery =
    XQuery("fn:false()")

  def filter(p: XQuery, seq: XQuery): XQuery =
    XQuery(s"fn:filter($p, $seq)")

  def floor(n: XQuery): XQuery =
    XQuery(s"fn:floor($n)")

  def formatDateTime(value: XQuery, picture: XQuery): XQuery =
    XQuery(s"fn:format-dateTime($value, $picture)")

  def formatTime(value: XQuery, picture: XQuery): XQuery =
    XQuery(s"fn:format-time($value, $picture)")

  def head(seq: XQuery): XQuery =
    XQuery(s"fn:head($seq)")

  def hoursFromDateTime(dt: XQuery): XQuery =
    XQuery(s"fn:hours-from-dateTime($dt)")

  def hoursFromDuration(dur: XQuery): XQuery =
    XQuery(s"fn:hours-from-duration($dur)")

  def indexOf(seq: XQuery, elt: XQuery): XQuery =
    XQuery(s"fn:index-of($seq, $elt)")

  val last: XQuery =
    XQuery("fn:last()")

  def lowerCase(str: XQuery): XQuery =
    XQuery(s"fn:lower-case($str)")

  def map(f: XQuery, seq: XQuery): XQuery =
    XQuery(s"fn:map($f, $seq)")

  def matches(input: XQuery, pattern: XQuery, flags: Option[XQuery] = None): XQuery =
    XQuery(s"fn:matches($input, ${pattern}${asArg(flags)})")

  def max(seq: XQuery): XQuery =
    XQuery(s"fn:max($seq)")

  def min(seq: XQuery): XQuery =
    XQuery(s"fn:min($seq)")

  def minutesFromDateTime(dt: XQuery): XQuery =
    XQuery(s"fn:minutes-from-dateTime($dt)")

  def minutesFromDuration(dur: XQuery): XQuery =
    XQuery(s"fn:minutes-from-duration($dur)")

  def monthFromDateTime(dt: XQuery): XQuery =
    XQuery(s"fn:month-from-dateTime($dt)")

  def nodeName(node: XQuery): XQuery =
    XQuery(s"fn:node-name($node)")

  def not(bool: XQuery): XQuery =
    XQuery(s"fn:not($bool)")

  def QName(localPart: XQuery): XQuery =
    XQuery(s"fn:QName($localPart)")

  def QName(namespace: XQuery, localPart: XQuery): XQuery =
    XQuery(s"fn:QName($namespace, $localPart)")

  def secondsFromDateTime(dt: XQuery): XQuery =
    XQuery(s"fn:seconds-from-dateTime($dt)")

  def startsWith(str: XQuery, prefix: XQuery, collation: Option[XQuery] = None): XQuery =
    XQuery(s"fn:starts-with($str, ${prefix}${asArg(collation)})")

  def string(xqy: XQuery): XQuery =
    XQuery(s"fn:string($xqy)")

  def stringLength(str: XQuery): XQuery =
    XQuery(s"fn:string-length($str)")

  def stringJoin(strs: XQuery, sep: XQuery): XQuery =
    XQuery(s"fn:string-join($strs, $sep)")

  def stringToCodepoints(str: XQuery): XQuery =
    XQuery(s"fn:string-to-codepoints($str)")

  def substring(str: XQuery, startLoc: XQuery, length: Option[XQuery] = None): XQuery =
    XQuery(s"fn:substring($str, ${startLoc}${asArg(length)})")

  def substringAfter(input: XQuery, after: XQuery): XQuery =
    XQuery(s"fn:substring-after($input, $after)")

  def subsequence(srcSeq: XQuery, startLoc: XQuery, length: Option[XQuery] = None): XQuery =
    XQuery(s"fn:subsequence($srcSeq, ${startLoc}${asArg(length)})")

  def sum(seq: XQuery): XQuery =
    XQuery(s"fn:sum($seq)")

  def timezoneFromDateTime(dt: XQuery): XQuery =
    XQuery(s"fn:timezone-from-dateTime($dt)")

  def tokenize(input: XQuery, pattern: XQuery, flags: Option[XQuery] = None): XQuery =
    XQuery(s"fn:tokenize($input, ${pattern}${asArg(flags)})")

  def trace(value: XQuery, label: XQuery): XQuery =
    XQuery(s"fn:trace($value, $label)")

  val True: XQuery =
    XQuery("fn:true()")

  def upperCase(str: XQuery): XQuery =
    XQuery(s"fn:upper-case($str)")

  def yearFromDateTime(dt: XQuery): XQuery =
    XQuery(s"fn:year-from-dateTime($dt)")
}
