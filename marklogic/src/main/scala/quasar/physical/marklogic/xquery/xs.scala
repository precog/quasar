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

object xs {
  def base64Binary(xqy: XQuery): XQuery =
    XQuery(s"xs:base64Binary($xqy)")

  def boolean(xqy: XQuery): XQuery =
    XQuery(s"xs:boolean($xqy)")

  def byte(xqy: XQuery): XQuery =
    XQuery(s"xs:byte($xqy)")

  def date(xqy: XQuery): XQuery =
    XQuery(s"xs:date($xqy)")

  def dateTime(xqy: XQuery): XQuery =
    XQuery(s"xs:dateTime($xqy)")

  def dateTime(date: XQuery, time: XQuery): XQuery =
    XQuery(s"xs:dateTime($date, $time)")

  def dayTimeDuration(xqy: XQuery): XQuery =
    XQuery(s"xs:dayTimeDuration($xqy)")

  def double(xqy: XQuery): XQuery =
    XQuery(s"xs:double($xqy)")

  def duration(xqy: XQuery): XQuery =
    XQuery(s"xs:duration($xqy)")

  def hexBinary(xqy: XQuery): XQuery =
    XQuery(s"xs:hexBinary($xqy)")

  def integer(xqy: XQuery): XQuery =
    XQuery(s"xs:integer($xqy)")

  def QName(xqy: XQuery): XQuery =
    XQuery(s"xs:QName($xqy)")

  def string(xqy: XQuery): XQuery =
    XQuery(s"xs:string($xqy)")

  def time(xqy: XQuery): XQuery =
    XQuery(s"xs:time($xqy)")
}
