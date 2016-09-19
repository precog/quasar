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
import quasar.physical.marklogic.xquery.syntax._

import java.lang.SuppressWarnings

import scalaz.{Order => _, _}
import scalaz.std.iterable._

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object cts {
  def andQuery(query: XQuery, queryOrOptions: XQuery*): XQuery =
    XQuery(s"cts:and-query${mkSeq_(query, queryOrOptions: _*)}")

  def directoryQuery(uri: XQuery, urisOrDepth: XQuery*): XQuery =
    XQuery(s"cts:directory-query${mkSeq(uri +: urisOrDepth)}")

  def documentOrder(direction: XQuery): XQuery =
    XQuery(s"cts:document-order($direction)")

  def elementValueQuery(elementName: XQuery, textOrOptionsOrWeight: XQuery*): XQuery =
    XQuery(s"cts:element-value-query${mkSeq_(elementName, textOrOptionsOrWeight: _*)}")

  def jsonPropertyValueQuery(propertyName: XQuery, propertyNameOrValueOrOptionsOrWeight: XQuery*): XQuery =
    XQuery(s"cts:json-property-value-query${mkSeq_(propertyName, propertyNameOrValueOrOptionsOrWeight: _*)}")

  def indexOrder(index: XQuery, options: XQuery*) =
    XQuery(s"cts:index-order($index, ${mkSeq(options)})")

  def orQuery(query: XQuery, queryOrOptions: XQuery*): XQuery =
    XQuery(s"cts:or-query${mkSeq_(query, queryOrOptions: _*)}")

  def search(
    expr: XQuery,
    query: XQuery,
    options: IList[XQuery] = IList.empty,
    qualityWeight: Option[XQuery] = None,
    forestIds: IList[XQuery] = IList.empty
  ): XQuery =
    XQuery(s"cts:search($expr, $query, ${mkSeq(options)}, ${qualityWeight getOrElse "1.0".xqy}, ${mkSeq(forestIds)})")

  val trueQuery: XQuery =
    XQuery("cts:true-query()")

  val uriReference: XQuery =
    XQuery("cts:uri-reference()")

  val uris: XQuery =
    XQuery("cts:uris()")

  def uris(start: XQuery, options: IList[XQuery]): XQuery =
    XQuery(s"cts:uris($start, ${mkSeq(options)})")

  def uris(
    start: XQuery,
    options: IList[XQuery] = IList.empty,
    query: XQuery,
    qualityWeight: Option[XQuery] = None,
    forestIds: IList[XQuery] = IList.empty
  ): XQuery =
    XQuery(s"cts:uris($start, ${mkSeq(options)}, $query, ${qualityWeight getOrElse "1.0".xqy}, ${mkSeq(forestIds)})")
}
