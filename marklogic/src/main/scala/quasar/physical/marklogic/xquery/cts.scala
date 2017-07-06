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

package quasar.physical.marklogic.xquery

import slamdata.Predef._
import quasar.physical.marklogic.xquery.syntax._

import java.lang.SuppressWarnings

import scalaz.{Order => _, _}
import scalaz.std.iterable._

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object cts {
  def andQuery(queries: XQuery): XQuery =
    XQuery(s"cts:and-query($queries)")

  def andNotQuery(positive: XQuery, negative: XQuery): XQuery =
    XQuery(s"cts:and-not-query($positive, $negative)")

  def directoryQuery(uris: XQuery, depth: XQuery): XQuery =
    XQuery(s"cts:directory-query($uris, $depth)")

  def documentQuery(uris: XQuery): XQuery =
    XQuery(s"cts:document-query($uris)")

  def documentFragmentQuery(query: XQuery): XQuery =
    XQuery(s"cts:document-fragment-query($query)")

  def collectionQuery(uris: XQuery): XQuery =
    XQuery(s"cts:collection-query($uris)")

  def elementQuery(elements: XQuery, query: XQuery): XQuery =
    XQuery(s"cts:element-query($elements)")

  def elementRangeQuery(elements: XQuery, operator: XQuery, values: XQuery): XQuery =
    XQuery(s"cts:element-range-query($elements, $operator, $values)")

  def elementValueQuery(elements: XQuery, values: XQuery): XQuery =
    XQuery(s"cts:element-value-query($elements, $values)")

  def elementWordQuery(elements: XQuery, words: XQuery): XQuery =
    XQuery(s"cts:element-word-query($elements, $words)")

  def elementAttributeRangeQuery(elements: XQuery, attributes: XQuery, operator: XQuery, values: XQuery): XQuery =
    XQuery(s"cts:element-attribute-range-query($elements, $attributes, $operator, $values)")

  def elementAttributeValueQuery(elements: XQuery, attributes: XQuery, values: XQuery): XQuery =
    XQuery(s"cts:element-attribute-value-query($elements, $attributes, $values)")

  def elementAttributeWordQuery(elements: XQuery, attributes: XQuery, words: XQuery): XQuery =
    XQuery(s"cts:element-attribute-word-query($elements, $attributes, $words)")

  def jsonPropertyRangeQuery(properties: XQuery, op: XQuery, values: XQuery): XQuery =
    XQuery(s"cts:json-property-range-query($properties, $op, $values)")

  def jsonPropertyScopeQuery(properties: XQuery, query: XQuery): XQuery =
    XQuery(s"cts:json-property-scope-query($properties, $query)")

  def jsonPropertyValueQuery(properties: XQuery, values: XQuery): XQuery =
    XQuery(s"cts:json-property-value-query($properties, $values)")

  def jsonPropertyWordQuery(properties: XQuery, words: XQuery): XQuery =
    XQuery(s"cts:json-property-word-query($properties, $words)")

  def nearQuery(queries: XQuery, weight: Double): XQuery =
    XQuery(s"cts:near-query($queries, $weight)")

  def wordQuery(texts: XQuery): XQuery =
    XQuery(s"cts:word-query($texts)")

  def pathRangeQuery(paths: XQuery, op: XQuery, values: XQuery) =
    XQuery(s"cts:path-range-query($paths, $op, $values)")

  def indexOrder(index: XQuery, options: XQuery*): XQuery =
    XQuery(s"cts:index-order($index, ${mkSeq(options)})")

  def notQuery(query: XQuery): XQuery =
    XQuery(s"cts:not-query($query)")

  def orQuery(queries: XQuery): XQuery =
    XQuery(s"cts:or-query($queries)")

  def search(
    expr: XQuery,
    query: XQuery,
    options: IList[XQuery] = IList.empty,
    qualityWeight: Option[XQuery] = None,
    forestIds: IList[XQuery] = IList.empty
  ): XQuery =
    XQuery(s"cts:search($expr, $query, ${mkSeq(options)}, ${qualityWeight getOrElse "1.0".xqy}, ${mkSeq(forestIds)})")

  def uriMatch(
    pattern: XQuery,
    options: IList[XQuery] = IList.empty,
    query: Option[XQuery] = None
  ): XQuery =
    XQuery(s"cts:uri-match($pattern, ${mkSeq(options)}${asArg(query)})")

  val uriReference: XQuery =
    XQuery("cts:uri-reference()")

  val uris: XQuery =
    XQuery("cts:uris()")

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  def uris(start: XQuery, options: IList[XQuery]): XQuery =
    XQuery(s"cts:uris($start, ${mkSeq(options)})")

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  def uris(
    start: XQuery,
    options: IList[XQuery] = IList.empty,
    query: XQuery,
    qualityWeight: Option[XQuery] = None,
    forestIds: IList[XQuery] = IList.empty
  ): XQuery =
    XQuery(s"cts:uris($start, ${mkSeq(options)}, $query, ${qualityWeight getOrElse "1.0".xqy}, ${mkSeq(forestIds)})")

  val True: XQuery =
    XQuery("cts:true-query()")

  val False: XQuery =
    XQuery("cts:false-query()")
}
