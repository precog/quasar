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

  def directoryQuery(uri: XQuery, urisOrDepth: XQuery*): XQuery =
    XQuery(s"cts:directory-query${mkSeq(uri +: urisOrDepth)}")

  def documentQuery(uri: XQuery, uris: XQuery*): XQuery =
    XQuery(s"cts:document-query${mkSeq(uri +: uris)}")

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
}
