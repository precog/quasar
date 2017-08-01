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

import monocle.macros.Lenses
import scalaz._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import xml.name._

@Lenses
final case class ModuleImport(prefix: Option[NSPrefix], uri: NSUri, locs: IList[NSUri]) {
  def render: String = {
    val pfxStr = prefix.map(p => s" namespace ${p.shows} =")
    val locStr = locs.toNel.map(ls => s" at ${ls.map(_.xs.shows).intercalate(", ")}")
    s"import module${~pfxStr} ${uri.xs}${~locStr}"
  }
}

object ModuleImport {
  def prefixed(pfx: NSPrefix, uri: NSUri): ModuleImport =
    ModuleImport(some(pfx), uri, IList.empty)

  implicit val order: Order[ModuleImport] =
    Order.orderBy(mi => (mi.prefix, mi.uri, mi.locs))

  implicit val show: Show[ModuleImport] =
    Show.shows(mi => s"ModuleImport(${mi.render})")
}
