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

package quasar.physical.marklogic.xml

import quasar.Predef._
import quasar.physical.marklogic.validation._

import eu.timepit.refined.refineV
import monocle.Prism
import scalaz.{Order, Show}
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.apply._
import scalaz.syntax.show._

final case class QName(prefix: Option[NSPrefix], local: NCName) {
  override def toString = this.shows
}

object QName {
  def local(name: NCName): QName =
    QName(None, name)

  def prefixed(prefix: NSPrefix, local: NCName): QName =
    QName(Some(prefix), local)

  val string: Prism[String, QName] = {
    def asNCName(s: String): Option[NCName] =
      refineV[IsNCName](s).right.toOption map (NCName(_))

    Prism((_: String).split(':') match {
      case Array(pfx, loc) => (asNCName(pfx) |@| asNCName(loc))((p, l) => QName.prefixed(NSPrefix(p), l))
      case Array(loc)      => asNCName(loc) map (QName.local)
      case _               => None
    })(_.shows)
  }

  implicit val order: Order[QName] =
    Order.orderBy(qn => (qn.prefix, qn.local))

  implicit val show: Show[QName] =
    Show.shows(qn => qn.prefix.fold(qn.local.shows)(p => s"${p.shows}:${qn.local.shows}"))
}
