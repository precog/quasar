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
import quasar.physical.marklogic.validation._
import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.refineMV
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.Uri
import scalaz.{Order, Show}
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._

object xml {
  final case class Name(value: String Refined IsName) {
    override def toString = this.shows
  }

  object Name {
    implicit val order: Order[Name] =
      Order.orderBy(_.value.get)

    implicit val show: Show[Name] =
      Show.shows(_.value.get)
  }

  final case class NCName(value: String Refined IsNCName) {
    def xs: XQuery = value.get.xs
    override def toString = this.shows
  }

  object NCName {
    implicit val order: Order[NCName] =
      Order.orderBy(_.value.get)

    implicit val show: Show[NCName] =
      Show.shows(_.value.get)
  }

  /** A URI used to denote a namespace. */
  final case class NSUri(value: String Refined Uri) {
    def xs: XQuery = value.get.xs
    override def toString = this.shows
  }

  object NSUri {
    implicit val order: Order[NSUri] =
      Order.orderBy(_.value.get)

    implicit val show: Show[NSUri] =
      Show.shows(_.value.get)
  }

  /** A namespace prefix like `xs` or `fn`. */
  final case class NSPrefix(value: NCName) {
    def apply(local: NCName): QName = QName.prefixed(this, local)
    override def toString = this.shows
  }

  object NSPrefix {
    val local: NSPrefix =
      NSPrefix(NCName(refineMV[IsNCName]("local")))

    implicit val order: Order[NSPrefix] =
      Order.orderBy(_.value)

    implicit val show: Show[NSPrefix] =
      Show.shows(_.value.shows)
  }

  final case class QName(prefix: Option[NSPrefix], local: NCName) {
    def asName: Name = Name(Refined.unsafeApply(this.shows))
    def xs: XQuery = this.shows.xs
    override def toString = this.shows
  }

  object QName {
    def local(name: NCName): QName =
      QName(None, name)

    def prefixed(prefix: NSPrefix, local: NCName): QName =
      QName(Some(prefix), local)

    implicit val order: Order[QName] =
      Order.orderBy(qn => (qn.prefix, qn.local))

    implicit val show: Show[QName] =
      Show.shows(qn => qn.prefix.fold(qn.local.shows)(p => s"${p.shows}:${qn.local.shows}"))
  }
}
