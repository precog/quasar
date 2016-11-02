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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.NameGenerator
import quasar.physical.marklogic.validation._
import quasar.physical.marklogic.xml._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.Uri
import scalaz._
import scalaz.std.string._
import scalaz.std.iterable._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.syntax.std.option._

package object xquery {

  type XPath = String

  type Prologs = ISet[Prolog]
  type PrologW[F[_]] = MonadTell[F, Prologs]

  sealed abstract class SortDirection {
    def asOrderModifier: String = this match {
      case SortDirection.Ascending  => "ascending"
      case SortDirection.Descending => "descending"
    }
  }

  object SortDirection {

    case object Descending extends SortDirection
    case object Ascending  extends SortDirection

    def fromQScript(s: quasar.qscript.SortDir): SortDirection = s match {
      case quasar.qscript.SortDir.Ascending  => Ascending
      case quasar.qscript.SortDir.Descending => Descending
    }
  }

  final case class BindingName(value: QName) {
    def as(tpe: SequenceType): TypedBinding = TypedBinding(this, tpe)
    def xqy: XQuery = XQuery(s"$$${value}")
  }

  object BindingName {
    implicit val order: Order[BindingName] =
      Order.orderBy(_.value)

    implicit val show: Show[BindingName] =
      Show.shows(_.xqy.shows)
  }

  final case class SequenceType(override val toString: String) extends scala.AnyVal

  object SequenceType {
    val Top: SequenceType = SequenceType("item()*")

    implicit val order: Order[SequenceType] =
      Order.orderBy(_.toString)

    implicit val show: Show[SequenceType] =
      Show.showFromToString
  }

  final case class TypedBinding(name: BindingName, tpe: SequenceType) {
    def render: String = s"${name.xqy} as $tpe"
  }

  object TypedBinding {
    implicit val order: Order[TypedBinding] =
      Order.orderBy(fp => (fp.name, fp.tpe))

    implicit val show: Show[TypedBinding] =
      Show.shows(fp => s"TypedBinding(${fp.render})")
  }

  def asArg(opt: Option[XQuery]): String =
    opt.map(", " + _.shows).orZero

  def declare(fname: QName): FunctionDecl.FunctionDeclDsl =
    FunctionDecl.FunctionDeclDsl(fname)

  def declareLocal(fname: NCName): FunctionDecl.FunctionDeclDsl =
    declare(NSPrefix.local(fname))

  def freshVar[F[_]: NameGenerator: Functor]: F[String] =
    NameGenerator[F].prefixedName("$v")

  def mkSeq[F[_]: Foldable](fa: F[XQuery]): XQuery =
    XQuery(s"(${fa.toList.map(_.shows).intercalate(", ")})")

  def mkSeq_(x: XQuery, xs: XQuery*): XQuery =
    mkSeq(x +: xs)

  def module(prefix: String Refined IsNCName, uri: String Refined Uri, locs: (String Refined Uri)*): ModuleImport =
    ModuleImport(Some(NSPrefix(NCName(prefix))), NSUri(uri), locs.map(NSUri(_)).toIList)

  def namespace(prefix: String Refined IsNCName, uri: String Refined Uri): NamespaceDecl =
    NamespaceDecl(Namespace(NSPrefix(NCName(prefix)), NSUri(uri)))
}
