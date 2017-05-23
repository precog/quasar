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

package quasar.physical.marklogic

import slamdata.Predef._
import quasar.common.SortDir
import quasar.contrib.scalaz.{MonadListen_, MonadTell_}

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.Uri
import scalaz._
import scalaz.std.string._
import scalaz.std.iterable._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import _root_.xml.name._
import _root_.xml.name.validate._

package object xquery {

  type XPath = String

  type Prologs          = ISet[Prolog]
  type PrologT[F[_], A] = WriterT[F, Prologs, A]
  type PrologW[F[_]]    = MonadTell_[F, Prologs]
  type PrologL[F[_]]    = MonadListen_[F, Prologs]

  def PrologW[F[_]](implicit F: PrologW[F]): PrologW[F] = F
  def PrologL[F[_]](implicit F: PrologL[F]): PrologL[F] = F

  sealed abstract class SortDirection {
    def asOrderModifier: String = this match {
      case SortDirection.Ascending  => "ascending"
      case SortDirection.Descending => "descending"
    }
  }

  object SortDirection {

    case object Descending extends SortDirection
    case object Ascending  extends SortDirection

    def fromQScript(s: SortDir): SortDirection = s match {
      case SortDir.Ascending  => Ascending
      case SortDir.Descending => Descending
    }
  }

  final case class Binding(name: BindingName \/ TypedBindingName, expression: XQuery) {
    def render(relation: String): String =
      s"${name.fold(_.render, _.render)} $relation ${expression}"
  }

  final case class PositionalBinding(binding: Binding, at: Option[BindingName]) {
    def render(relation: String): String = {
      val renderedAt = ~at.map(bn => s" at ${bn.render}")
      s"${binding.name.fold(_.render, _.render)}$renderedAt $relation ${binding.expression}"
    }
  }

  final case class BindingName(value: QName) {
    def unary_~ : XQuery = XQuery(render)
    def as(tpe: SequenceType): TypedBindingName = TypedBindingName(this, tpe)
    def render: String = s"$$${value}"
  }

  object BindingName {
    implicit val order: Order[BindingName] =
      Order.orderBy(_.value)

    implicit val show: Show[BindingName] =
      Show.shows(bn => s"BindingName(${bn.render})")
  }

  final case class SequenceType(override val toString: String) extends scala.AnyVal

  object SequenceType {
    val Top: SequenceType = SequenceType("item()*")

    implicit val order: Order[SequenceType] =
      Order.orderBy(_.toString)

    implicit val show: Show[SequenceType] =
      Show.showFromToString
  }

  final case class TypedBindingName(name: BindingName, tpe: SequenceType) {
    def unary_~ : XQuery = ~name
    def render: String = s"${name.render} as $tpe"
  }

  object TypedBindingName {
    implicit val order: Order[TypedBindingName] =
      Order.orderBy(tn => (tn.name, tn.tpe))

    implicit val show: Show[TypedBindingName] =
      Show.shows(tn => s"TypedBindingName(${tn.render})")
  }

  def asArg(opt: Option[XQuery]): String =
    opt.map(", " + _.shows).orZero

  def declare(fname: QName): FunctionDecl.FunctionDeclDsl =
    FunctionDecl.FunctionDeclDsl(fname)

  def freshName[F[_]: QNameGenerator: Functor]: F[BindingName] =
    QNameGenerator[F].freshQName map (BindingName(_))

  def mkSeq[F[_]: Foldable](fa: F[XQuery]): XQuery =
    XQuery(s"(${fa.toList.map(_.render).intercalate(", ")})")

  def mkSeq_(x: XQuery, xs: XQuery*): XQuery =
    mkSeq(x +: xs)

  def module(prefix: String Refined IsNCName, uri: String Refined Uri, locs: (String Refined Uri)*): ModuleImport =
    ModuleImport(Some(NSPrefix(NCName(prefix))), NSUri(uri), locs.map(NSUri(_)).toIList)

  def namespace(prefix: String Refined IsNCName, uri: String Refined Uri): NamespaceDecl =
    NamespaceDecl(Namespace(NSPrefix(NCName(prefix)), NSUri(uri)))
}
