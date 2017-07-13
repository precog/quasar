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

import java.lang.SuppressWarnings

import scalaz._
import scalaz.std.string._
import scalaz.std.iterable._
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object expr {
  def attribute(name: XQuery)(content: XQuery): XQuery =
    XQuery(s"attribute {$name} {$content}")

  def element(name: XQuery)(content: XQuery): XQuery =
    XQuery(s"element {$name} {$content}")

  val emptySeq: XQuery =
    XQuery("()")

  def every(b: Binding, bs: Binding*): QuantifiedExpr =
    QuantifiedExpr(Quantifier.Every, NonEmptyList(b, bs: _*))

  def for_(b: PositionalBinding, bs: PositionalBinding*): FlworExpr =
    FlworExpr.fromBindings(NonEmptyList(BindingClause.forClause(NonEmptyList(b, bs: _*))))

  // FIXME: Use TypedBindingName instead of string
  // FIXME: Refactor body to be args => XQuery
  def func(args: String*)(body: XQuery): XQuery =
    XQuery(s"function${mkSeq(args map (XQuery(_)))} { $body }")

  def if_(cond: XQuery): IfExpr =
    IfExpr(cond)

  def let_(b: Binding, bs: Binding*): FlworExpr =
    FlworExpr.fromBindings(NonEmptyList(BindingClause.letClause(NonEmptyList(b, bs: _*))))

  def isCastable(x: XQuery, tpe: SequenceType): XQuery =
    XQuery(s"$x castable as $tpe")

  def some(b: Binding, bs: Binding*): QuantifiedExpr =
    QuantifiedExpr(Quantifier.Some, NonEmptyList(b, bs: _*))

  def try_(body: XQuery): TryCatchExpr =
    TryCatchExpr(body)

  def typeswitch(on: XQuery)(cases: TypeswitchCaseClause*): TypeswitchExpr =
    TypeswitchExpr(on, cases.toList)

  final case class FlworExpr(
    bindingClauses: NonEmptyList[BindingClause],
    filterExpr: Option[XQuery],
    orderSpecs: IList[(XQuery, SortDirection)],
    orderIsStable: Boolean
  ) {
    def for_(b: PositionalBinding, bs: PositionalBinding*): FlworExpr =
      copy(bindingClauses = bindingClauses :::> IList(BindingClause.forClause(NonEmptyList(b, bs: _*))))

    def let_(b: Binding, bs: Binding*): FlworExpr =
      copy(bindingClauses = bindingClauses :::> IList(BindingClause.letClause(NonEmptyList(b, bs: _*))))

    def where_(expr: XQuery): FlworExpr =
      copy(filterExpr = Some(expr))

    def orderBy(s: (XQuery, SortDirection), ss: (XQuery, SortDirection)*): FlworExpr =
      copy(orderSpecs = s :: IList.fromList(ss.toList))

    def return_(expr: XQuery): XQuery =
      XQuery.Flwor(
        bindingClauses,
        filterExpr,
        orderSpecs,
        orderIsStable,
        expr)
  }

  object FlworExpr {
    def fromBindings(bindingClauses: NonEmptyList[BindingClause]): FlworExpr =
      FlworExpr(bindingClauses, None, IList.empty, false)
  }

  final case class IfExpr(cond: XQuery) {
    def then_(whenTrue: XQuery) = IfThenExpr(cond, whenTrue)
  }

  final case class IfThenExpr(cond: XQuery, whenTrue: XQuery) {
    def else_(whenFalse: XQuery): XQuery =
      XQuery(s"if ($cond) then $whenTrue else $whenFalse")
  }

  final case class TypeswitchExpr(on: XQuery, cases: List[TypeswitchCaseClause]) {
    @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
    def default(xqy: XQuery): XQuery =
      default(TypeswitchDefaultClause(None, xqy))

    @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
    def default(binding: BindingName, f: XQuery => XQuery): XQuery =
      default(TypeswitchDefaultClause(Some(binding), f(~binding)))

    def default(dc: TypeswitchDefaultClause): XQuery = {
      val body = (cases.map(_.render) :+ dc.render).map("  " + _).mkString("\n")
      XQuery(s"typeswitch($on)\n$body")
    }
  }

  final case class TypeswitchCaseClause(matching: TypedBindingName \/ SequenceType, result: XQuery) {
    def render: String =
      s"case ${matching.fold(_.render, _.toString)} return $result"
  }

  final case class TypeswitchDefaultClause(binding: Option[BindingName], result: XQuery) {
    def render: String = {
      val bind = binding.map(_.render + " ")
      s"default ${~bind}return $result"
    }
  }

  sealed abstract class Quantifier {
    override def toString = this match {
      case Quantifier.Some  => "some"
      case Quantifier.Every => "every"
    }
  }

  object Quantifier {
    case object Some  extends Quantifier
    case object Every extends Quantifier
  }

  // TODO: Should become an XQuery node
  final case class QuantifiedExpr(quantifier: Quantifier, bindings: NonEmptyList[Binding]) {
    def satisfies(xqy: XQuery): XQuery = {
      val streams = bindings map (_.render("in")) intercalate (", ")
      XQuery(s"$quantifier $streams satisfies $xqy")
    }
  }

  // TODO: Should become an XQuery node
  final case class TryCatchExpr(body: XQuery) {
    def catch_(e: BindingName)(f: XQuery => XQuery): XQuery =
      catchF[Id.Id](e)(f)

    def catchF[F[_]: Functor](e: BindingName)(f: XQuery => F[XQuery]): F[XQuery] =
      f(~e) map (recover => XQuery(s"try { $body } catch(${e.render}) { $recover }"))
  }
}
