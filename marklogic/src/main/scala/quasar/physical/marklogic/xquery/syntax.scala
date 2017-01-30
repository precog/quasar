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
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xquery.{xs => xxs}

import scala.math.Integral
import scala.xml.Utility

import eu.timepit.refined.api.Refined
import scalaz._, Scalaz._
import scalaz.std.iterable._

object syntax {
  import FunctionDecl._
  import expr.TypeswitchCaseClause

  val ST = SequenceType

  final case class NameBuilder(decl: NamespaceDecl, local: NCName) {
    def qn[F[_]](implicit F: PrologW[F]): F[QName] =
      F.writer(ISet singleton Prolog.nsDecl(decl), decl.ns(local))

    def xqy[F[_]: Functor: PrologW]: F[XQuery] =
      qn map (_.xqy)

    def xs[F[_]: Functor: PrologW]: F[XQuery] =
      qn map (_.xs)
  }

  final case class PositionalBuilder(name: BindingName \/ TypedBindingName, at: BindingName) {
    def := (expression: XQuery): PositionalBinding =
      PositionalBinding(Binding(name, expression), Some(at))

    def in(expression: XQuery): PositionalBinding =
      this := expression
  }

  def $(bindingName: String Refined IsNCName): BindingName =
    BindingName(QName.local(NCName(bindingName)))

  // NB: Not ideal, but only used for syntatic purposes. A proper encoding of
  //     the XQuery AST should obviate this.
  implicit def bindingAsPositional(binding: Binding): PositionalBinding =
    PositionalBinding(binding, None)

  final implicit class BindingNameOps(val bn: BindingName) extends scala.AnyVal {
    def := (expression: XQuery): Binding =
      Binding(bn.left, expression)

    def at(pos: BindingName): PositionalBuilder =
      PositionalBuilder(bn.left, pos)

    def in(expression: XQuery): Binding =
      this := expression
  }

  final implicit class TypedBindingNameOps(val tb: TypedBindingName) extends scala.AnyVal {
    def := (expression: XQuery): Binding =
      Binding(tb.right, expression)

    def at(pos: BindingName): PositionalBuilder =
      PositionalBuilder(tb.right, pos)

    def in(expression: XQuery): Binding =
      this := expression

    def return_[F[_]: Functor](result: XQuery => F[XQuery]): F[TypeswitchCaseClause] =
      result(~tb) map (TypeswitchCaseClause(tb.left, _))

    def return_(result: XQuery => XQuery): TypeswitchCaseClause =
      return_[Id.Id](result)
  }

  final implicit class SequenceTypeOps(val tpe: SequenceType) {
    def return_(result: XQuery): TypeswitchCaseClause =
      TypeswitchCaseClause(tpe.right, result)
  }

  final implicit class XQueryStringOps(val str: String) extends scala.AnyVal {
    def xqy: XQuery = XQuery(str)
    def xs: XQuery = XQuery.StringLit(Utility.escape(str))
  }

  final implicit class XQueryIntegralOps[N](val num: N)(implicit N: Integral[N]) {
    def xqy: XQuery = XQuery(N.toLong(num).toString)
  }

  final implicit class QNameOps(val qn: QName) extends scala.AnyVal {
    def apply(args: XQuery*): XQuery = XQuery(s"${qn}${mkSeq(args)}")
    def :#(arity: Int): XQuery = XQuery(s"${qn}#$arity")
    def xqy: XQuery = xxs.QName(xs)
    def xs: XQuery = qn.shows.xs
  }

  final implicit class QNameFOps[F[_]: Functor](val qnf: F[QName]) {
    def apply(args: XQuery*): F[XQuery] = qnf map (_(args: _*))
  }

  final implicit class NCNameOps(val ncname: NCName) extends scala.AnyVal {
    def xs: XQuery = ncname.value.get.xs
  }

  final implicit class NSUriOps(val uri: NSUri) extends scala.AnyVal {
    def xs: XQuery = uri.value.get.xs
  }

  final implicit class NamespaceDeclOps(val ns: NamespaceDecl) extends scala.AnyVal {
    def name(local: String Refined IsNCName): NameBuilder = name(NCName(local))
    def name(local: NCName): NameBuilder = NameBuilder(ns, local)

    def declare[F[_]: Functor: PrologW](local: String Refined IsNCName): F[FunctionDeclDsl] =
      name(local).qn[F] map (quasar.physical.marklogic.xquery.declare)
  }

  final implicit class ModuleImportOps(val mod: ModuleImport) extends scala.AnyVal {
    def apply[F[_]](local: String Refined IsNCName)(implicit F: PrologW[F]): F[QName] =
      F.writer(ISet singleton Prolog.modImport(mod), QName(mod.prefix, NCName(local)))
  }

  final implicit class FunctionDeclOps[D <: FunctionDecl](val func: D) extends scala.AnyVal {
    def ref[F[_]](implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name :# func.arity)
  }

  final implicit class FunctionDecl1Ops(val func: FunctionDecl1) extends scala.AnyVal {
    def apply[F[_]](p1: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name(p1))
  }

  final implicit class FunctionDecl1FOps[F[_]](val funcF: F[FunctionDecl1]) extends scala.AnyVal {
    def apply(p1: XQuery)(implicit F0: Bind[F], F1: PrologW[F]): F[XQuery] =
      F0.bind(funcF)(_(p1))
  }

  final implicit class FunctionDecl2Ops(val func: FunctionDecl2) extends scala.AnyVal {
    def apply[F[_]](p1: XQuery, p2: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name(p1, p2))
  }

  final implicit class FunctionDecl2FOps[F[_]](val funcF: F[FunctionDecl2]) extends scala.AnyVal {
    def apply(p1: XQuery, p2: XQuery)(implicit F0: Bind[F], F1: PrologW[F]): F[XQuery] =
      F0.bind(funcF)(_(p1, p2))
  }

  final implicit class FunctionDecl3Ops(val func: FunctionDecl3) extends scala.AnyVal {
    def apply[F[_]](p1: XQuery, p2: XQuery, p3: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name(p1, p2, p3))
  }

  final implicit class FunctionDecl3FOps[F[_]](val funcF: F[FunctionDecl3]) extends scala.AnyVal {
    def apply(p1: XQuery, p2: XQuery, p3: XQuery)(implicit F0: Bind[F], F1: PrologW[F]): F[XQuery] =
      F0.bind(funcF)(_(p1, p2, p3))
  }

  final implicit class FunctionDecl4Ops(val func: FunctionDecl4) extends scala.AnyVal {
    def apply[F[_]](p1: XQuery, p2: XQuery, p3: XQuery, p4: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name(p1, p2, p3, p4))
  }

  final implicit class FunctionDecl4FOps[F[_]](val funcF: F[FunctionDecl4]) extends scala.AnyVal {
    def apply(p1: XQuery, p2: XQuery, p3: XQuery, p4: XQuery)(implicit F0: Bind[F], F1: PrologW[F]): F[XQuery] =
      F0.bind(funcF)(_(p1, p2, p3, p4))
  }

  final implicit class FunctionDecl5Ops(val func: FunctionDecl5) extends scala.AnyVal {
    def apply[F[_]](p1: XQuery, p2: XQuery, p3: XQuery, p4: XQuery, p5: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name(p1, p2, p3, p4, p5))
  }

  final implicit class FunctionDecl5FOps[F[_]](val funcF: F[FunctionDecl5]) extends scala.AnyVal {
    def apply(p1: XQuery, p2: XQuery, p3: XQuery, p4: XQuery, p5: XQuery)(implicit F0: Bind[F], F1: PrologW[F]): F[XQuery] =
      F0.bind(funcF)(_(p1, p2, p3, p4, p5))
  }
}
