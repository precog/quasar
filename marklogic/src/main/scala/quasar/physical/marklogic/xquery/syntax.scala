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
import quasar.physical.marklogic.xquery.xml._

import scala.math.Integral

import eu.timepit.refined.api.Refined
import scalaz.{Functor, ISet}
import scalaz.std.iterable._
import scalaz.syntax.functor._

object syntax {
  import FunctionDecl._

  final case class NameBuilder(ns: NamespaceDecl, local: NCName) {
    def qn[F[_]](implicit F: PrologW[F]): F[QName] =
      F.writer(ISet singleton Prolog.nsDecl(ns), ns.prefix(local))

    def xqy[F[_]: PrologW]: F[XQuery] =
      xs map (fn.QName(ns.uri.xs, _))

    def xs[F[_]: PrologW]: F[XQuery] =
      qn map (_.xs)
  }

  def $(paramName: String Refined IsNCName): ParamName =
    ParamName(QName.local(NCName(paramName)))

  final implicit class XQueryStringOps(val str: String) extends scala.AnyVal {
    def xqy: XQuery = XQuery(str)
    def xs: XQuery = XQuery.StringLit(str)
  }

  final implicit class XQueryIntegralOps[N](val num: N)(implicit N: Integral[N]) {
    def xqy: XQuery = XQuery(N.toLong(num).toString)
  }

  final implicit class QNameOps(val qn: QName) extends scala.AnyVal {
    def apply(args: XQuery*): XQuery = XQuery(s"${qn}${mkSeq(args)}")
  }

  final implicit class QNameFOps[F[_]: Functor](val qnf: F[QName]) {
    def apply(args: XQuery*): F[XQuery] = qnf map (_(args: _*))
  }

  final implicit class NamespaceDeclOps(val ns: NamespaceDecl) extends scala.AnyVal {
    def name(local: String Refined IsNCName): NameBuilder = NameBuilder(ns, NCName(local))
  }

  final implicit class ModuleImportOps(val mod: ModuleImport) extends scala.AnyVal {
    def apply[F[_]](local: String Refined IsNCName)(implicit F: PrologW[F]): F[QName] =
      F.writer(ISet singleton Prolog.modImport(mod), QName(mod.prefix, NCName(local)))
  }

  final implicit class FunctionDecl1Ops(val func: FunctionDecl1) extends scala.AnyVal {
    def apply[F[_]](p1: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name(p1))
  }

  final implicit class FunctionDecl1FOps[F[_]](val funcF: F[FunctionDecl1]) extends scala.AnyVal {
    def apply(p1: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.bind(funcF)(_(p1))
    def getApply(implicit F: PrologW[F]): F[XQuery => XQuery] =
      F.bind(funcF)(f => F.writer(ISet singleton Prolog.funcDecl(f), xqy => f.name(xqy)))
  }

  final implicit class FunctionDecl2Ops(val func: FunctionDecl2) extends scala.AnyVal {
    def apply[F[_]](p1: XQuery, p2: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name(p1, p2))
  }

  final implicit class FunctionDecl2FOps[F[_]](val funcF: F[FunctionDecl2]) extends scala.AnyVal {
    def apply(p1: XQuery, p2: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.bind(funcF)(_(p1, p2))
  }

  final implicit class FunctionDecl3Ops(val func: FunctionDecl3) extends scala.AnyVal {
    def apply[F[_]](p1: XQuery, p2: XQuery, p3: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.writer(ISet singleton Prolog.funcDecl(func), func.name(p1, p2, p3))
  }

  final implicit class FunctionDecl3FOps[F[_]](val funcF: F[FunctionDecl3]) extends scala.AnyVal {
    def apply(p1: XQuery, p2: XQuery, p3: XQuery)(implicit F: PrologW[F]): F[XQuery] =
      F.bind(funcF)(_(p1, p2, p3))
  }
}
