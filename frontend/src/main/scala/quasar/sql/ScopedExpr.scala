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

package quasar.sql

import slamdata.Predef._
import quasar._, RenderTree.ops._

import matryoshka.Recursive
import monocle.macros.Lenses
import scalaz._, Scalaz._

@Lenses final case class ScopedExpr[T](expr: T, scope: List[Statement[T]]) {
  def mapExpressionM[M[_]:Functor](f: T => M[T]): M[ScopedExpr[T]] =
    f(expr).map(ScopedExpr(_, scope))
  def imports: List[Import[T]] =
    scope.collect { case i: Import[_] => i }
  def defs: List[FunctionDecl[T]] =
    scope.collect { case d: FunctionDecl[_] => d }
  def pprint(implicit T: Recursive.Aux[T, Sql]): String = {
    val scopeString = if (scope.isEmpty) "" else scope.pprint + ";\n"
    scopeString + sql.pprint(expr)
  }
}

object ScopedExpr {
  implicit def renderTree[T:RenderTree]: RenderTree[ScopedExpr[T]] =
    new RenderTree[ScopedExpr[T]] {
      def render(sExpr: ScopedExpr[T]) =
        NonTerminal("Sql Scoped Expr" :: Nil, None, List(sExpr.scope.render, sExpr.expr.render))
    }
  implicit val traverse: Traverse[ScopedExpr] = new Traverse[ScopedExpr] {
    def traverseImpl[G[_]:Applicative,A,B](ba: ScopedExpr[A])(f: A => G[B]): G[ScopedExpr[B]] =
      (f(ba.expr) |@| ba.scope.traverse(_.traverse(f)))(ScopedExpr(_, _))
  }

  implicit def equal[T: Equal]: Equal[ScopedExpr[T]] = Equal.equalBy(s => (s.expr, s.scope))
}
