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

package quasar.physical.rdbms.planner.sql

import scalaz._, Scalaz._

trait SqlExprInstances extends SqlExprTraverse with SqlExprRenderTree

trait SqlExprTraverse {
  import SqlExpr._, Select._

  implicit val traverse: Traverse[SqlExpr] = new Traverse[SqlExpr] {
    def traverseImpl[G[_], A, B](
        fa: SqlExpr[A]
    )(
        f: A => G[B]
    )(
        implicit G: Applicative[G]
    ): G[SqlExpr[B]] = fa match {
      case Id(str)             => G.point(Id(str))
      case Data(v)             => G.point(Data(v))
      case Null()              => G.point(Null())
      case Length(v)           => f(v) ∘ Length.apply
      case ExprPair(e1, e2)    => (f(e1) ⊛ f(e2))(ExprPair.apply)
      case ExprWithAlias(e, a) => f(e) ∘ (ExprWithAlias(_, Id[B](a.v)))
      case Table(name)         => G.point(Table(name))
      case RowIds()            => G.point(RowIds())
      case AllCols()           => G.point(AllCols())
      case SomeCols(names)     => G.point(SomeCols(names))
      case WithIds(v)          => f(v) ∘ WithIds.apply

      case Select(selection, from, filterOpt) =>
        val sel = f(selection.v) ∘ (i => Selection(i, selection.alias ∘ (a => Id[B](a.v))))
        (sel ⊛
          (f(from.v) ∘ (From(_, from.alias ∘ (a => Id[B](a.v))))) ⊛
          filterOpt.traverse(i => f(i.v) ∘ Filter.apply))(
          Select(_, _, _)
        )
    }
  }
}
