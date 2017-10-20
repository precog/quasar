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

import slamdata.Predef._
import quasar.{NonTerminal, RenderTree, RenderedTree, Terminal}
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._

import matryoshka.Delay
import scalaz._
import Scalaz._

trait SqlExprRenderTree {

  import SqlExpr._

  implicit val renderTree: Delay[RenderTree, SqlExpr] =
    new Delay[RenderTree, SqlExpr] {
      def apply[A](r: RenderTree[A]): RenderTree[SqlExpr[A]] = {

        def nonTerminal(typ: String, c: A*): RenderedTree =
          NonTerminal(typ :: Nil, none, c.toList ∘ r.render)

        RenderTree.make {
          case Id(v) =>
            Terminal("Id" :: Nil, v.some)
          case Null() =>
            Terminal("Null" :: Nil, none)
          case Data(v) =>
            Terminal("Data" :: Nil, v.shows.some)
          case Length(v) =>
            nonTerminal("Length", v)
          case Table(v) =>
            Terminal("Table" :: Nil, v.some)
          case RowIds() =>
            Terminal("row ids" :: Nil, none)
          case AllCols() =>
            Terminal("*" :: Nil, none)
          case SomeCols(names) =>
            Terminal("Columns" :: Nil, names.mkString(",").some)
          case ExprWithAlias(expr, alias) =>
            NonTerminal("Expr with alias" :: Nil, alias.v.some, List(r.render(expr)))
          case ExprPair(expr1, expr2) =>
            NonTerminal("Pair" :: Nil, none, List(expr1, expr2) ∘ r.render)
          case WithIds(v) =>
            nonTerminal("With ids", v)
          case Select(selection, from, filter) =>
            def nt(tpe: String, label: Option[String], child: A) =
              NonTerminal(tpe :: Nil, label, List(r.render(child)))

            NonTerminal(
              "Select" :: Nil,
              none,
              nt("selection", selection.alias ∘ (_.v), selection.v) ::
                nt("from", from.alias ∘ (_.v), from.v) ::
                (filter ∘ (f => nt("filter", none, f.v))).toList
            )
        }
      }
    }
}
