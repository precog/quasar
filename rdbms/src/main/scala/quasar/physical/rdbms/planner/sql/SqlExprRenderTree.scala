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

        def nt(tpe: String, label: Option[String], child: A) =
          NonTerminal(tpe :: Nil, label, List(r.render(child)))

        RenderTree.make {
          case Unreferenced() =>
            Terminal("Unreferenced" :: Nil, none)
          case Null() =>
            Terminal("Null" :: Nil, none)
          case Constant(d) =>
            Terminal("Constant" :: Nil, d.shows.some)
          case Obj(m) =>
            NonTerminal("Obj" :: Nil, none, m.map { case (k, v) =>
              NonTerminal("K → V" :: Nil, none,
                List(r.render(k), r.render(v)))
            })
          case IsNotNull(a1) =>
            nonTerminal("NotNull", a1)
          case IfNull(a) =>
            nonTerminal("IfNull", a.toList: _*)
          case RegexMatches(a1, a2, caseInsensitive) =>
            nonTerminal(s"RegexMatches (insensitive = $caseInsensitive)", a1, a2)
          case ExprWithAlias(e, a) =>
            nonTerminal(s"ExprWithAlias($a)", e)
          case ExprPair(expr1, expr2) =>
            NonTerminal("Pair" :: Nil, none, List(expr1, expr2) ∘ r.render)
          case ConcatStr(a1, a2) =>
            nonTerminal("ConcatStr", a1, a2)
          case Time(a1) =>
            nonTerminal("Time", a1)
          case Id(v) =>
            Terminal("Id" :: Nil, v.some)
          case Table(v) =>
            Terminal("Table" :: Nil, v.some)
          case RowIds() =>
            Terminal("row ids" :: Nil, none)
          case AllCols() =>
            Terminal(s"*" :: Nil, none)
          case WithIds(v) =>
            nonTerminal("With ids", v)
          case NumericOp(op, left, right) =>
            nonTerminal(op, left, right)
          case Mod(a1, a2) =>
            nonTerminal("Mod", a1, a2)
          case Pow(a1, a2) =>
            nonTerminal("Pow", a1, a2)
          case Neg(a1) =>
            nonTerminal("Neg", a1)
          case And(a1, a2) =>
            nonTerminal("And", a1, a2)
          case Eq(a1, a2) =>
            nonTerminal("Equal", a1, a2)
          case Neq(a1, a2) =>
            nonTerminal("Not Equal", a1, a2)
          case Lt(a1, a2) =>
            nonTerminal("<", a1, a2)
          case Lte(a1, a2) =>
            nonTerminal("<=", a1, a2)
          case Gt(a1, a2) =>
            nonTerminal(">", a1, a2)
          case Gte(a1, a2) =>
            nonTerminal(">=", a1, a2)
          case Or(a1, a2) =>
            nonTerminal("Or", a1, a2)
          case Refs(srcs) =>
            nonTerminal("References", srcs:_*)
          case Select(selection, from, filter, order) =>
            NonTerminal(
              "Select" :: Nil,
              none,
              nt("selection", selection.alias ∘ (_.v), selection.v) ::
                nt("from", from.alias.v.some, from.v) ::
                (filter ∘ (f => nt("filter", none, f.v))).toList ++
                  order.map {
                    o =>
                      nt(s"OrderBy ${o.sortDir}", none, o.v)
                  }
            )
          case Union(left, right) =>
            nonTerminal("UNION", left, right)
          case Limit(from, count) => nonTerminal("Limit", from, count)
          case Offset(from, count) => nonTerminal("Offset", from, count)
          case Case(wt, e) =>
            NonTerminal("Case" :: Nil, none,
              (wt ∘ (i => nonTerminal("whenThen", i.when, i.`then`))).toList :+
                nonTerminal("else", e.v))
          case Coercion(t, e) =>
            nonTerminal(s"Coercion: $t", e)
          case ToArray(v) =>
            nonTerminal("ARRAY", v)
          case UnaryFunction(t, e) =>
            nonTerminal(s"Function call: $t", e)
          case BinaryFunction(t, a1, a2) =>
            nonTerminal(s"Function call: $t", a1, a2)
          case TernaryFunction(t, a1, a2, a3) =>
            nonTerminal(s"Function call: $t", a1, a2, a3)
        }
      }
    }
}
