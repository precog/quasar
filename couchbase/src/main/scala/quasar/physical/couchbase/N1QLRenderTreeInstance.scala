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

package quasar.physical.couchbase

import quasar.Predef._
import quasar._

import matryoshka._
import scalaz._, Scalaz._

trait N1QLRenderTreeInstance {
  import N1QL._

  implicit val renderTree: Delay[RenderTree, N1QL] = new Delay[RenderTree, N1QL] {
    def apply[A](r: RenderTree[A]): RenderTree[N1QL[A]] = {
      def nonTerminal(typ: String, c: A*): RenderedTree =
        NonTerminal(typ :: Nil, none, c.toList.map(r.render))

      RenderTree.make {
        case Data(v) =>
          Terminal("Data" :: Nil, v.shows.some)
        case Id(v) =>
          Terminal("Id" :: Nil, v.some)
        case Obj(m) =>
          NonTerminal("Obj" :: Nil, none, m.map { case (k, v) =>
            NonTerminal("K → V" :: Nil, none,
              List(r.render(k), r.render(v)))
          }.toList)
        case Arr(l) =>
          nonTerminal("Arr", l: _*)
        case Date(a1) =>
          nonTerminal("Date", a1)
        case Time(a1) =>
          nonTerminal("Time", a1)
        case Timestamp(a1) =>
          nonTerminal("Timestamp", a1)
        case Null() =>
          Terminal("Null" :: Nil, none)
        case SelectField(a1, a2) =>
          nonTerminal("SelectField", a1, a2)
        case SelectElem(a1, a2) =>
          nonTerminal("SelectElem", a1, a2)
        case Slice(a1, a2) =>
          nonTerminal("Slice", a1 :: a2.toList: _*)
        case ConcatStr(a1, a2) =>
          nonTerminal("ConcatStr", a1, a2)
        case Not(a1) =>
          nonTerminal("Not", a1)
        case Eq(a1, a2) =>
          nonTerminal("Eq", a1, a2)
        case Neq(a1, a2) =>
          nonTerminal("Neq", a1, a2)
        case Lt(a1, a2) =>
          nonTerminal("Lt", a1, a2)
        case Lte(a1, a2) =>
          nonTerminal("Lte", a1, a2)
        case Gt(a1, a2) =>
          nonTerminal("Gt", a1, a2)
        case Gte(a1, a2) =>
          nonTerminal("Gte", a1, a2)
        case IsNull(a1) =>
          nonTerminal("IsNull", a1)
        case IsNotNull(a1) =>
          nonTerminal("IsNotNull", a1)
        case Neg(a1) =>
          nonTerminal("Neg", a1)
        case Add(a1, a2) =>
          nonTerminal("Add", a1, a2)
        case Sub(a1, a2) =>
          nonTerminal("Sub", a1, a2)
        case Mult(a1, a2) =>
          nonTerminal("Mult", a1, a2)
        case Div(a1, a2) =>
          nonTerminal("Div", a1, a2)
        case Mod(a1, a2) =>
          nonTerminal("Mod", a1, a2)
        case And(a1, a2) =>
          nonTerminal("And", a1, a2)
        case Or(a1, a2) =>
          nonTerminal("Or", a1, a2)
        case Meta(a1) =>
          nonTerminal("Meta", a1)
        case ConcatArr(a1, a2) =>
          nonTerminal("ConcatArr", a1, a2)
        case ConcatObj(a1, a2) =>
          nonTerminal("ConcatObj", a1, a2)
        case IfNull(a) =>
          nonTerminal("IfNull", a.toList: _*)
        case IfMissing(a) =>
          nonTerminal("IfMissing", a.toList: _*)
        case IfMissingOrNull(a) =>
          nonTerminal("IfMissingOrNull", a.toList: _*)
        case Type(a1) =>
          nonTerminal("Type", a1)
        case ToString(a1) =>
          nonTerminal("ToString", a1)
        case ToNumber(a1) =>
          nonTerminal("ToNumber", a1)
        case Floor(a1) =>
          nonTerminal("Floor", a1)
        case Length(a1) =>
          nonTerminal("Length", a1)
        case LengthArr(a1) =>
          nonTerminal("LengthArr", a1)
        case LengthObj(a1) =>
          nonTerminal("LengthObj", a1)
        case IsString(a1) =>
          nonTerminal("IsString", a1)
        case Lower(a1) =>
          nonTerminal("Lower", a1)
        case Upper(a1) =>
          nonTerminal("Upper", a1)
        case Split(a1, a2) =>
          nonTerminal("Split", a1, a2)
        case Substr(a1, a2, a3) =>
          nonTerminal("Substr", a1 :: a2 :: a3.toList: _*)
        case RegexContains(a1, a2) =>
          nonTerminal("RegexContains", a1, a2)
        case Least(a) =>
          nonTerminal("Least", a.toList: _*)
        case Pow(a1, a2) =>
          nonTerminal("Pow", a1, a2)
        case Ceil(a1) =>
          nonTerminal("Ceil", a1)
        case Millis(a1) =>
          nonTerminal("Millis", a1)
        case MillisToUTC(a1, a2) =>
          nonTerminal("MillisToUTC", a1 :: a2.toList: _*)
        case DateAddStr(a1, a2, a3) =>
          nonTerminal("DateAddStr", a1, a2, a3)
        case DatePartStr(a1, a2) =>
          nonTerminal("DatePartStr", a1, a2)
        case DateDiffStr(a1, a2, a3) =>
          nonTerminal("DateDiffStr", a1, a2, a3)
        case DateTruncStr(a1, a2) =>
          nonTerminal("DateTruncStr", a1, a2)
        case StrToMillis(a1) =>
          nonTerminal("StrToMillis", a1)
        case NowStr() =>
          Terminal("NowStr" :: Nil, none)
        case ArrContains(a1, a2) =>
          nonTerminal("ArrContains", a1, a2)
        case ArrRange(a1, a2, a3) =>
          nonTerminal("ArrRange", a1 :: a2 :: a3.toList: _*)
        case IsArr(a1) =>
          nonTerminal("IsArr", a1)
        case ObjNames(a1) =>
          nonTerminal("ObjNames", a1)
        case ObjValues(a1) =>
          nonTerminal("ObjValues", a1)
        case ObjRemove(a1, a2) =>
          nonTerminal("ObjRemove", a1, a2)
        case IsObj(a1) =>
          nonTerminal("IsObj", a1)
        case Avg(a1) =>
          nonTerminal("Avg", a1)
        case Count(a1) =>
          nonTerminal("Count", a1)
        case Max(a1) =>
          nonTerminal("Max", a1)
        case Min(a1) =>
          nonTerminal("Min", a1)
        case Sum(a1) =>
          nonTerminal("Sum", a1)
        case ArrAgg(a1) =>
          nonTerminal("ArrAgg", a1)
        case Union(a1, a2) =>
          nonTerminal("Union", a1, a2)
        case ArrFor(a1, a2, a3) =>
          nonTerminal("ArrFor", a1, a2, a3)
        case Select(v, re, ks, un, lt, ft, gb, ob) =>
          def nt(tpe: String, label: Option[String], child: A) =
            NonTerminal(
              tpe :: Nil,
              label,
              List(r.render(child)))

          NonTerminal("Select" :: Nil, none,
            Terminal("value" :: Nil, v.v .shows.some)                      ::
            (re ∘ (i => nt("resultExpr", i.alias ∘ (_.v), i.expr))).toList :::
            (ks ∘ (i => nt("keyspace", i.alias ∘ (_.v), i.expr))).toList   :::
            (un ∘ (i => nt("unnest", i.alias ∘ (_.v), i.expr))).toList     :::
            (lt ∘ (i => nt("let", i.id.v.some, i.expr))).toList            :::
            (ft ∘ (f => nonTerminal("filter", f.v))).toList                :::
            (gb ∘ (g => nonTerminal("groupBy", g.v))).toList               :::
            (ob ∘ (i => nt("orderBy", i.sortDir.shows.some, i.a))).toList)
        case Case(wt, e) =>
          NonTerminal("Case" :: Nil, none,
            (wt ∘ (i => nonTerminal("whenThen", i.when, i.`then`))).toList :+
            nonTerminal("else", e.v))
      }
    }
  }
}
