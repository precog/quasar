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

package quasar.physical.couchbase

import scalaz._, Scalaz._

trait N1QLTraverseInstance {
  import N1QL._, Case._, Select._

  implicit val traverse: Traverse[N1QL] = new Traverse[N1QL] {
    def traverseImpl[G[_], A, B](
      fa: N1QL[A]
    )(
      f: A => G[B]
    )(
      implicit G: Applicative[G]
    ): G[N1QL[B]] = fa match {
      case Data(v)                 => G.point(Data(v))
      case Id(v)                   => G.point(Id(v))
      case Obj(m)                  => m.toList.traverse(_.bitraverse(f, f)) ∘ (l => Obj(l))
      case Arr(l)                  => l.traverse(f) ∘ (Arr(_))
      case Date(a1)                => f(a1) ∘ (Date(_))
      case Time(a1)                => f(a1) ∘ (Time(_))
      case Timestamp(a1)           => f(a1) ∘ (Timestamp(_))
      case Null()                  => G.point(Null())
      case Unreferenced()          => G.point(Unreferenced())
      case SelectField(a1, a2)     => (f(a1) ⊛ f(a2))(SelectField(_, _))
      case SelectElem(a1, a2)      => (f(a1) ⊛ f(a2))(SelectElem(_, _))
      case Slice(a1, a2)           => (f(a1) ⊛ a2.traverse(f))(Slice(_, _))
      case ConcatStr(a1, a2)       => (f(a1) ⊛ f(a2))(ConcatStr(_, _))
      case Not(a1)                 => f(a1) ∘ (Not(_))
      case Eq(a1, a2)              => (f(a1) ⊛ f(a2))(Eq(_, _))
      case Neq(a1, a2)             => (f(a1) ⊛ f(a2))(Neq(_, _))
      case Lt(a1, a2)              => (f(a1) ⊛ f(a2))(Lt(_, _))
      case Lte(a1, a2)             => (f(a1) ⊛ f(a2))(Lte(_, _))
      case Gt(a1, a2)              => (f(a1) ⊛ f(a2))(Gt(_, _))
      case Gte(a1, a2)             => (f(a1) ⊛ f(a2))(Gte(_, _))
      case IsNull(a1)              => f(a1) ∘ (IsNull(_))
      case IsNotNull(a1)           => f(a1) ∘ (IsNotNull(_))
      case Neg(a1)                 => f(a1) ∘ (Neg(_))
      case Add(a1, a2)             => (f(a1) ⊛ f(a2))(Add(_, _))
      case Sub(a1, a2)             => (f(a1) ⊛ f(a2))(Sub(_, _))
      case Mult(a1, a2)            => (f(a1) ⊛ f(a2))(Mult(_, _))
      case Div(a1, a2)             => (f(a1) ⊛ f(a2))(Div(_, _))
      case Mod(a1, a2)             => (f(a1) ⊛ f(a2))(Mod(_, _))
      case And(a1, a2)             => (f(a1) ⊛ f(a2))(And(_, _))
      case Or(a1, a2)              => (f(a1) ⊛ f(a2))(Or(_, _))
      case Meta(a1)                => f(a1) ∘ (Meta(_))
      case ConcatArr(a1, a2)       => (f(a1) ⊛ f(a2))(ConcatArr(_, _))
      case ConcatObj(a1, a2)       => (f(a1) ⊛ f(a2))(ConcatObj(_, _))
      case IfNull(a)               => (a.traverse(f)) ∘ (IfNull(_))
      case IfMissing(a)            => (a.traverse(f)) ∘ (IfMissing(_))
      case IfMissingOrNull(a)      => (a.traverse(f)) ∘ (IfMissingOrNull(_))
      case Type(a1)                => f(a1) ∘ (Type(_))
      case ToString(a1)            => f(a1) ∘ (ToString(_))
      case ToNumber(a1)            => f(a1) ∘ (ToNumber(_))
      case Floor(a1)               => f(a1) ∘ (Floor(_))
      case Length(a1)              => f(a1) ∘ (Length(_))
      case LengthArr(a1)           => f(a1) ∘ (LengthArr(_))
      case LengthObj(a1)           => f(a1) ∘ (LengthObj(_))
      case IsString(a1)            => f(a1) ∘ (IsString(_))
      case Lower(a1)               => f(a1) ∘ (Lower(_))
      case Upper(a1)               => f(a1) ∘ (Upper(_))
      case Split(a1, a2)           => (f(a1) ⊛ f(a2))(Split(_, _))
      case Substr(a1, a2, a3)      => (f(a1) ⊛ f(a2) ⊛ a3.traverse(f))(Substr(_, _, _))
      case RegexContains(a1, a2)   => (f(a1) ⊛ f(a2))(RegexContains(_, _))
      case Least(a)                => (a.traverse(f)) ∘ (Least(_))
      case Pow(a1, a2)             => (f(a1) ⊛ f(a2))(Pow(_, _))
      case Ceil(a1)                => f(a1) ∘ (Ceil(_))
      case Millis(a1)              => f(a1) ∘ (Millis(_))
      case MillisToUTC(a1, a2)     => (f(a1) ⊛ a2.traverse(f))(MillisToUTC(_, _))
      case DateAddStr(a1, a2, a3)  => (f(a1) ⊛ f(a2) ⊛ f(a3))(DateAddStr(_, _, _))
      case DatePartStr(a1, a2)     => (f(a1) ⊛ f(a2))(DatePartStr(_, _))
      case DateDiffStr(a1, a2, a3) => (f(a1) ⊛ f(a2) ⊛ f(a3))(DateDiffStr(_, _, _))
      case DateTruncStr(a1, a2)    => (f(a1) ⊛ f(a2))(DateTruncStr(_, _))
      case StrToMillis(a1)         => f(a1) ∘ (StrToMillis(_))
      case NowStr()                => G.point(NowStr())
      case ArrContains(a1, a2)     => (f(a1) ⊛ f(a2))(ArrContains(_, _))
      case ArrRange(a1, a2, a3)    => (f(a1) ⊛ f(a2) ⊛ a3.traverse(f))(ArrRange(_, _, _))
      case IsArr(a1)               => f(a1) ∘ (IsArr(_))
      case ObjNames(a1)            => f(a1) ∘ (ObjNames(_))
      case ObjValues(a1)           => f(a1) ∘ (ObjValues(_))
      case ObjRemove(a1, a2)       => (f(a1) ⊛ f(a2))(ObjRemove(_, _))
      case IsObj(a1)               => f(a1) ∘ (IsObj(_))
      case Avg(a1)                 => f(a1) ∘ (Avg(_))
      case Count(a1)               => f(a1) ∘ (Count(_))
      case Max(a1)                 => f(a1) ∘ (Max(_))
      case Min(a1)                 => f(a1) ∘ (Min(_))
      case Sum(a1)                 => f(a1) ∘ (Sum(_))
      case ArrAgg(a1)              => f(a1) ∘ (ArrAgg(_))
      case Union(a1, a2)           => (f(a1) ⊛ f(a2))(Union(_, _))
      case ArrFor(a1, a2, a3)      => (f(a1) ⊛ f(a2) ⊛ f(a3))(ArrFor(_, _, _))
      case Select(v, re, ks, jn, un, lt, fr, gb, ob) =>
        (re.traverse(i => f(i.expr) ∘ (ResultExpr(_, i.alias ∘ (a => Id[B](a.v)))))                            ⊛
         ks.traverse(i => f(i.expr) ∘ (Keyspace  (_, i.alias ∘ (a => Id[B](a.v)))))                            ⊛
         jn.traverse(i => f(i.pred) ∘ (LookupJoin(Id[B](i.id.v), i.alias ∘ (a => Id[B](a.v)), _, i.joinType))) ⊛
         un.traverse(i => f(i.expr) ∘ (Unnest    (_, i.alias ∘ (a => Id[B](a.v)))))                            ⊛
         lt.traverse(i => f(i.expr) ∘ (Binding(Id[B](i.id.v), _)))                                             ⊛
         fr.traverse(i => f(i.v)    ∘ (Filter(_)))                                                             ⊛
         gb.traverse(i => f(i.v)    ∘ (GroupBy(_)))                                                            ⊛
         ob.traverse(i => f(i.a)    ∘ (OrderBy   (_, i.sortDir)))
        )(
          Select(v, _, _, _, _, _, _, _, _)
        )
      case Case(wt, Else(e)) =>
        (wt.traverse { case WhenThen(w, t) => (f(w) ⊛ f(t))(WhenThen(_, _)) } ⊛
         f(e)
        )((wt, e) =>
          Case(wt, Else(e))
        )
    }
  }
}
