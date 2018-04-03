/*
 * Copyright 2014–2018 SlamData Inc.
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

import slamdata.Predef._
import quasar.{Data => QData}
import quasar.common.{JoinType, SortDir}, SortDir._
import quasar.DataGenerators._
import quasar.physical.couchbase.N1QL.{Eq, Id, Split, _}, Case._, Select.{Value, _}

import scala.Predef.$conforms

import org.scalacheck._
import org.specs2.scalaz.Spec
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazProperties, ScalazProperties._
import scalaz._, Scalaz._

class N1QLSpec extends Spec with N1QLArbitrary {

    checkAll(ScalazProperties.traverse.laws[N1QL])

}

trait N1QLArbitrary {
  def arb[A: Arbitrary] = Arbitrary.arbitrary[A]

  // Overly simplistic
  implicit def equal[A]: Equal[N1QL[A]] = Equal.equalA

  implicit def arbId[A]: Arbitrary[Id[A]] =
    Arbitrary(arb[String] ∘ (Id[A](_)))

  implicit def arbValue: Arbitrary[Value] =
    Arbitrary(arb[Boolean] ∘ (Value(_)))

  implicit def arbResultExpr[A: Arbitrary]: Arbitrary[ResultExpr[A]] =
    Arbitrary((arb[A] ⊛ arb[Option[Id[A]]])(ResultExpr(_, _)))

  implicit def arbKeyspace[A: Arbitrary]: Arbitrary[Keyspace[A]] =
    Arbitrary((arb[A] ⊛ arb[Option[Id[A]]])(Keyspace(_, _)))

  implicit def arbLookupJoin[A: Arbitrary]: Arbitrary[LookupJoin[A]] =
    Arbitrary((
      arb[Id[A]]         ⊛
      arb[Option[Id[A]]] ⊛
      arb[A]             ⊛
      Gen.oneOf(JoinType.LeftOuter.left, JoinType.Inner.right)
     )(
      LookupJoin(_, _, _, _))
    )

  implicit def arbUnnest[A: Arbitrary]: Arbitrary[Unnest[A]] =
    Arbitrary((arb[A] ⊛ arb[Option[Id[A]]])(Unnest(_, _)))

  implicit def arbBindingExpr[A: Arbitrary]: Arbitrary[Binding[A]] =
    Arbitrary((arb[Id[A]] ⊛ arb[A])(Binding(_, _)))

  implicit def arbFilter[A: Arbitrary]: Arbitrary[Filter[A]] =
    Arbitrary(arb[A] ∘ (Filter(_)))

  implicit def arbGroupBy[A: Arbitrary]: Arbitrary[GroupBy[A]] =
    Arbitrary(arb[A] ∘ (GroupBy(_)))

  implicit def arbOrderBy[A: Arbitrary]: Arbitrary[OrderBy[A]] =
    Arbitrary((arb[A] ⊛ arb[SortDir])(OrderBy(_, _)))

  implicit def arbWhenThen[A: Arbitrary]: Arbitrary[WhenThen[A]] =
    Arbitrary((arb[A] ⊛ arb[A])(WhenThen(_, _)))

  implicit def arbElse[A: Arbitrary]: Arbitrary[Else[A]] =
     Arbitrary(arb[A] ∘ (Else(_)))

  implicit val arbSortDir: Arbitrary[SortDir] =
    Arbitrary(Gen.oneOf(Ascending, Descending))

  // TODO: Fragile to changes in N1QL terms
  implicit def arbN1QL[A: Arbitrary]: Arbitrary[N1QL[A]] = Arbitrary(Gen.oneOf(
    arb[QData]                         ∘ (Data[A](_)),
    arb[String]                        ∘ (Id[A](_)),
    arb[List[(A, A)]]                  ∘ (Obj(_)),
    arb[List[A]]                       ∘ (Arr(_)),
    arb[A]                             ∘ (Time(_)),
    arb[A]                             ∘ (Timestamp(_)),
    Gen.const                            (Null[A]()),
    (arb[A] ⊛ arb[A])                    (SelectField(_, _)),
    (arb[A] ⊛ arb[A])                    (SelectElem(_, _)),
    (arb[A] ⊛ arb[Option[A]])            (Slice(_, _)),
    (arb[A] ⊛ arb[A])                    (ConcatStr(_, _)),
    arb[A]                             ∘ (Not(_)),
    (arb[A] ⊛ arb[A])                    (Eq(_, _)),
    (arb[A] ⊛ arb[A])                    (Neq(_, _)),
    (arb[A] ⊛ arb[A])                    (Lt(_, _)),
    (arb[A] ⊛ arb[A])                    (Lte(_, _)),
    (arb[A] ⊛ arb[A])                    (Gt(_, _)),
    (arb[A] ⊛ arb[A])                    (Gte(_, _)),
    arb[A]                             ∘ (IsNull(_)),
    arb[A]                             ∘ (IsNotNull(_)),
    arb[A]                             ∘ (Neg(_)),
    (arb[A] ⊛ arb[A])                    (Add(_, _)),
    (arb[A] ⊛ arb[A])                    (Sub(_, _)),
    (arb[A] ⊛ arb[A])                    (Mult(_, _)),
    (arb[A] ⊛ arb[A])                    (Div(_, _)),
    (arb[A] ⊛ arb[A])                    (Mod(_, _)),
    (arb[A] ⊛ arb[A])                    (And(_, _)),
    (arb[A] ⊛ arb[A])                    (Or(_, _)),
    arb[A]                             ∘ (Meta(_)),
    (arb[A] ⊛ arb[A])                    (ConcatArr(_, _)),
    (arb[A] ⊛ arb[A])                    (ConcatObj(_, _)),
    arb[OneAnd[NonEmptyList, A]]       ∘ (IfNull(_)),
    arb[OneAnd[NonEmptyList, A]]       ∘ (IfMissing(_)),
    arb[OneAnd[NonEmptyList, A]]       ∘ (IfMissingOrNull(_)),
    arb[A]                             ∘ (Type(_)),
    arb[A]                             ∘ (ToString(_)),
    arb[A]                             ∘ (ToNumber(_)),
    arb[A]                             ∘ (Floor(_)),
    arb[A]                             ∘ (Length(_)),
    arb[A]                             ∘ (LengthArr(_)),
    arb[A]                             ∘ (LengthObj(_)),
    arb[A]                             ∘ (IsString(_)),
    arb[A]                             ∘ (Lower(_)),
    arb[A]                             ∘ (Upper(_)),
    (arb[A] ⊛ arb[A])                    (Split(_, _)),
    (arb[A] ⊛ arb[A] ⊛ arb[Option[A]])   (Substr(_, _, _)),
    (arb[A] ⊛ arb[A])                    (RegexContains(_, _)),
    arb[OneAnd[NonEmptyList, A]]       ∘ (Least(_)),
    (arb[A] ⊛ arb[A])                    (Pow(_, _)),
    arb[A]                             ∘ (Ceil(_)),
    arb[A]                             ∘ (Millis(_)),
    (arb[A] ⊛ arb[Option[A]])            (MillisToUTC(_, _)),
    (arb[A] ⊛ arb[A] ⊛ arb[A])           (DateAddStr(_, _, _)),
    (arb[A] ⊛ arb[A])                    (DatePartStr(_, _)),
    (arb[A] ⊛ arb[A] ⊛ arb[A])           (DateDiffStr(_, _, _)),
    (arb[A] ⊛ arb[A])                    (DateTruncStr(_, _)),
    arb[A]                             ∘ (StrToMillis(_)),
    Gen.const                            (NowStr[A]()),
    (arb[A] ⊛ arb[A])                    (ArrContains(_, _)),
    (arb[A] ⊛ arb[A] ⊛ arb[Option[A]])   (ArrRange(_, _, _)),
    arb[A]                             ∘ (IsArr(_)),
    arb[A]                             ∘ (ObjNames(_)),
    arb[A]                             ∘ (ObjValues(_)),
    (arb[A] ⊛ arb[A])                    (ObjRemove(_, _)),
    arb[A]                             ∘ (IsObj(_)),
    arb[A]                             ∘ (Avg(_)),
    arb[A]                             ∘ (Count(_)),
    arb[A]                             ∘ (Max(_)),
    arb[A]                             ∘ (Min(_)),
    arb[A]                             ∘ (Sum(_)),
    arb[A]                             ∘ (ArrAgg(_)),
    (arb[A] ⊛ arb[A])                    (Union(_, _)),
    (arb[A] ⊛ arb[A] ⊛ arb[A])           (ArrFor(_, _, _)),
    (arb[Value]                       ⊛
     arb[NonEmptyList[ResultExpr[A]]] ⊛
     arb[Option[Keyspace[A]]]         ⊛
     arb[Option[LookupJoin[A]]]       ⊛
     arb[Option[Unnest[A]]]           ⊛
     arb[List[Binding[A]]]            ⊛
     arb[Option[Filter[A]]]           ⊛
     arb[Option[GroupBy[A]]]          ⊛
     arb[List[OrderBy[A]]]
    )(
      Select(_, _, _, _, _, _, _, _, _)
    ),
    (arb[NonEmptyList[WhenThen[A]]]  ⊛
     arb[Else[A]]
   )(
     Case(_, _)
   )
  ))

}
