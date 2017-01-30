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
import quasar.{Data => QData}
import quasar.common.SortDir

import scalaz._, NonEmptyList.nels, OneAnd.oneAnd

sealed abstract class N1QL[A]

// Overloading used for convenience constructors
@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
object N1QL extends N1QLInstances {
  import Select._, Case._

  // TODO: Move to EJson
  final case class Data[A](v: QData)                              extends N1QL[A]

  //
  final case class Id[A](v: String)                               extends N1QL[A]
  final case class Obj[A](m: Map[A, A])                           extends N1QL[A]
  final case class Arr[A](l: List[A])                             extends N1QL[A]
  final case class Date[A](a1: A)                                 extends N1QL[A]
  final case class Time[A](a1: A)                                 extends N1QL[A]
  final case class Timestamp[A](a1: A)                            extends N1QL[A]
  final case class Null[A]()                                      extends N1QL[A]

  // N1QL Operators
  final case class SelectField[A](a1: A, a2: A)                   extends N1QL[A]
  final case class SelectElem[A](a1: A, a2: A)                    extends N1QL[A]
  final case class Slice[A](a1: A, a2: Option[A])                 extends N1QL[A]
  final case class ConcatStr[A](a1: A, a2: A)                     extends N1QL[A]
  final case class Not[A](a1: A)                                  extends N1QL[A]
  final case class Eq[A](a1: A, a2: A)                            extends N1QL[A]
  final case class Neq[A](a1: A, a2: A)                           extends N1QL[A]
  final case class Lt[A](a1: A, a2: A)                            extends N1QL[A]
  final case class Lte[A](a1: A, a2: A)                           extends N1QL[A]
  final case class Gt[A](a1: A, a2: A)                            extends N1QL[A]
  final case class Gte[A](a1: A, a2: A)                           extends N1QL[A]
  final case class IsNull[A](a1: A)                               extends N1QL[A]
  final case class IsNotNull[A](a1: A)                            extends N1QL[A]
  final case class Neg[A](a1: A)                                  extends N1QL[A]
  final case class Add[A](a1: A, a2: A)                           extends N1QL[A]
  final case class Sub[A](a1: A, a2: A)                           extends N1QL[A]
  final case class Mult[A](a1: A, a2: A)                          extends N1QL[A]
  final case class Div[A](a1: A, a2: A)                           extends N1QL[A]
  final case class Mod[A](a1: A, a2: A)                           extends N1QL[A]
  final case class And[A](a1: A, a2: A)                           extends N1QL[A]
  final case class Or[A](a1: A, a2: A)                            extends N1QL[A]

  // N1QL Funcs
  final case class Meta[A](a1: A)                                 extends N1QL[A]
  final case class ConcatArr[A](a1: A, a2: A)                     extends N1QL[A]
  final case class ConcatObj[A](a1: A, a2: A)                     extends N1QL[A]
  final case class IfNull[A](a: OneAnd[NonEmptyList, A])          extends N1QL[A]
  final case class IfMissing[A](a: OneAnd[NonEmptyList, A])       extends N1QL[A]
  final case class IfMissingOrNull[A](a: OneAnd[NonEmptyList, A]) extends N1QL[A]
  final case class Type[A](a1: A)                                 extends N1QL[A]
  final case class ToString[A](a1: A)                             extends N1QL[A]
  final case class ToNumber[A](a1: A)                             extends N1QL[A]
  final case class Floor[A](a1: A)                                extends N1QL[A]
  final case class Length[A](a1: A)                               extends N1QL[A]
  final case class LengthArr[A](a1: A)                            extends N1QL[A]
  final case class LengthObj[A](a1: A)                            extends N1QL[A]
  final case class IsString[A](a1: A)                             extends N1QL[A]
  final case class Lower[A](a1: A)                                extends N1QL[A]
  final case class Upper[A](a1: A)                                extends N1QL[A]
  final case class Split[A](a1: A, a2: A)                         extends N1QL[A]
  final case class Substr[A](a1: A, a2: A, a3: Option[A])         extends N1QL[A]
  final case class RegexContains[A](a1: A, a2: A)                 extends N1QL[A]
  final case class Least[A](a: OneAnd[NonEmptyList, A])           extends N1QL[A]
  final case class Pow[A](a1: A, a2: A)                           extends N1QL[A]
  final case class Ceil[A](a1: A)                                 extends N1QL[A]
  final case class Millis[A](a1: A)                               extends N1QL[A]
  final case class MillisToUTC[A](a1: A, a2: Option[A])           extends N1QL[A]
  final case class DateAddStr[A](a1: A, a2: A, a3: A)             extends N1QL[A]
  final case class DatePartStr[A](a1: A, a2: A)                   extends N1QL[A]
  final case class DateDiffStr[A](a1: A, a2: A, a3: A)            extends N1QL[A]
  final case class DateTruncStr[A](a1: A, a2: A)                  extends N1QL[A]
  final case class StrToMillis[A](a1: A)                          extends N1QL[A]
  final case class NowStr[A]()                                    extends N1QL[A]
  final case class ArrContains[A](a1: A, a2: A)                   extends N1QL[A]
  final case class ArrRange[A](a1: A, a2: A, a3: Option[A])       extends N1QL[A]
  final case class IsArr[A](a1: A)                                extends N1QL[A]
  final case class ObjNames[A](a1: A)                             extends N1QL[A]
  final case class ObjValues[A](a1: A)                            extends N1QL[A]
  final case class ObjRemove[A](a1: A, a2: A)                     extends N1QL[A]
  final case class IsObj[A](a1: A)                                extends N1QL[A]

  object IfNull {
    def apply[A](a1: A, a2: A, a3: A*): IfNull[A] = IfNull(oneAnd(a1, nels(a2, a3: _*)))
  }
  object IfMissing {
    def apply[A](a1: A, a2: A, a3: A*): IfMissing[A] = IfMissing(oneAnd(a1, nels(a2, a3: _*)))
  }
  object IfMissingOrNull {
    def apply[A](a1: A, a2: A, a3: A*): IfMissingOrNull[A] = IfMissingOrNull(oneAnd(a1, nels(a2, a3: _*)))
  }
  object Least {
    def apply[A](a1: A, a2: A, a3: A*): Least[A] = Least(oneAnd(a1, nels(a2, a3: _*)))
  }

  // N1QL Aggregate Funcs
  final case class Avg[A](a1: A)                                  extends N1QL[A]
  final case class Count[A](a1: A)                                extends N1QL[A]
  final case class Max[A](a1: A)                                  extends N1QL[A]
  final case class Min[A](a1: A)                                  extends N1QL[A]
  final case class Sum[A](a1: A)                                  extends N1QL[A]
  final case class ArrAgg[A](a1: A)                               extends N1QL[A]

  //
  final case class Union[A](a1: A, a2: A)                         extends N1QL[A]
  final case class ArrFor[A](expr: A, `var`: A, inExpr: A)        extends N1QL[A]

  final case class Select[A](
    value: Value,
    resultExprs: NonEmptyList[ResultExpr[A]],
    keyspace: Option[Keyspace[A]],
    unnest: Option[Unnest[A]],
    let: List[Binding[A]],
    filter: Option[Filter[A]],
    groupBy: Option[GroupBy[A]],
    orderBy: List[OrderBy[A]]
  ) extends N1QL[A]

  object Select {
    final case class Value(v: Boolean)
    final case class ResultExpr[A](expr: A, alias: Option[Id[A]])
    final case class Keyspace[A](expr: A, alias: Option[Id[A]])
    final case class Unnest[A](expr: A, alias: Option[Id[A]])
    final case class Binding[A](id: Id[A], expr: A)
    final case class Filter[A](v: A)
    final case class GroupBy[A](v: A)
    final case class OrderBy[A](a: A, sortDir: SortDir)
  }

  final case class Case[A](
    whenThen: NonEmptyList[WhenThen[A]],
    `else`: Else[A]
  ) extends N1QL[A]

  object Case {
    final case class WhenThen[A](when: A, `then`: A)
    final case class Else[A](v: A)

    def apply[A](a1: WhenThen[A], a: WhenThen[A]*)(`else`: Else[A]): Case[A] =
      Case(nels(a1, a: _*), `else`)
  }
}
