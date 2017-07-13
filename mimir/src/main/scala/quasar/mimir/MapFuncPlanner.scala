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

package quasar.mimir

import quasar.Data
import quasar.qscript.{MapFuncCore, MapFuncsCore}

import quasar.blueeyes.json.JValue
import quasar.precog.common.RValue
import quasar.yggdrasil.table.cf.math
import quasar.yggdrasil.table.cf.util.Undefined

import matryoshka.{AlgebraM, RecursiveT}
import matryoshka.implicits._

import scalaz.Applicative
import scalaz.syntax.applicative._

final class MapFuncPlanner[T[_[_]]: RecursiveT, M[_], F[_]: Applicative] {
  def plan(cake: Precog): AlgebraM[F, MapFuncCore[T, ?], cake.trans.TransSpec1] = {
    import cake.trans._
    {
      case MapFuncsCore.Undefined() =>
        (Map1[Source1](TransSpec1.Id, Undefined): TransSpec1).point[F]

      case MapFuncsCore.Constant(ejson) =>
        // EJson => Data => JValue => RValue => Table
        val data: Data = ejson.cata(Data.fromEJson)
        val jvalue: JValue = JValue.fromData(data)
        val rvalue: RValue = RValue.fromJValue(jvalue)
        transRValue(rvalue, TransSpec1.Id).point[F]

      case MapFuncsCore.JoinSideName(_) => ??? // should never be received

      case MapFuncsCore.Length(a1) => ???

      case MapFuncsCore.ExtractCentury(a1) => ???
      case MapFuncsCore.ExtractDayOfMonth(a1) => ???
      case MapFuncsCore.ExtractDecade(a1) => ???
      case MapFuncsCore.ExtractDayOfWeek(a1) => ???
      case MapFuncsCore.ExtractDayOfYear(a1) => ???
      case MapFuncsCore.ExtractEpoch(a1) => ???
      case MapFuncsCore.ExtractHour(a1) => ???
      case MapFuncsCore.ExtractIsoDayOfWeek(a1) => ???
      case MapFuncsCore.ExtractIsoYear(a1) => ???
      case MapFuncsCore.ExtractMicroseconds(a1) => ???
      case MapFuncsCore.ExtractMillennium(a1) => ???
      case MapFuncsCore.ExtractMilliseconds(a1) => ???
      case MapFuncsCore.ExtractMinute(a1) => ???
      case MapFuncsCore.ExtractMonth(a1) => ???
      case MapFuncsCore.ExtractQuarter(a1) => ???
      case MapFuncsCore.ExtractSecond(a1) => ???
      case MapFuncsCore.ExtractTimezone(a1) => ???
      case MapFuncsCore.ExtractTimezoneHour(a1) => ???
      case MapFuncsCore.ExtractTimezoneMinute(a1) => ???
      case MapFuncsCore.ExtractWeek(a1) => ???
      case MapFuncsCore.ExtractYear(a1) => ???
      case MapFuncsCore.Date(a1) => ???
      case MapFuncsCore.Time(a1) => ???
      case MapFuncsCore.Timestamp(a1) => ???
      case MapFuncsCore.Interval(a1) => ???
      case MapFuncsCore.StartOfDay(a1) => ???
      case MapFuncsCore.TemporalTrunc(part, a1) => ???
      case MapFuncsCore.TimeOfDay(a1) => ???
      case MapFuncsCore.ToTimestamp(a1) => ???
      case MapFuncsCore.Now() => ???

      case MapFuncsCore.TypeOf(a1) => ???

      case MapFuncsCore.Negate(a1) =>
        (Map1[Source1](a1, math.Negate): TransSpec1).point[F]
      case MapFuncsCore.Add(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Add.f2): TransSpec1).point[F]
      case MapFuncsCore.Multiply(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Mul.f2): TransSpec1).point[F]
      case MapFuncsCore.Subtract(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Sub.f2): TransSpec1).point[F]
      case MapFuncsCore.Divide(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Div.f2): TransSpec1).point[F]
      case MapFuncsCore.Modulo(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Mod.f2): TransSpec1).point[F]
      case MapFuncsCore.Power(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Pow.f2): TransSpec1).point[F]

      case MapFuncsCore.Not(a1) => ???
      case MapFuncsCore.Eq(a1, a2) =>
        (Equal[Source1](a1, a2): TransSpec1).point[F]
      case MapFuncsCore.Neq(a1, a2) => ???
      case MapFuncsCore.Lt(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Lt.f2): TransSpec1).point[F]
      case MapFuncsCore.Lte(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.LtEq.f2): TransSpec1).point[F]
      case MapFuncsCore.Gt(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Gt.f2): TransSpec1).point[F]
      case MapFuncsCore.Gte(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.GtEq.f2): TransSpec1).point[F]

      case MapFuncsCore.IfUndefined(a1, a2) => ???
      case MapFuncsCore.And(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.And.f2): TransSpec1).point[F]
      case MapFuncsCore.Or(a1, a2) =>
        (Map2[Source1](a1, a2, cake.Library.Infix.Or.f2): TransSpec1).point[F]
      case MapFuncsCore.Between(a1, a2, a3) => ???
      case MapFuncsCore.Cond(a1, a2, a3) => ???

      case MapFuncsCore.Within(a1, a2) => ???

      case MapFuncsCore.Lower(a1) =>
        (Map1[Source1](a1, cake.Library.toLowerCase.f1): TransSpec1).point[F]
      case MapFuncsCore.Upper(a1) =>
        (Map1[Source1](a1, cake.Library.toUpperCase.f1): TransSpec1).point[F]
      case MapFuncsCore.Bool(a1) => ???
      case MapFuncsCore.Integer(a1) => ???
      case MapFuncsCore.Decimal(a1) => ???
      case MapFuncsCore.Null(a1) => ???
      case MapFuncsCore.ToString(a1) => ???
      case MapFuncsCore.Search(a1, a2, a3) => ???
      case MapFuncsCore.Substring(string, from, count) => ???

      // FIXME detect constant cases so we don't have to always use the dynamic variants
      case MapFuncsCore.MakeArray(a1) =>
        (WrapArray[Source1](a1): TransSpec1).point[F]
      case MapFuncsCore.MakeMap(key, value) =>
        (WrapObjectDynamic[Source1](key, value): TransSpec1).point[F]
      case MapFuncsCore.ConcatArrays(a1, a2) =>
        (OuterArrayConcat[Source1](a1, a2): TransSpec1).point[F]
      case MapFuncsCore.ConcatMaps(a1, a2) =>
        (OuterObjectConcat[Source1](a1, a2): TransSpec1).point[F]
      case MapFuncsCore.ProjectIndex(src, index) =>
        (DerefArrayDynamic[Source1](src, index): TransSpec1).point[F]
      case MapFuncsCore.ProjectField(src, field) =>
        (DerefObjectDynamic[Source1](src, field): TransSpec1).point[F]
      case MapFuncsCore.DeleteField(src, field) => ???

      case MapFuncsCore.Meta(a1) => ???

      case MapFuncsCore.Range(from, to) => ???

      // FIXME if fallback is not undefined don't throw this away
      case MapFuncsCore.Guard(_, _, a2, _) => a2.point[F]
    }
  }
}
