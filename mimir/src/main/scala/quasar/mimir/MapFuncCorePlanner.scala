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
import quasar.precog.common.{CBoolean, CLong, CPathField, CPathIndex, CString, RValue}
// import quasar.yggdrasil.TransSpecModule
// import quasar.yggdrasil.bytecode.JType
import quasar.yggdrasil.table.cf.util.Undefined

import matryoshka.{AlgebraM, RecursiveT}
import matryoshka.implicits._

import scalaz.Applicative
import scalaz.syntax.applicative._

final class MapFuncCorePlanner[T[_[_]]: RecursiveT, F[_]: Applicative]
  extends MapFuncPlanner[T, F, MapFuncCore[T, ?]] {

  def plan(cake: Precog): PlanApplicator[cake.type] =
    new PlanApplicatorCore(cake)

  final class PlanApplicatorCore[P <: Precog](override val cake: P)
    extends PlanApplicator[P](cake) {

    import cake.trans._
    import cake.Library._

    def apply[A <: SourceType](id: cake.trans.TransSpec[A]): AlgebraM[F, MapFuncCore[T, ?], TransSpec[A]] = {
      case MapFuncsCore.Undefined() =>
        (Map1[A](id, Undefined): TransSpec[A]).point[F]

      case MapFuncsCore.Constant(ejson) =>
        // EJson => Data => JValue => RValue => Table
        val data: Data = ejson.cata(Data.fromEJson)
        val jvalue: JValue = JValue.fromData(data)
        val rvalue: RValue = RValue.fromJValue(jvalue)
        transRValue(rvalue, id).point[F]

      case MapFuncsCore.JoinSideName(_) => ??? // should never be received

      case MapFuncsCore.Length(a1) => length.spec(a1).point[F]

      case MapFuncsCore.ExtractCentury(a1) => ???
      case MapFuncsCore.ExtractDayOfMonth(a1) =>
        (Map1[A](a1, cake.Library.DayOfMonth.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractDecade(a1) => ???
      case MapFuncsCore.ExtractDayOfWeek(a1) =>
        (Map1[A](a1, cake.Library.DayOfWeek.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractDayOfYear(a1) =>
        (Map1[A](a1, cake.Library.DayOfYear.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractEpoch(a1) => ???
      case MapFuncsCore.ExtractHour(a1) =>
        (Map1[A](a1, cake.Library.HourOfDay.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractIsoDayOfWeek(a1) =>
        (Map1[A](a1, cake.Library.DayOfWeek.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractIsoYear(a1) =>
        (Map1[A](a1, cake.Library.Year.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractMicroseconds(a1) => ???
      case MapFuncsCore.ExtractMillennium(a1) => ???
      case MapFuncsCore.ExtractMilliseconds(a1) => ???
      case MapFuncsCore.ExtractMinute(a1) =>
        (Map1[A](a1, cake.Library.MinuteOfHour.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractMonth(a1) =>
        (Map1[A](a1, cake.Library.MonthOfYear.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractQuarter(a1) =>
        (Map1[A](a1, cake.Library.QuarterOfYear.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractSecond(a1) => ??? // expects a decimal like 32.12383
      case MapFuncsCore.ExtractTimezone(a1) =>
        (Map1[A](a1, cake.Library.TimeZone.f1): TransSpec[A]).point[F]
      case MapFuncsCore.ExtractTimezoneHour(a1) => ???
      case MapFuncsCore.ExtractTimezoneMinute(a1) => ???
      case MapFuncsCore.ExtractWeek(a1) => ??? // week of year
      case MapFuncsCore.ExtractYear(a1) =>
        (Map1[A](a1, cake.Library.Year.f1): TransSpec[A]).point[F]
      case MapFuncsCore.Date(a1) =>
        (Map1[A](a1, cake.Library.Date.f1): TransSpec[A]).point[F]
      case MapFuncsCore.Time(a1) =>
        (Map1[A](a1, cake.Library.Time.f1): TransSpec[A]).point[F]
      case MapFuncsCore.Timestamp(a1) => ???
      case MapFuncsCore.Interval(a1) => ???
      case MapFuncsCore.StartOfDay(a1) => ???
      case MapFuncsCore.TemporalTrunc(part, a1) => ???
      case MapFuncsCore.TimeOfDay(a1) => ???
      case MapFuncsCore.ToTimestamp(a1) => ???
      case MapFuncsCore.Now() => ???

      case MapFuncsCore.TypeOf(a1) => ???

      case MapFuncsCore.Negate(a1) =>
        Unary.Neg.spec(a1).point[F]   // NB: don't use math.Negate here; it does weird things to booleans
      case MapFuncsCore.Add(a1, a2) =>
        Infix.Add.spec(a1, a2).point[F]
      case MapFuncsCore.Multiply(a1, a2) =>
        Infix.Mul.spec(a1, a2).point[F]
      case MapFuncsCore.Subtract(a1, a2) =>
        Infix.Sub.spec(a1, a2).point[F]
      case MapFuncsCore.Divide(a1, a2) =>
        Infix.Div.spec(a1, a2).point[F]
      case MapFuncsCore.Modulo(a1, a2) =>
        Infix.Mod.spec(a1, a2).point[F]
      case MapFuncsCore.Power(a1, a2) =>
        Infix.Pow.spec(a1, a2).point[F]

      case MapFuncsCore.Not(a1) =>
        Unary.Comp.spec(a1).point[F]
      case MapFuncsCore.Eq(a1, a2) =>
        (Equal[A](a1, a2): TransSpec[A]).point[F]
      case MapFuncsCore.Neq(a1, a2) =>
        Unary.Comp.spec(Equal[A](a1, a2)).point[F]
      case MapFuncsCore.Lt(a1, a2) =>
        Infix.Lt.spec(a1, a2).point[F]
      case MapFuncsCore.Lte(a1, a2) =>
        Infix.LtEq.spec(a1, a2).point[F]
      case MapFuncsCore.Gt(a1, a2) =>
        Infix.Gt.spec(a1, a2).point[F]
      case MapFuncsCore.Gte(a1, a2) =>
        Infix.GtEq.spec(a1, a2).point[F]

      case MapFuncsCore.IfUndefined(a1, a2) =>
        (DerefObjectStatic(
          OuterObjectConcat(    // this operation is right-biased, so we default to a1
            WrapObject(a2, "foo"),
            WrapObject(a1, "foo")),
          CPathField("foo")): TransSpec[A]).point[F]

      case MapFuncsCore.And(a1, a2) =>
        Infix.And.spec(a1, a2).point[F]
      case MapFuncsCore.Or(a1, a2) =>
        Infix.Or.spec(a1, a2).point[F]
      case MapFuncsCore.Between(a1, a2, a3) =>
        (MapN(OuterArrayConcat(WrapArray(a1), WrapArray(a2), WrapArray(a3)), between): TransSpec[A]).point[F]
      case MapFuncsCore.Cond(a1, a2, a3) =>
        (Cond(a1, a2, a3): TransSpec[A]).point[F]

      case MapFuncsCore.Within(a1, a2) => ???

      case MapFuncsCore.Lower(a1) =>
        toLowerCase.spec(a1).point[F]
      case MapFuncsCore.Upper(a1) =>
        toUpperCase.spec(a1).point[F]
      case MapFuncsCore.Bool(a1) =>
        readBoolean.spec[A](a1).point[F]
      case MapFuncsCore.Integer(a1) =>
        readInteger.spec[A](a1).point[F]
      case MapFuncsCore.Decimal(a1) =>
        readDecimal.spec[A](a1).point[F]
      case MapFuncsCore.Null(a1) =>
        readNull.spec[A](a1).point[F]
      case MapFuncsCore.ToString(a1) =>
        convertToString.spec[A](a1).point[F]

      // significantly faster fast path
      case MapFuncsCore.Search(src, ConstLiteral(CString(pattern), _), ConstLiteral(CBoolean(flag), _)) =>
        search(pattern, flag).spec[A](src).point[F]

      // this case is hideously slow; hopefully we don't see it too often
      case MapFuncsCore.Search(src, pattern, flag) =>
        (MapN((OuterArrayConcat(WrapArray(src), WrapArray(pattern), WrapArray(flag))), searchDynamic): TransSpec[A]).point[F]

      case MapFuncsCore.Substring(string, from, count) =>
        val args = OuterArrayConcat(WrapArray(string), WrapArray(from), WrapArray(count))
        (MapN(args, substring): TransSpec[A]).point[F]

      case MapFuncsCore.MakeArray(a1) =>
        (WrapArray[A](a1): TransSpec[A]).point[F]
      case MapFuncsCore.MakeMap(ConstLiteral(CString(key), _), value) =>
        (WrapObject[A](value, key): TransSpec[A]).point[F]
      case MapFuncsCore.MakeMap(key, value) =>
        (WrapObjectDynamic[A](key, value): TransSpec[A]).point[F]
      case MapFuncsCore.ConcatArrays(a1, a2) =>
        (OuterArrayConcat[A](a1, a2): TransSpec[A]).point[F]
      case MapFuncsCore.ConcatMaps(a1, a2) =>
        (OuterObjectConcat[A](a1, a2): TransSpec[A]).point[F]
      case MapFuncsCore.ProjectIndex(src, ConstLiteral(CLong(index), _)) =>
        (DerefArrayStatic[A](src, CPathIndex(index.toInt)): TransSpec[A]).point[F]
      case MapFuncsCore.ProjectIndex(src, index) =>
        (DerefArrayDynamic[A](src, index): TransSpec[A]).point[F]
      case MapFuncsCore.ProjectField(src, ConstLiteral(CString(field), _)) =>
        (DerefObjectStatic[A](src, CPathField(field)): TransSpec[A]).point[F]
      case MapFuncsCore.ProjectField(src, field) =>
        (DerefObjectDynamic[A](src, field): TransSpec[A]).point[F]

      case MapFuncsCore.DeleteField(src, ConstLiteral(CString(key), _)) =>
        (ObjectDelete[A](src, Set(CPathField(key))): TransSpec[A]).point[F]

      // mimir doesn't have a way to implement this
      case MapFuncsCore.DeleteField(src, field) => ???

      case MapFuncsCore.Meta(a1) => ???

      // this returns rows, rather than array; literally cannot be implemented in mimir at present
      case MapFuncsCore.Range(from, to) => ???

      // rely on implicit guard
      // this really should be restricted to undefined only, but that breaks some join/ integration tests
      case MapFuncsCore.Guard(_, _, a2, _ /*Map1(_, Undefined)*/) =>
        a2.point[F]

      /*case MapFuncsCore.Guard(src, tpe, a2, Map1(_, Undefined)) =>
        (FilterDefined(a2, Typed(src, JType.fromType(tpe)), TransSpecModule.AllDefined): TransSpec[A]).point[F]*/

      // FIXME not sure exactly how to implement this...
      // case MapFuncsCore.Guard(_, _, _, _) => ???
    }
  }
}
