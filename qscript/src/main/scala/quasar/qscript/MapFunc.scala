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

package quasar.qscript

import quasar._
import quasar.qscript.{MapFuncsCore => C, MapFuncsDerived => D}
import quasar.std.StdLib._
import quasar.contrib.iota.{:<<:, ACopK}

object MapFunc {
  def translateNullaryMapping[T[_[_]], MF[a] <: ACopK[a] , A]
      (implicit MFC: MapFuncCore[T, ?] :<<: MF)
      : scala.PartialFunction[NullaryFunc, MF[A]] = {
    case date.Now => MFC(C.Now())
    case date.NowTime => MFC(C.NowTime())
    case date.NowDate => MFC(C.NowDate())
    case date.CurrentTimeZone => MFC(C.CurrentTimeZone())
  }

  def translateUnaryMapping[T[_[_]], MF[a] <: ACopK[a], A]
      (implicit MFC: MapFuncCore[T, ?] :<<: MF, MFD: MapFuncDerived[T, ?] :<<: MF)
      : scala.PartialFunction[UnaryFunc, A => MF[A]] = {
    case date.ExtractCentury => a => MFC(C.ExtractCentury(a))
    case date.ExtractDayOfMonth => a => MFC(C.ExtractDayOfMonth(a))
    case date.ExtractDecade => a => MFC(C.ExtractDecade(a))
    case date.ExtractDayOfWeek => a => MFC(C.ExtractDayOfWeek(a))
    case date.ExtractDayOfYear => a => MFC(C.ExtractDayOfYear(a))
    case date.ExtractEpoch => a => MFC(C.ExtractEpoch(a))
    case date.ExtractHour => a => MFC(C.ExtractHour(a))
    case date.ExtractIsoDayOfWeek => a => MFC(C.ExtractIsoDayOfWeek(a))
    case date.ExtractIsoYear => a => MFC(C.ExtractIsoYear(a))
    case date.ExtractMicrosecond => a => MFC(C.ExtractMicrosecond(a))
    case date.ExtractMillennium => a => MFC(C.ExtractMillennium(a))
    case date.ExtractMillisecond => a => MFC(C.ExtractMillisecond(a))
    case date.ExtractMinute => a => MFC(C.ExtractMinute(a))
    case date.ExtractMonth => a => MFC(C.ExtractMonth(a))
    case date.ExtractQuarter => a => MFC(C.ExtractQuarter(a))
    case date.ExtractSecond => a => MFC(C.ExtractSecond(a))
    case date.ExtractTimeZone => a => MFC(C.ExtractTimeZone(a))
    case date.ExtractTimeZoneHour => a => MFC(C.ExtractTimeZoneHour(a))
    case date.ExtractTimeZoneMinute => a => MFC(C.ExtractTimeZoneMinute(a))
    case date.ExtractWeek => a => MFC(C.ExtractWeek(a))
    case date.ExtractYear => a => MFC(C.ExtractYear(a))
    case date.OffsetDateTime => a => MFC(C.OffsetDateTime(a))
    case date.OffsetTime => a => MFC(C.OffsetTime(a))
    case date.OffsetDate => a => MFC(C.OffsetDate(a))
    case date.LocalDateTime => a => MFC(C.LocalDateTime(a))
    case date.LocalTime => a => MFC(C.LocalTime(a))
    case date.LocalDate => a => MFC(C.LocalDate(a))
    case date.Interval => a => MFC(C.Interval(a))
    case date.StartOfDay => a => MFC(C.StartOfDay(a))
    case date.TimeOfDay => a => MFC(C.TimeOfDay(a))
    case date.ToTimestamp => a => MFC(C.ToTimestamp(a))
    case date.ToLocal => a => MFC(C.ToLocal(a))
    case identity.TypeOf => a => MFC(C.TypeOf(a))
    case math.Abs => a => MFD(D.Abs(a))
    case math.Ceil => a => MFD(D.Ceil(a))
    case math.Floor => a => MFD(D.Floor(a))
    case math.Negate => a => MFC(C.Negate(a))
    case math.Trunc => a => MFD(D.Trunc(a))
    case math.Round => a => MFD(D.Round(a))
    case relations.Not => a => MFC(C.Not(a))
    case string.Length => a => MFC(C.Length(a))
    case string.Lower => a => MFC(C.Lower(a))
    case string.Upper => a => MFC(C.Upper(a))
    case string.Boolean => a => MFC(C.Bool(a))
    case string.Integer => a => MFC(C.Integer(a))
    case string.Decimal => a => MFC(C.Decimal(a))
    case string.Number => a => MFC(C.Number(a))
    case string.Null => a => MFC(C.Null(a))
    case string.ToString => a => MFC(C.ToString(a))
    case structural.MakeArray => a => MFC(C.MakeArray(a))
    case structural.Meta => a => MFC(C.Meta(a))
  }

  def translateBinaryMapping[T[_[_]], MF[a] <: ACopK[a], A]
      (implicit MFC: MapFuncCore[T, ?] :<<: MF, MFD: MapFuncDerived[T, ?] :<<: MF)
      : scala.PartialFunction[BinaryFunc, (A, A) => MF[A]] = {
    // NB: ArrayLength takes 2 params because of SQL, but we really don’t care
    //     about the second. And it shouldn’t even have two in LP.
    case array.ArrayLength => (a1, a2) => MFC(C.Length(a1))
    case date.SetTimeZone => (a1, a2) => MFC(C.SetTimeZone(a1, a2))
    case date.SetTimeZoneHour => (a1, a2) => MFC(C.SetTimeZoneHour(a1, a2))
    case date.SetTimeZoneMinute => (a1, a2) => MFC(C.SetTimeZoneMinute(a1, a2))
    case math.Add => (a1, a2) => MFC(C.Add(a1, a2))
    case math.Multiply => (a1, a2) => MFC(C.Multiply(a1, a2))
    case math.Subtract => (a1, a2) => MFC(C.Subtract(a1, a2))
    case math.Divide => (a1, a2) => MFC(C.Divide(a1, a2))
    case math.Modulo => (a1, a2) => MFC(C.Modulo(a1, a2))
    case math.Power => (a1, a2) => MFC(C.Power(a1, a2))
    case math.CeilScale => (a1, a2) => MFD(D.CeilScale(a1, a2))
    case math.FloorScale => (a1, a2) => MFD(D.FloorScale(a1, a2))
    case math.RoundScale => (a1, a2) => MFD(D.RoundScale(a1, a2))
    case relations.Eq => (a1, a2) => MFC(C.Eq(a1, a2))
    case relations.Neq => (a1, a2) => MFC(C.Neq(a1, a2))
    case relations.Lt => (a1, a2) => MFC(C.Lt(a1, a2))
    case relations.Lte => (a1, a2) => MFC(C.Lte(a1, a2))
    case relations.Gt => (a1, a2) => MFC(C.Gt(a1, a2))
    case relations.Gte => (a1, a2) => MFC(C.Gte(a1, a2))
    case relations.IfUndefined => (a1, a2) => MFC(C.IfUndefined(a1, a2))
    case relations.And => (a1, a2) => MFC(C.And(a1, a2))
    case relations.Or => (a1, a2) => MFC(C.Or(a1, a2))
    case set.Range => (a1, a2) => MFC(C.Range(a1, a2))
    case set.Within => (a1, a2) => MFC(C.Within(a1, a2))
    case string.Split => (a1, a2) => MFC(C.Split(a1, a2))
    case structural.MakeMap => (a1, a2) => MFC(C.MakeMap(a1, a2))
    case structural.MapConcat => (a1, a2) => MFC(C.ConcatMaps(a1, a2))
    case structural.ArrayProject => (a1, a2) => MFC(C.ProjectIndex(a1, a2))
    case structural.MapProject => (a1, a2) => MFC(C.ProjectKey(a1, a2))
    case structural.DeleteKey => (a1, a2) => MFC(C.DeleteKey(a1, a2))
    case string.Concat
       | structural.ArrayConcat
       | structural.ConcatOp => (a1, a2) => MFC(C.ConcatArrays(a1, a2))
  }

  def translateTernaryMapping[T[_[_]], MF[a] <: ACopK[a], A]
      (implicit MFC: MapFuncCore[T, ?] :<<: MF)
      : scala.PartialFunction[TernaryFunc, (A, A, A) => MF[A]] = {
    case relations.Between => (a1, a2, a3) => MFC(C.Between(a1, a2, a3))
    case relations.Cond    => (a1, a2, a3) => MFC(C.Cond(a1, a2, a3))
    case string.Search     => (a1, a2, a3) => MFC(C.Search(a1, a2, a3))
    case string.Like       => (a1, a2, a3) => MFC(C.Like(a1, a2, a3))
    case string.Substring  => (a1, a2, a3) => MFC(C.Substring(a1, a2, a3))
  }
}
