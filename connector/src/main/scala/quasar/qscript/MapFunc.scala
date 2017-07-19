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

package quasar.qscript

import quasar._
import quasar.qscript.{MapFuncsCore => C, MapFuncsDerived => D}
import quasar.std.StdLib._

import scalaz._

object MapFunc {
  def translateNullaryMapping[T[_[_]], MF[_], A]
      (implicit MFC: MapFuncCore[T, ?] :<: MF)
      : NullaryFunc => MF[A] = {
    case date.Now => MFC(C.Now())
  }

  def translateUnaryMapping[T[_[_]], MF[_], A]
      (implicit MFC: MapFuncCore[T, ?] :<: MF, MFD: MapFuncDerived[T, ?] :<: MF)
      : UnaryFunc => A => MF[A] = {
    case date.ExtractCentury => a => MFC(C.ExtractCentury(a))
    case date.ExtractDayOfMonth => a => MFC(C.ExtractDayOfMonth(a))
    case date.ExtractDecade => a => MFC(C.ExtractDecade(a))
    case date.ExtractDayOfWeek => a => MFC(C.ExtractDayOfWeek(a))
    case date.ExtractDayOfYear => a => MFC(C.ExtractDayOfYear(a))
    case date.ExtractEpoch => a => MFC(C.ExtractEpoch(a))
    case date.ExtractHour => a => MFC(C.ExtractHour(a))
    case date.ExtractIsoDayOfWeek => a => MFC(C.ExtractIsoDayOfWeek(a))
    case date.ExtractIsoYear => a => MFC(C.ExtractIsoYear(a))
    case date.ExtractMicroseconds => a => MFC(C.ExtractMicroseconds(a))
    case date.ExtractMillennium => a => MFC(C.ExtractMillennium(a))
    case date.ExtractMilliseconds => a => MFC(C.ExtractMilliseconds(a))
    case date.ExtractMinute => a => MFC(C.ExtractMinute(a))
    case date.ExtractMonth => a => MFC(C.ExtractMonth(a))
    case date.ExtractQuarter => a => MFC(C.ExtractQuarter(a))
    case date.ExtractSecond => a => MFC(C.ExtractSecond(a))
    case date.ExtractTimezone => a => MFC(C.ExtractTimezone(a))
    case date.ExtractTimezoneHour => a => MFC(C.ExtractTimezoneHour(a))
    case date.ExtractTimezoneMinute => a => MFC(C.ExtractTimezoneMinute(a))
    case date.ExtractWeek => a => MFC(C.ExtractWeek(a))
    case date.ExtractYear => a => MFC(C.ExtractYear(a))
    case date.Date => a => MFC(C.Date(a))
    case date.Time => a => MFC(C.Time(a))
    case date.Timestamp => a => MFC(C.Timestamp(a))
    case date.Interval => a => MFC(C.Interval(a))
    case date.StartOfDay => a => MFC(C.StartOfDay(a))
    case date.TimeOfDay => a => MFC(C.TimeOfDay(a))
    case date.ToTimestamp => a => MFC(C.ToTimestamp(a))
    case identity.TypeOf => a => MFC(C.TypeOf(a))
    case math.Abs => a => MFD(D.Abs(a))
    case math.Ceil => a => MFD(D.Ceil(a))
    case math.Floor => a => MFD(D.Floor(a))
    case math.Negate => a => MFC(C.Negate(a))
    case math.Trunc => a => MFD(D.Trunc(a))
    case relations.Not => a => MFC(C.Not(a))
    case string.Length => a => MFC(C.Length(a))
    case string.Lower => a => MFC(C.Lower(a))
    case string.Upper => a => MFC(C.Upper(a))
    case string.Boolean => a => MFC(C.Bool(a))
    case string.Integer => a => MFC(C.Integer(a))
    case string.Decimal => a => MFC(C.Decimal(a))
    case string.Null => a => MFC(C.Null(a))
    case string.ToString => a => MFC(C.ToString(a))
    case structural.MakeArray => a => MFC(C.MakeArray(a))
    case structural.Meta => a => MFC(C.Meta(a))
  }

  def translateBinaryMapping[T[_[_]], MF[_], A]
      (implicit MFC: MapFuncCore[T, ?] :<: MF)
      : BinaryFunc => (A, A) => MF[A] = {
    // NB: ArrayLength takes 2 params because of SQL, but we really don’t care
    //     about the second. And it shouldn’t even have two in LP.
    case array.ArrayLength => (a1, a2) => MFC(C.Length(a1))
    case math.Add => (a1, a2) => MFC(C.Add(a1, a2))
    case math.Multiply => (a1, a2) => MFC(C.Multiply(a1, a2))
    case math.Subtract => (a1, a2) => MFC(C.Subtract(a1, a2))
    case math.Divide => (a1, a2) => MFC(C.Divide(a1, a2))
    case math.Modulo => (a1, a2) => MFC(C.Modulo(a1, a2))
    case math.Power => (a1, a2) => MFC(C.Power(a1, a2))
    case relations.Eq => (a1, a2) => MFC(C.Eq(a1, a2))
    case relations.Neq => (a1, a2) => MFC(C.Neq(a1, a2))
    case relations.Lt => (a1, a2) => MFC(C.Lt(a1, a2))
    case relations.Lte => (a1, a2) => MFC(C.Lte(a1, a2))
    case relations.Gt => (a1, a2) => MFC(C.Gt(a1, a2))
    case relations.Gte => (a1, a2) => MFC(C.Gte(a1, a2))
    case relations.IfUndefined => (a1, a2) => MFC(C.IfUndefined(a1, a2))
    case relations.And => (a1, a2) => MFC(C.And(a1, a2))
    case relations.Or => (a1, a2) => MFC(C.Or(a1, a2))
    case set.Within => (a1, a2) => MFC(C.Within(a1, a2))
    case structural.MakeObject => (a1, a2) => MFC(C.MakeMap(a1, a2))
    case structural.ObjectConcat => (a1, a2) => MFC(C.ConcatMaps(a1, a2))
    case structural.ArrayProject => (a1, a2) => MFC(C.ProjectIndex(a1, a2))
    case structural.ObjectProject => (a1, a2) => MFC(C.ProjectField(a1, a2))
    case structural.DeleteField => (a1, a2) => MFC(C.DeleteField(a1, a2))
    case string.Concat
       | structural.ArrayConcat
       | structural.ConcatOp => (a1, a2) => MFC(C.ConcatArrays(a1, a2))
  }

  def translateTernaryMapping[T[_[_]], MF[_], A]
      (implicit MFC: MapFuncCore[T, ?] :<: MF)
      : TernaryFunc => (A, A, A) => MF[A] = {
    case relations.Between => (a1, a2, a3) => MFC(C.Between(a1, a2, a3))
    case relations.Cond    => (a1, a2, a3) => MFC(C.Cond(a1, a2, a3))
    case string.Search     => (a1, a2, a3) => MFC(C.Search(a1, a2, a3))
    case string.Substring  => (a1, a2, a3) => MFC(C.Substring(a1, a2, a3))
  }
}
