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

import slamdata.Predef._
import matryoshka._
import matryoshka.data.Fix

import scalaz._
import scalaz.Leibniz.===
import quasar.{ejson, qscript}
import quasar.common.{JoinType, SortDir}
import quasar.std.TemporalPart
import quasar.ejson.EJson

object construction {
  final case class Func[T[_[_]]]()(implicit birec: BirecursiveT[T]) {
    private val json = ejson.Fixed[T[EJson]]
    private def rollCore[A](in: MapFuncCore[T, FreeMapA[T, A]]): FreeMapA[T, A] = Free.roll(MFC(in))
    private def rollDerived[A](in: MapFuncDerived[T, FreeMapA[T, A]]): FreeMapA[T, A] = Free.roll(MFD(in))
    def Constant[A](in: T[EJson]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Constant(in))
    def Undefined[A]: FreeMapA[T, A] =
      rollCore(MapFuncsCore.Undefined())
    def Now[A]: FreeMapA[T, A] =
      rollCore(MapFuncsCore.Now())
    def JoinSideName[A](sym: Symbol): FreeMapA[T, A] =
      rollCore(MapFuncsCore.JoinSideName(sym))
    def ExtractCentury[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractCentury(a1))
    def ExtractDayOfMonth[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractDayOfMonth(a1))
    def ExtractDecade[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractDecade(a1))
    def ExtractDayOfWeek[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractDayOfWeek(a1))
    def ExtractDayOfYear[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractDayOfYear(a1))
    def ExtractEpoch[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractEpoch(a1))
    def ExtractHour[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractHour(a1))
    def ExtractIsoDayOfWeek[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractIsoDayOfWeek(a1))
    def ExtractIsoYear[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractIsoYear(a1))
    def ExtractMicroseconds[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractMicroseconds(a1))
    def ExtractMillennium[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractMillennium(a1))
    def ExtractMilliseconds[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractMilliseconds(a1))
    def ExtractMinute[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractMinute(a1))
    def ExtractMonth[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractMonth(a1))
    def ExtractQuarter[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractQuarter(a1))
    def ExtractSecond[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractSecond(a1))
    def ExtractTimezone[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractTimezone(a1))
    def ExtractTimezoneHour[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractTimezoneHour(a1))
    def ExtractTimezoneMinute[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractTimezoneMinute(a1))
    def ExtractWeek[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractWeek(a1))
    def ExtractYear[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ExtractYear(a1))
    def Date[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Date(a1))
    def Time[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Time(a1))
    def Timestamp[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Timestamp(a1))
    def Interval[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Interval(a1))
    def StartOfDay[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.StartOfDay(a1))
    def TimeOfDay[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.TimeOfDay(a1))
    def ToTimestamp[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ToTimestamp(a1))
    def TypeOf[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.TypeOf(a1))
    def ToId[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ToId(a1))
    def Negate[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Negate(a1))
    def Not[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Not(a1))
    def Length[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Length(a1))
    def Lower[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Lower(a1))
    def Upper[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Upper(a1))
    def Bool[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Bool(a1))
    def Integer[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Integer(a1))
    def Decimal[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Decimal(a1))
    def Null[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Null(a1))
    def ToString[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ToString(a1))
    def MakeArray[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.MakeArray(a1))
    def Meta[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Meta(a1))
    def ProjectKey[A](src: FreeMapA[T, A], key: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ProjectKey(src, key))
    def ProjectKeyS[A](src: FreeMapA[T, A], key: String): FreeMapA[T, A] =
      ProjectKey(src, Constant(json.str(key)))
    def DeleteKey[A](src: FreeMapA[T, A], key: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.DeleteKey(src, key))
    def DeleteKeyS[A](src: FreeMapA[T, A], key: String): FreeMapA[T, A] =
      DeleteKey(src, Constant(json.str(key)))
    def MakeMap[A](key: FreeMapA[T, A], src: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.MakeMap(key, src))
    def MakeMapS[A](key: String, src: FreeMapA[T, A]): FreeMapA[T, A] =
      MakeMap(Constant(json.str(key)), src)
    def ProjectIndex[A](src: FreeMapA[T, A], index: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ProjectIndex(src, index))
    def ProjectIndexI[A](src: FreeMapA[T, A], index: Int): FreeMapA[T, A] =
      ProjectIndex(src, Constant(json.int(index)))
    def ConcatArrays[A](left: FreeMapA[T, A], right: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ConcatArrays(left, right))
    def ConcatMaps[A](left: FreeMapA[T, A], right: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.ConcatMaps(left, right))
    def Add[A](left: FreeMapA[T, A], right: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Add(left, right))
    def Multiply[A](left: FreeMapA[T, A], right: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Multiply(left, right))
    def Subtract[A](left: FreeMapA[T, A], right: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Subtract(left, right))
    def Divide[A](left: FreeMapA[T, A], right: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Divide(left, right))
    def Modulo[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Modulo(a1, a2))
    def Power[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Power(a1, a2))
    def Lt[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Lt(a1, a2))
    def Lte[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Lte(a1, a2))
    def Gt[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Gt(a1, a2))
    def Gte[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Gte(a1, a2))
    def IfUndefined[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.IfUndefined(a1, a2))
    def And[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.And(a1, a2))
    def Or[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Or(a1, a2))
    def TemporalTrunc[A](part: TemporalPart, a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.TemporalTrunc(part, a1))
    def Within[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Within(a1, a2))
    def Range[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Range(a1, a2))
    def Split[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Split(a1, a2))
    def Eq[A](left: FreeMapA[T, A], right: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Eq(left, right))
    def Neq[A](left: FreeMapA[T, A], right: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Neq(left, right))
    def Between[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A], a3: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Between(a1, a2, a3))
    def Cond[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A], a3: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Cond(a1, a2, a3))
    def Search[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A], a3: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Search(a1, a2, a3))
    def Substring[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A], a3: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Substring(a1, a2, a3))
    def Guard[A](a1: FreeMapA[T, A], tpe: quasar.Type, a2: FreeMapA[T, A], a3: FreeMapA[T, A]): FreeMapA[T, A] =
      rollCore(MapFuncsCore.Guard(a1, tpe, a2, a3))
    def Hole: FreeMap[T] = Free.pure(SrcHole)
    def LeftSide: JoinFunc[T] = Free.pure(qscript.LeftSide)
    def RightSide: JoinFunc[T] = Free.pure(qscript.RightSide)
    def ReduceIndex(i: Int \/ Int): FreeMapA[T, ReduceIndex] = Free.pure(qscript.ReduceIndex(i))

    def Abs[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollDerived(MapFuncsDerived.Abs(a1))
    def Ceil[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollDerived(MapFuncsDerived.Ceil(a1))
    def Floor[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollDerived(MapFuncsDerived.Floor(a1))
    def Trunc[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollDerived(MapFuncsDerived.Trunc(a1))
    def Round[A](a1: FreeMapA[T, A]): FreeMapA[T, A] =
      rollDerived(MapFuncsDerived.Round(a1))

    def FloorScale[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollDerived(MapFuncsDerived.FloorScale(a1, a2))
    def CeilScale[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollDerived(MapFuncsDerived.CeilScale(a1, a2))
    def RoundScale[A](a1: FreeMapA[T, A], a2: FreeMapA[T, A]): FreeMapA[T, A] =
      rollDerived(MapFuncsDerived.RoundScale(a1, a2))
  }

  final case class Dsl[T[_[_]], F[_], R](embed: F[R] => R)
                                        (implicit corec: CorecursiveT[T],
                                         injCore: Injectable.Aux[QScriptCore[T, ?], F],
                                         injTotal: Injectable.Aux[F, QScriptTotal[T, ?]]) {
    private def core(fr: QScriptCore[T, R]): R = embed(injCore.inject(fr))
    def Map(r: R, func: FreeMap[T]): R =
      core(qscript.Map[T, R](r, func))
    def LeftShift(src: R,
                  struct: FreeMap[T],
                  idStatus: IdStatus,
                  repair: JoinFunc[T]): R =
      core(qscript.LeftShift(src, struct, idStatus, repair))
    def Reduce(src: R,
               bucket: List[FreeMap[T]],
               reducers: List[ReduceFunc[FreeMap[T]]],
               repair: FreeMapA[T, ReduceIndex]): R =
      core(qscript.Reduce(src, bucket, reducers, repair))
    def Sort(src: R,
             bucket: List[FreeMap[T]],
             order: NonEmptyList[(FreeMap[T], SortDir)]): R =
      core(qscript.Sort(src, bucket, order))
    def Union(src: R,
              lBranch: Free[F, Hole],
              rBranch: Free[F, Hole]): R =
      core(qscript.Union(src, lBranch.mapSuspension(injTotal.inject), rBranch.mapSuspension(injTotal.inject)))
    def Filter(src: R,
               f: FreeMap[T]): R =
      core(qscript.Filter(src, f))
    def Subset(src: R,
               from: Free[F, Hole],
               op: SelectionOp,
               count: Free[F, Hole]): R =
      core(qscript.Subset(src, from.mapSuspension(injTotal.inject), op, count.mapSuspension(injTotal.inject)))
    def Unreferenced: R =
      core(qscript.Unreferenced())
    def BucketKey(src: R, value: FreeMap[T], name: FreeMap[T])(implicit F: Injectable.Aux[ProjectBucket[T, ?], F]): R =
      embed(F.inject(qscript.BucketKey(src, value, name)))
    def BucketIndex(src: R, value: FreeMap[T], index: FreeMap[T])(implicit F: Injectable.Aux[ProjectBucket[T, ?], F]): R =
      embed(F.inject(qscript.BucketIndex(src, value, index)))
    def ThetaJoin(src: R,
                  lBranch: Free[F, Hole],
                  rBranch: Free[F, Hole],
                  on: JoinFunc[T],
                  f: JoinType,
                  combine: JoinFunc[T])
                 (implicit F: Injectable.Aux[ThetaJoin[T, ?], F]): R =
      embed(F.inject(qscript.ThetaJoin(src, lBranch.mapSuspension(injTotal.inject), rBranch.mapSuspension(injTotal.inject), on, f, combine)))
    def EquiJoin(src: R,
                 lBranch: Free[F, Hole],
                 rBranch: Free[F, Hole],
                 key: List[(FreeMap[T], FreeMap[T])],
                 f: JoinType,
                 combine: JoinFunc[T])
                (implicit F: Injectable.Aux[EquiJoin[T, ?], F]): R =
      embed(F.inject(qscript.EquiJoin(src, lBranch.mapSuspension(injTotal.inject), rBranch.mapSuspension(injTotal.inject), key, f, combine)))
    def ShiftedRead[A](path: A,
                       idStatus: IdStatus)
                      (implicit F: Injectable.Aux[Const[ShiftedRead[A], ?], F]): R =
      embed(F.inject(Const(qscript.ShiftedRead(path, idStatus))))
    def Read[A](path: A)
               (implicit F: Injectable.Aux[Const[Read[A], ?], F]): R =
      embed(F.inject(Const(qscript.Read(path))))
    def Root(implicit F: Injectable.Aux[Const[DeadEnd, ?], F]): R =
      embed(F.inject(Const(qscript.Root)))
    def Hole(implicit ev: Free[F, Hole] === R): R =
      ev(Free.pure(SrcHole))
  }

  class Defaults[T[_[_]]: BirecursiveT, F[_]](implicit injCore: Injectable.Aux[QScriptCore[T, ?], F],
                                              injTotal: Injectable.Aux[F, QScriptTotal[T, ?]]) {
    val func: Func[T] = Func[T]
    val free: Dsl[T, F, Free[F, Hole]] = mkFree[T, F]
    val fix: Dsl[T, F, Fix[F]] = mkFix[T, F]
  }

  def mkDefaults[T[_[_]]: BirecursiveT, F[_]](implicit
                                              injCore: Injectable.Aux[QScriptCore[T, ?], F],
                                              injTotal: Injectable.Aux[F, QScriptTotal[T, ?]]): Defaults[T, F] =
    new Defaults[T, F]

  def mkFree[T[_[_]]: CorecursiveT, F[_]](implicit
                                          injCore: Injectable.Aux[QScriptCore[T, ?], F],
                                          injTotal: Injectable.Aux[F, QScriptTotal[T, ?]]): Dsl[T, F, Free[F, Hole]] =
    Dsl[T, F, Free[F, Hole]](Free.roll)

  def mkFix[T[_[_]]: CorecursiveT, F[_]](implicit
                                         injCore: Injectable.Aux[QScriptCore[T, ?], F],
                                         injTotal: Injectable.Aux[F, QScriptTotal[T, ?]]): Dsl[T, F, Fix[F]] =
    Dsl[T, F, Fix[F]](Fix(_))

  def mkGeneric[T[_[_]], F[_]: Functor](implicit corec: CorecursiveT[T],
                                        injCore: Injectable.Aux[QScriptCore[T, ?], F],
                                        injTotal: Injectable.Aux[F, QScriptTotal[T, ?]]): Dsl[T, F, T[F]] =
    Dsl[T, F, T[F]](corec.embedT(_))
}
