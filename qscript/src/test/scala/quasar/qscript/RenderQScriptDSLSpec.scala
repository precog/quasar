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

import slamdata.Predef.{Eq => _, _}
import quasar.Type
import quasar.fp._
import quasar.contrib.iota._
import matryoshka.Delay
import matryoshka.data.{Fix, freeEqual, freeShow}
import matryoshka.{delayEqual, delayShow}
import org.specs2.execute.Result
import quasar.ejson.{EJson, Extension}
import quasar.time.TemporalPart
import quasar.qscript.RenderQScriptDSL.RenderQScriptDSL
import quasar.common.{JoinType, SortDir}
import pathy.Path._
import quasar.contrib.pathy._

import scalaz.{Equal, NonEmptyList, Show}
import scalaz.syntax.show._
import scalaz.std.list._
import scalaz.syntax.either._

class RenderQScriptDSLSpec extends quasar.Qspec with QScriptHelpers {

  import scala.tools.reflect.ToolBox

  val cm = scala.reflect.runtime.currentMirror
  val tb = cm.mkToolBox()
  // there's a lot of injectable instances in here that are *not* necessary in user code.
  // as far as I can tell, they need to be here because partial unification isn't enabled in the runtime compiler.
  // nor is kind-projector. go figure.
  val prefix =
    """
      |import quasar.Type
      |import quasar.common.{JoinType, SortDir}
      |import quasar.common.data.Data
      |import quasar.time.TemporalPart
      |import quasar.qscript._
      |import quasar.ejson.{EJson, Fixed}
      |import quasar.contrib.iota.{copkEqual, copkTraverse}
      |import pathy.Path._
      |import quasar.contrib.pathy._
      |import matryoshka.data.Fix
      |import scalaz.{Const, Inject, NonEmptyList}
      |import scalaz.syntax.either._
      |import quasar.fp.Injectable
      |type QT[A] = QScriptTotal[Fix, A]
      |type DET[A] = Const[DeadEnd, A]
      |type RTD[A] = Const[Read[ADir], A]
      |type RTF[A] = Const[Read[AFile], A]
      |type SRTD[A] = Const[ShiftedRead[ADir], A]
      |type SRTF[A] = Const[ShiftedRead[AFile], A]
      |implicit def idInj = Injectable.id[QT]
      |implicit def QCT = Injectable.inject(QScriptHelpers.QCT)
      |implicit def TJT = Injectable.inject(QScriptHelpers.TJT)
      |implicit def EJT = Injectable.inject(QScriptHelpers.EJT)
      |implicit def PBT = Injectable.inject(QScriptHelpers.PBT)
      |implicit def RTD = Injectable.inject[RTD, QT](QScriptHelpers.RTD)
      |implicit def RTF = Injectable.inject[RTF, QT](QScriptHelpers.RTF)
      |implicit def SRTD = Injectable.inject[SRTD, QT](QScriptHelpers.SRTD)
      |implicit def SRTF = Injectable.inject[SRTF, QT](QScriptHelpers.SRTF)
      |implicit def DET = Injectable.inject[DET, QT](QScriptHelpers.DET)
      |val dsl = construction.mkDefaults[Fix, QT]
      |val json = Fixed[Fix[EJson]]
      |import dsl._
    """.stripMargin

  def testDSL[A: Equal: Show](in: List[A], rend: RenderQScriptDSL[A]): Result = {
    val combined = in.map(rend("fix", _).shows).mkString("List(\n", ",\n", "\n)")
    (tb.eval(tb.parse(prefix + combined)).asInstanceOf[List[A]] must equal(in)).toResult
  }

  import qstdsl._

  def recFreeMaps: List[RecFreeMap] = {
    val h = recFunc.Hole

    List(
      recFunc.Constant(EJson.nul[Fix[EJson]]),
      recFunc.Now,
      recFunc.NowDate,
      recFunc.NowTime,
      recFunc.CurrentTimeZone,
      recFunc.ToLocal(h),
      recFunc.Undefined,
      recFunc.Length(h),
      recFunc.SetTimeZone(h, h),
      recFunc.SetTimeZoneMinute(h, h),
      recFunc.SetTimeZoneHour(h, h),
      recFunc.ExtractCentury(h),
      recFunc.ExtractDayOfMonth(h),
      recFunc.ExtractDecade(h),
      recFunc.ExtractDayOfWeek(h),
      recFunc.ExtractDayOfYear(h),
      recFunc.ExtractEpoch(h),
      recFunc.ExtractHour(h),
      recFunc.ExtractIsoDayOfWeek(h),
      recFunc.ExtractIsoYear(h),
      recFunc.ExtractMicrosecond(h),
      recFunc.ExtractMillennium(h),
      recFunc.ExtractMillisecond(h),
      recFunc.ExtractMinute(h),
      recFunc.ExtractMonth(h),
      recFunc.ExtractQuarter(h),
      recFunc.ExtractSecond(h),
      recFunc.ExtractTimeZone(h),
      recFunc.ExtractTimeZoneHour(h),
      recFunc.ExtractTimeZoneMinute(h),
      recFunc.ExtractWeek(h),
      recFunc.ExtractYear(h),
      recFunc.LocalDateTime(h),
      recFunc.LocalDate(h),
      recFunc.LocalTime(h),
      recFunc.OffsetDateTime(h),
      recFunc.OffsetDate(h),
      recFunc.OffsetTime(h),
      recFunc.Interval(h),
      recFunc.StartOfDay(h),
      recFunc.TemporalTrunc(TemporalPart.Day, h),
      recFunc.TimeOfDay(h),
      recFunc.ToTimestamp(h),
      recFunc.TypeOf(h),
      recFunc.Negate(h),
      recFunc.MakeArray(h),
      recFunc.Lower(h),
      recFunc.Upper(h),
      recFunc.Bool(h),
      recFunc.Integer(h),
      recFunc.Decimal(h),
      recFunc.Number(h),
      recFunc.Null(h),
      recFunc.ToString(h),
      recFunc.Not(h),
      recFunc.Meta(h),
      recFunc.Add(h, h),
      recFunc.Multiply(h, h),
      recFunc.Subtract(h, h),
      recFunc.Divide(h, h),
      recFunc.Modulo(h, h),
      recFunc.Power(h, h),
      recFunc.Eq(h, h),
      recFunc.Neq(h, h),
      recFunc.Lt(h, h),
      recFunc.Lte(h, h),
      recFunc.Gt(h, h),
      recFunc.Gte(h, h),
      recFunc.IfUndefined(h, h),
      recFunc.And(h, h),
      recFunc.Or(h, h),
      recFunc.Within(h, h),
      recFunc.Split(h, h),
      recFunc.MakeMap(h, h),
      recFunc.ConcatArrays(h, h),
      recFunc.ConcatMaps(h, h),
      recFunc.ProjectIndex(h, h),
      recFunc.ProjectKey(h, h),
      recFunc.DeleteKey(h, h),
      recFunc.Range(h, h),
      recFunc.Between(h, h, h),
      recFunc.Guard(h, Type.Top, h, h),
      recFunc.Cond(h, h, h),
      recFunc.Search(h, h, h),
      recFunc.Like(h, h, h),
      recFunc.Substring(h, h, h),
      recFunc.Abs(h),
      recFunc.Ceil(h),
      recFunc.Floor(h),
      recFunc.Trunc(h),
      recFunc.Round(h),
      recFunc.FloorScale(h, h),
      recFunc.CeilScale(h, h),
      recFunc.RoundScale(h, h))
  }

  def freeMaps: List[FreeMap] = {
    val h = func.Hole

    List(
      func.Constant(EJson.nul[Fix[EJson]]),
      func.Now,
      func.NowDate,
      func.NowTime,
      func.CurrentTimeZone,
      func.ToLocal(h),
      func.Undefined,
      func.Length(h),
      func.SetTimeZone(h, h),
      func.SetTimeZoneMinute(h, h),
      func.SetTimeZoneHour(h, h),
      func.ExtractCentury(h),
      func.ExtractDayOfMonth(h),
      func.ExtractDecade(h),
      func.ExtractDayOfWeek(h),
      func.ExtractDayOfYear(h),
      func.ExtractEpoch(h),
      func.ExtractHour(h),
      func.ExtractIsoDayOfWeek(h),
      func.ExtractIsoYear(h),
      func.ExtractMicrosecond(h),
      func.ExtractMillennium(h),
      func.ExtractMillisecond(h),
      func.ExtractMinute(h),
      func.ExtractMonth(h),
      func.ExtractQuarter(h),
      func.ExtractSecond(h),
      func.ExtractTimeZone(h),
      func.ExtractTimeZoneHour(h),
      func.ExtractTimeZoneMinute(h),
      func.ExtractWeek(h),
      func.ExtractYear(h),
      func.LocalDateTime(h),
      func.LocalDate(h),
      func.LocalTime(h),
      func.OffsetDateTime(h),
      func.OffsetDate(h),
      func.OffsetTime(h),
      func.Interval(h),
      func.StartOfDay(h),
      func.TemporalTrunc(TemporalPart.Day, h),
      func.TimeOfDay(h),
      func.ToTimestamp(h),
      func.TypeOf(h),
      func.Negate(h),
      func.MakeArray(h),
      func.Lower(h),
      func.Upper(h),
      func.Bool(h),
      func.Integer(h),
      func.Decimal(h),
      func.Number(h),
      func.Null(h),
      func.ToString(h),
      func.Not(h),
      func.Meta(h),
      func.Add(h, h),
      func.Multiply(h, h),
      func.Subtract(h, h),
      func.Divide(h, h),
      func.Modulo(h, h),
      func.Power(h, h),
      func.Eq(h, h),
      func.Neq(h, h),
      func.Lt(h, h),
      func.Lte(h, h),
      func.Gt(h, h),
      func.Gte(h, h),
      func.IfUndefined(h, h),
      func.And(h, h),
      func.Or(h, h),
      func.Within(h, h),
      func.Split(h, h),
      func.MakeMap(h, h),
      func.ConcatArrays(h, h),
      func.ConcatMaps(h, h),
      func.ProjectIndex(h, h),
      func.ProjectKey(h, h),
      func.DeleteKey(h, h),
      func.Range(h, h),
      func.Between(h, h, h),
      func.Guard(h, Type.Top, h, h),
      func.Cond(h, h, h),
      func.Search(h, h, h),
      func.Like(h, h, h),
      func.Substring(h, h, h),
      func.Abs(h),
      func.Ceil(h),
      func.Floor(h),
      func.Trunc(h),
      func.Round(h),
      func.FloorScale(h, h),
      func.CeilScale(h, h),
      func.RoundScale(h, h))
  }

  def freeQScripts = {
    val h = free.Hole
    List[FreeQS](
      free.Hole,
      free.Map(h, recFunc.Undefined),
      free.LeftShift(h, recFunc.Undefined, ExcludeId, ShiftType.Array, OnUndefined.Omit, func.LeftSide),
      free.Reduce(h, Nil, Nil, func.ReduceIndex(1.right)),
      free.Sort(h, Nil, NonEmptyList((func.Hole, SortDir.Ascending))),
      free.Union(h, h, h),
      free.Filter(h, recFunc.Hole),
      free.Subset(h, h, Take, h),
      free.Subset(h, h, Drop, h),
      free.Subset(h, h, Sample, h),
      free.Unreferenced,
      free.BucketKey(h, func.Hole, func.Hole),
        free.BucketIndex(h, func.Hole, func.Hole),
      free.ThetaJoin(h, h, h, func.LeftSide, JoinType.Inner, func.LeftSide),
      free.EquiJoin(h, h, h, List((func.Hole, func.Hole)), JoinType.Inner, func.LeftSide),
      free.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), ExcludeId),
      free.ShiftedRead[ADir](rootDir </> dir("db"), ExcludeId),
      free.Read[AFile](rootDir </> dir("db") </> file("zips")),
      free.Read[ADir](rootDir </> dir("db")),
      free.Root)
  }

  def fixQScripts = {
    val u = fix.Unreferenced
    val h = free.Hole
    List[Fix[QST]](
      fix.Map(u, recFunc.Undefined),
      fix.LeftShift(u, recFunc.Undefined, ExcludeId, ShiftType.Array, OnUndefined.Omit, func.LeftSide),
      fix.Reduce(u, Nil, Nil, func.ReduceIndex(1.right)),
      fix.Sort(u, Nil, NonEmptyList((func.Hole, SortDir.Ascending), (func.Hole, SortDir.Descending))),
      fix.Union(u, free.Hole, free.Hole),
      fix.Filter(u, recFunc.Hole),
      fix.Subset(u, free.Hole, Take, free.Hole),
      fix.Unreferenced,
      fix.BucketKey(u, func.Hole, func.Hole),
      fix.BucketIndex(u, func.Hole, func.Hole),
      fix.ThetaJoin(u, h, h, func.LeftSide, JoinType.Inner, func.LeftSide),
      fix.EquiJoin(u, h, h, List((func.Hole, func.Hole)), JoinType.Inner, func.LeftSide),
      fix.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), ExcludeId),
      fix.ShiftedRead[ADir](rootDir </> dir("db"), ExcludeId),
      fix.Read[AFile](rootDir </> dir("db") </> file("zips")),
      fix.Read[ADir](rootDir </> dir("db")),
      fix.Root)
  }

  def reduceFuncs = {
    val h = func.Hole
    import ReduceFuncs._
    List[ReduceFunc[FreeMap]](
      Count(h), Sum(h),
      Min(h), Max(h),
      Avg(h), Arbitrary(h),
      First(h), Last(h),
      UnshiftArray(h), UnshiftMap(h, h))
  }

  def joinSides = {
    List[JoinFunc](func.LeftSide, func.RightSide)
  }

  def ejsons = {
    List[Fix[EJson]](
      json.arr(List(json.nul(), json.nul())),
      json.map(List((json.nul(), json.nul()))),
      json.bool(true),
      json.char('c'),
      json.dec(1.1),
      json.int(1),
      json.meta(json.nul(), json.nul()),
      json.nul(),
      json.str("test"))
  }

  "rendered dsl should represent the tree rendered" >> {
    import RenderQScriptDSL._

    "QScript" >> {
      "Fix" >> testDSL(fixQScripts, fixQSRender[Fix])
      "Free" >> testDSL(freeQScripts, freeQSRender[Fix])

      "FreeMap" >> testDSL(freeMaps, freeMapRender[Fix])
      "RecFreeMap" >> testDSL(recFreeMaps, recFreeMapRender[Fix])
      "ReduceFunc" >> testDSL(reduceFuncs, reduceFuncRender[Fix])
      "JoinFunc" >> testDSL(joinSides, joinFuncRender[Fix])
    }
    implicit val extEqual: Delay[Equal, Extension] = Extension.structuralEqual
    "EJson" >> testDSL(ejsons, eJsonRenderQScriptDSL[Fix])
  }

}
