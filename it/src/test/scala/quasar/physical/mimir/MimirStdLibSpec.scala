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

import slamdata.Predef._

import quasar.Data
import quasar.blueeyes.json.JValue
import quasar.contrib.scalacheck.gen
import quasar.fp.ski.κ
import quasar.fp.tree.{BinaryArg, TernaryArg, UnaryArg}
import quasar.precog.common.RValue
import quasar.qscript._
import quasar.std.StdLibSpec

import org.scalacheck.{Arbitrary, Gen}

import org.specs2.execute.{Result, Skipped}
import org.specs2.specification.{AfterAll, Scope}

import java.time.LocalDate

import matryoshka.{AlgebraM, Embed}
import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka.patterns._

import scalaz.{\/, \/-, Id}
import scalaz.syntax.applicative._
import scalaz.syntax.either._

import java.nio.file.Files

import scala.concurrent.Await
import scala.concurrent.duration._

class MimirStdLibSpec extends StdLibSpec with PrecogCake {
  import scala.concurrent.ExecutionContext.Implicits.global

  private val notImplemented: Result = Skipped("TODO")

  private def skipBinary(prg: FreeMapA[Fix, BinaryArg], arg1: Data, arg2: Data)(run: => Result): Result =
    (prg, arg1, arg2) match {
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.ConcatArrays(_,_))))), Data.Arr(_), Data.Str(_)) => notImplemented
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.ConcatArrays(_,_))))), Data.Str(_), Data.Arr(_)) => notImplemented
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.ConcatArrays(_,_))))), Data.Str(_), Data.Str(_)) => notImplemented

      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Eq(_,_))))), Data.Date(_), Data.Timestamp(_)) => notImplemented

      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Lt(_,_))))), Data.Str(_), Data.Str(_)) => notImplemented
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Lt(_,_))))), Data.Date(_), Data.Timestamp(_)) => notImplemented

      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Lte(_,_))))), Data.Str(_), Data.Str(_)) => notImplemented
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Lte(_,_))))), Data.Date(_), Data.Timestamp(_)) => notImplemented

      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Gt(_,_))))), Data.Str(_), Data.Str(_)) => notImplemented
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Gt(_,_))))), Data.Date(_), Data.Timestamp(_)) => notImplemented

      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Gte(_,_))))), Data.Str(_), Data.Str(_)) => notImplemented
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Gte(_,_))))), Data.Date(_), Data.Timestamp(_)) => notImplemented

      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Add(_,_))))), Data.Int(_), Data.Dec(_)) => notImplemented // FIXME implemented: should be working
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Add(_,_))))), Data.Dec(_), Data.Int(_)) => notImplemented // FIXME implemented: should be working

      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Subtract(_,_))))), Data.Dec(_), Data.Dec(_)) => notImplemented // FIXME implemented: should be working
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Subtract(_,_))))), Data.Int(_), Data.Dec(_)) => notImplemented // FIXME implemented: should be working
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Subtract(_,_))))), Data.Dec(_), Data.Int(_)) => notImplemented // FIXME implemented: should be working

      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Power(_,_))))), Data.Int(_), Data.Int(one)) if one.toInt == 1 => notImplemented // FIXME implemented: should be working
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Power(_,_))))), Data.Dec(_), Data.Int(one)) if one.toInt == 1 => notImplemented // FIXME implemented: should be working
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Power(_,_))))), Data.Int(_), Data.Int(two)) if two.toInt == 2 => notImplemented // FIXME implemented: should be working

      case _ => run
    }

  // If we create a `AlgebraM[Result \/ ?, MapFunc[Fix, ?], Unit]` scalac errors
  // because it cannot check the match for unreachability.
  private val shortCircuitDerived: AlgebraM[Result \/ ?, MapFuncDerived[Fix, ?], Unit] = {
    case MapFuncsDerived.Ceil(_) => notImplemented.left
    case MapFuncsDerived.Floor(_) => notImplemented.left
    case MapFuncsDerived.Trunc(_) => notImplemented.left
    case MapFuncsDerived.Abs(_) => notImplemented.left
  }

  private val shortCircuitCore: AlgebraM[Result \/ ?, MapFuncCore[Fix, ?], Unit] = {
    case MapFuncsCore.Length(_) => notImplemented.left
    case MapFuncsCore.ExtractCentury(_) => notImplemented.left
    case MapFuncsCore.ExtractDayOfMonth(_) => notImplemented.left
    case MapFuncsCore.ExtractDecade(_) => notImplemented.left
    case MapFuncsCore.ExtractDayOfWeek(_) => notImplemented.left
    case MapFuncsCore.ExtractDayOfYear(_) => notImplemented.left
    case MapFuncsCore.ExtractEpoch(_) => notImplemented.left
    case MapFuncsCore.ExtractHour(_) => notImplemented.left
    case MapFuncsCore.ExtractIsoDayOfWeek(_) => notImplemented.left
    case MapFuncsCore.ExtractIsoYear(_) => notImplemented.left
    case MapFuncsCore.ExtractMicroseconds(_) => notImplemented.left
    case MapFuncsCore.ExtractMillennium(_) => notImplemented.left
    case MapFuncsCore.ExtractMilliseconds(_) => notImplemented.left
    case MapFuncsCore.ExtractMinute(_) => notImplemented.left
    case MapFuncsCore.ExtractMonth(_) => notImplemented.left
    case MapFuncsCore.ExtractQuarter(_) => notImplemented.left
    case MapFuncsCore.ExtractSecond(_) => notImplemented.left
    case MapFuncsCore.ExtractTimezone(_) => notImplemented.left
    case MapFuncsCore.ExtractTimezoneHour(_) => notImplemented.left
    case MapFuncsCore.ExtractTimezoneMinute(_) => notImplemented.left
    case MapFuncsCore.ExtractWeek(_) => notImplemented.left
    case MapFuncsCore.ExtractYear(_) => notImplemented.left
    case MapFuncsCore.Date(_) => notImplemented.left
    case MapFuncsCore.Time(_) => notImplemented.left
    case MapFuncsCore.Timestamp(_) => notImplemented.left
    case MapFuncsCore.Interval(_) => notImplemented.left
    case MapFuncsCore.StartOfDay(_) => notImplemented.left
    case MapFuncsCore.TemporalTrunc(part, _) => notImplemented.left
    case MapFuncsCore.TimeOfDay(_) => notImplemented.left
    case MapFuncsCore.ToTimestamp(_) => notImplemented.left
    case MapFuncsCore.Now() => notImplemented.left
    case MapFuncsCore.TypeOf(_) => notImplemented.left
    case MapFuncsCore.Negate(_) => notImplemented.left // FIXME implemented: should be working
    case MapFuncsCore.Modulo(_, _) => notImplemented.left // FIXME implemented: should be working
    case MapFuncsCore.Not(_) => notImplemented.left
    case MapFuncsCore.Neq(_, _) => notImplemented.left
    case MapFuncsCore.IfUndefined(_, _) => notImplemented.left
    case MapFuncsCore.Between(_, _, _) => notImplemented.left
    case MapFuncsCore.Cond(_, _, _) => notImplemented.left
    case MapFuncsCore.Within(_, _) => notImplemented.left
    case MapFuncsCore.Bool(_) => notImplemented.left
    case MapFuncsCore.Integer(_) => notImplemented.left
    case MapFuncsCore.Decimal(_) => notImplemented.left
    case MapFuncsCore.Null(_) => notImplemented.left
    case MapFuncsCore.ToString(_) => notImplemented.left
    case MapFuncsCore.Search(_, _, _) => notImplemented.left
    case MapFuncsCore.Substring(_, _, _) => notImplemented.left
    case MapFuncsCore.DeleteField(_, _) => notImplemented.left
    case MapFuncsCore.Meta(_) => notImplemented.left
    case MapFuncsCore.Range(_, _) => notImplemented.left
    case _ => ().right
  }

  private def check[A](fm: FreeMapA[Fix, A]): Option[Result] =
    fm.cataM(interpretM[Result \/ ?, MapFunc[Fix, ?], A, Unit](
      κ(().right),
      _.run.fold(shortCircuitCore, shortCircuitDerived))).swap.toOption

  private def run[A](
    freeMap: FreeMapA[Fix, A],
    hole: A => cake.trans.TransSpec1)
      : cake.trans.TransSpec1 =
    freeMap.cataM[Id.Id, cake.trans.TransSpec1](interpretM[Id.Id, MapFunc[Fix, ?], A, cake.trans.TransSpec1](
      hole(_).point[Id.Id],
      MapFuncPlanner[Fix, Id.Id, MapFunc[Fix, ?]].plan(cake)[cake.trans.Source1](cake.trans.TransSpec1.Id)))

  private def evaluate(transSpec: cake.trans.TransSpec1): cake.Table =
    cake.Table.constString(Set("")).transform(transSpec)

  private def dataToTransSpec(data: Data): cake.trans.TransSpec1 = {
    val jvalue: JValue = JValue.fromData(data)
    val rvalue: RValue = RValue.fromJValue(jvalue)
    cake.trans.transRValue(rvalue, cake.trans.TransSpec1.Id)
  }

  private def actual(table: cake.Table): List[Data] =
    Await.result(table.toJson.map(_.toList.map(JValue.toData)), Duration.Inf)

  def runner = new MapFuncStdLibTestRunner {
    def nullaryMapFunc(prg: FreeMapA[Fix, Nothing], expected: Data): Result =
      notImplemented

    def unaryMapFunc(
      prg: FreeMapA[Fix, UnaryArg],
      arg: Data,
      expected: Data)
        : Result =
      check(prg) getOrElse {
        val table: cake.Table = evaluate(
          run[UnaryArg](prg, _.fold(dataToTransSpec(arg))))
        ((actual(table) must haveSize(1)) and
	  (actual(table).head must beCloseTo(expected))).toResult
      }

    def binaryMapFunc(
      prg: FreeMapA[Fix, BinaryArg],
      arg1: Data,
      arg2: Data,
      expected: Data)
        : Result =
      skipBinary(prg, arg1, arg2)(check(prg) getOrElse {
        val table: cake.Table =
          evaluate(run[BinaryArg](prg, _.fold(
            dataToTransSpec(arg1),
            dataToTransSpec(arg2))))
        ((actual(table) must haveSize(1)) and
	  (actual(table).head must beCloseTo(expected))).toResult
      })

    def ternaryMapFunc(
      prg: FreeMapA[Fix, TernaryArg],
      arg1: Data,
      arg2: Data,
      arg3: Data,
      expected: Data)
        : Result =
      check(prg) getOrElse {
        val table: cake.Table =
          evaluate(run[TernaryArg](prg, _.fold(
            dataToTransSpec(arg1),
            dataToTransSpec(arg2),
            dataToTransSpec(arg3))))
        ((actual(table) must haveSize(1)) and
	  (actual(table).head must beCloseTo(expected))).toResult
      }

    def decDomain: Gen[BigDecimal] = Arbitrary.arbitrary[Long].map(BigDecimal(_))
    def intDomain: Gen[BigInt] = Arbitrary.arbitrary[Long].map(BigInt(_))
    def stringDomain: Gen[String] = gen.printableAsciiString

    def dateDomain: Gen[LocalDate] =
      Gen.choose(
        LocalDate.of(1, 1, 1).toEpochDay,
        LocalDate.of(9999, 12, 31).toEpochDay
      ) ∘ (LocalDate.ofEpochDay(_))
  }

  tests(runner)
}

trait PrecogCake extends Scope with AfterAll {
  val cake = Precog(Files.createTempDirectory("mimir").toFile()).unsafePerformSync

  def afterAll(): Unit = Await.result(cake.shutdown, Duration.Inf)
}
