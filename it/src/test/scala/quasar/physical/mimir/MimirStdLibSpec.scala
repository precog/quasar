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

package quasar.mimir

import slamdata.Predef._

import quasar.Data
import quasar.contrib.scalacheck.gen
import quasar.fp.ski.κ
import quasar.contrib.iota.copkTraverse
import quasar.fp.tree.{BinaryArg, TernaryArg, UnaryArg}
import quasar.precog.common.RValue
import quasar.qscript._
import quasar.std.StdLibSpec
import quasar.time.{DateGenerators, DateTimeInterval}

import org.scalacheck.{Arbitrary, Gen}

import org.specs2.execute.{Result, Skipped}
import org.specs2.specification.{AfterAll, Scope}

import java.time.{Duration => _, _}

import matryoshka.AlgebraM
import matryoshka.data.{Fix, freeRecursive}
import matryoshka.implicits._
import matryoshka.patterns._

import scalaz.{\/, Id}
import scalaz.syntax.applicative._
import scalaz.syntax.either._

import java.nio.file.Files

class MimirStdLibSpec extends StdLibSpec with PrecogCake {

  private val notImplemented: Result = Skipped("TODO")

  private def skipBinary(prg: FreeMapA[Fix, BinaryArg], arg1: Data, arg2: Data)(run: => Result): Result =
    (prg, arg1, arg2) match {
      case (ExtractFunc(MapFuncsCore.ConcatArrays(_,_)), Data.Arr(_), Data.Str(_)) => notImplemented
      case (ExtractFunc(MapFuncsCore.ConcatArrays(_,_)), Data.Str(_), Data.Arr(_)) => notImplemented

      case (ExtractFunc(MapFuncsCore.Add(_,_)), Data.Int(_), Data.Dec(_)) => notImplemented // FIXME implemented: should be working
      case (ExtractFunc(MapFuncsCore.Add(_,_)), Data.Dec(_), Data.Int(_)) => notImplemented // FIXME implemented: should be working
      case (ExtractFunc(MapFuncsCore.Add(_,_)), Data.Dec(_), Data.Dec(_)) => notImplemented // FIXME implemented: should be working

      case (ExtractFunc(MapFuncsCore.Subtract(_,_)), Data.Dec(_), Data.Dec(_)) => notImplemented // FIXME implemented: should be working
      case (ExtractFunc(MapFuncsCore.Subtract(_,_)), Data.Int(_), Data.Dec(_)) => notImplemented // FIXME implemented: should be working
      case (ExtractFunc(MapFuncsCore.Subtract(_,_)), Data.Dec(_), Data.Int(_)) => notImplemented // FIXME implemented: should be working

      case (ExtractFunc(MapFuncsCore.Divide(_,_)), Data.Int(_), Data.Int(_)) => notImplemented // FIXME implemented: should be working

      case (ExtractFunc(MapFuncsCore.Power(_,_)), Data.Int(_), Data.Int(one)) if one.toInt == 1 => notImplemented // FIXME implemented: should be working
      case (ExtractFunc(MapFuncsCore.Power(_,_)), Data.Dec(_), Data.Int(one)) if one.toInt == 1 => notImplemented // FIXME implemented: should be working
      case (ExtractFunc(MapFuncsCore.Power(_,_)), Data.Int(_), Data.Int(two)) if two.toInt == 2 => notImplemented // FIXME implemented: should be working

      case _ => run
    }

  // If we create a `AlgebraM[Result \/ ?, MapFunc[Fix, ?], Unit]` scalac errors
  // because it cannot check the match for unreachability.
  private val shortCircuitDerived: AlgebraM[Result \/ ?, MapFuncDerived[Fix, ?], Unit] = {
    // we're supporting some via primitives, some we get automatically
    case _ => ().right
  }

  private val shortCircuitCore: AlgebraM[Result \/ ?, MapFuncCore[Fix, ?], Unit] = {
    case MapFuncsCore.Now() => notImplemented.left
    case MapFuncsCore.NowTime() => notImplemented.left
    case MapFuncsCore.NowDate() => notImplemented.left
    case MapFuncsCore.CurrentTimeZone() => notImplemented.left
    case MapFuncsCore.ToTimestamp(_) => notImplemented.left
    case MapFuncsCore.ToLocal(_) => notImplemented.left
    case MapFuncsCore.Interval(_) => notImplemented.left
    case MapFuncsCore.TypeOf(_) => notImplemented.left
    case MapFuncsCore.Negate(_) => notImplemented.left // TODO this isn't passing because -Long.MinValue == Long.MinValue, so basically a limitation in ColumnarTable
    // case MapFuncsCore.ToString(Data.Date(_) | Data.Timestamp(_) | Data.Time(_) | Data.Interval(_)) => notImplemented.left
    case MapFuncsCore.ToString(_) => notImplemented.left    // TODO it's implemented but not for everything
    case MapFuncsCore.Meta(_) => notImplemented.left
    case _ => ().right
  }

  private def check[A](fm: FreeMapA[Fix, A]): Option[Result] =
    fm.cataM(interpretM[Result \/ ?, MapFunc[Fix, ?], A, Unit](
      κ(().right),
      {
        case MFC(mfc) => shortCircuitCore(mfc)
        case MFD(mfd) => shortCircuitDerived(mfd)
      }
    )).swap.toOption

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
    val rvalue: Option[RValue] = MapFuncCorePlanner.dataToRValue(data)
    rvalue.map(cake.trans.transRValue(_, cake.trans.TransSpec1.Id)).getOrElse(cake.trans.TransSpec1.Undef)
  }

  private def actual(table: cake.Table): List[Data] = {
    table.toJson.map(_.toList.map(MapFuncCorePlanner.rValueToData)).unsafeRunSync
  }

  def runner = new MapFuncStdLibTestRunner {
    def nullaryMapFunc(prg: FreeMapA[Fix, Nothing], expected: Data): Result =
      notImplemented

    def unaryMapFunc(
        prg: FreeMapA[Fix, UnaryArg],
        arg: Data,
        expected: Data): Result = {

      check(prg) getOrElse {
        val table: cake.Table =
          evaluate(
            run[UnaryArg](
              prg,
              _.fold(dataToTransSpec(arg))))

        val result = actual(table)

        lazy val primary = ((result must haveSize(1)) and
          (result.head must beCloseTo(expected)))

        lazy val fallback = ((result must haveSize(0))) and
          (expected mustEqual Data.NA)

        (primary or fallback).toResult
      }
    }

    def binaryMapFunc(
        prg: FreeMapA[Fix, BinaryArg],
        arg1: Data,
        arg2: Data,
        expected: Data): Result = {

      skipBinary(prg, arg1, arg2)(check(prg) getOrElse {
        val table: cake.Table =
          evaluate(run[BinaryArg](
            prg,
            _.fold(
              dataToTransSpec(arg1),
              dataToTransSpec(arg2))))

        val result = actual(table)

        lazy val primary = ((result must haveSize(1)) and
          (result.head must beCloseTo(expected)))

        lazy val fallback = ((result must haveSize(0))) and
          (expected mustEqual Data.NA)

        (primary or fallback).toResult
      })
    }

    def ternaryMapFunc(
        prg: FreeMapA[Fix, TernaryArg],
        arg1: Data,
        arg2: Data,
        arg3: Data,
        expected: Data): Result = {

      check(prg) getOrElse {
        val table: cake.Table =
          evaluate(run[TernaryArg](
            prg,
            _.fold(
              dataToTransSpec(arg1),
              dataToTransSpec(arg2),
              dataToTransSpec(arg3))))

        val result = actual(table)

        lazy val primary = ((result must haveSize(1)) and
          (result.head must beCloseTo(expected)))

        lazy val fallback = ((result must haveSize(0))) and
          (expected mustEqual Data.NA)

        (primary or fallback).toResult
      }
    }

    def decDomain: Gen[BigDecimal] = Arbitrary.arbitrary[Long].map(BigDecimal(_))
    def intDomain: Gen[BigInt] = Arbitrary.arbitrary[Long].map(BigInt(_))
    def stringDomain: Gen[String] = gen.printableAsciiString

    def dateDomain: Gen[LocalDate] =
      Gen.choose(
        LocalDate.of(1, 1, 1).toEpochDay,
        LocalDate.of(9999, 12, 31).toEpochDay
      ) ∘ (LocalDate.ofEpochDay(_))

    def timeDomain: Gen[LocalTime] = DateGenerators.genLocalTime
    def intervalDomain: Gen[DateTimeInterval] = DateGenerators.genInterval
    def timezoneDomain: Gen[ZoneOffset] = DateGenerators.genZoneOffset
  }

  tests(runner)
}

trait PrecogCake extends Scope with AfterAll {
  private val caked = Precog(Files.createTempDirectory("mimir").toFile()).unsafeRunSync

  val cake = caked.unsafeValue

  def afterAll(): Unit = caked.dispose.unsafeRunSync
}
