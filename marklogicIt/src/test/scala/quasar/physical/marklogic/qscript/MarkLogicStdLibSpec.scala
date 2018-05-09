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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.{Data, TestConfig, Type}
import quasar.contrib.scalacheck.gen
import quasar.contrib.scalaz.eitherT._
import quasar.effect._
import quasar.fp.ski._
import quasar.fp.tree._
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.testing
import quasar.physical.marklogic.xquery._
import quasar.qscript._
import quasar.std._
import quasar.time.{DateGenerators, DateTimeInterval}

import java.time._
import scala.math.{abs, round}

import com.marklogic.xcc.ContentSource
import matryoshka._
import matryoshka.data.Fix
import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import org.specs2.execute._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

abstract class MarkLogicStdLibSpec[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT](
  implicit SP: StructuralPlanner[F, FMT]
) extends StdLibSpec {

  private def ignoreUnary(prg: FreeMapA[Fix, UnaryArg], arg1: Data)(run: => Result)
      : Result =
    (prg, arg1) match {
      case (_, _) if isTemporal(arg1.dataType) => pending
      case (ExtractFunc(MapFuncsCore.ToTimestamp(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.ToLocal(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.OffsetDate(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.OffsetDateTime(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.OffsetTime(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.LocalDate(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.LocalDateTime(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.LocalTime(_)), _) => pending
      case (ExtractFunc(MapFuncsCore.Interval(_)), _) => pending
      case (_, _) => run
    }

  private def ignoreBinary(prg: FreeMapA[Fix, BinaryArg], arg1: Data, arg2: Data)(run: => Result)
      : Result =
    (prg, arg1, arg2) match {
      case (_, _, _) if isTemporal(arg1.dataType) || isTemporal(arg2.dataType) => pending
      case (ExtractFunc(MapFuncsCore.Split(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.Within(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.IfUndefined(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.ProjectKey(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.And(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.Or(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.Range(_,_)), _, _) => pending
      case (ExtractFunc(MapFuncsCore.Divide(_,_)), _, _) => pending
      case (_, _, _) => run
    }

  private def ignoreTernary(prg: FreeMapA[Fix, TernaryArg], arg1: Data, arg2: Data, arg3: Data)(run: => Result)
      : Result =
    (prg, arg1, arg2, arg3) match {
      case (_, _, _, _) if isTemporal(arg1.dataType) || isTemporal(arg2.dataType) || isTemporal(arg3.dataType) => pending
      case (_, _, _, _) => run
    }

  private def isTemporal(tpe: Type): Boolean =
    (tpe == Type.OffsetDateTime) || (tpe == Type.OffsetDate) || (tpe == Type.OffsetTime) ||
      (tpe == Type.LocalDateTime) || (tpe == Type.LocalDate) || (tpe == Type.LocalTime) ||
      (tpe == Type.Interval)

  type RunT[X[_], A] = EitherT[X, Result, A]

  def toMain[G[_]: Monad: Capture](xqy: F[XQuery]): RunT[G, MainModule]

  def runner(contentSource: ContentSource) = new MapFuncStdLibTestRunner {
    def nullaryMapFunc(
      prg: FreeMapA[Fix, Nothing],
      expected: Data
    ): Result = {
      val xqyPlan = planFreeMap[Nothing](prg)(absurd)

      run(xqyPlan, expected)
    }

    def unaryMapFunc(
      prg: FreeMapA[Fix, UnaryArg],
      arg: Data,
      expected: Data
    ): Result = ignoreUnary(prg, arg) {
      val xqyPlan = asXqy(arg) flatMap (a1 => planFreeMap[UnaryArg](prg)(κ(a1)))

      run(xqyPlan, expected)
    }

    def binaryMapFunc(
      prg: FreeMapA[Fix, BinaryArg],
      arg1: Data, arg2: Data,
      expected: Data
    ): Result = ignoreBinary(prg, arg1, arg2) {
      val xqyPlan = (asXqy(arg1) |@| asXqy(arg2)).tupled flatMap {
        case (a1, a2) => planFreeMap[BinaryArg](prg)(_.fold(a1, a2))
      }
      run(xqyPlan, expected)
    }

    def ternaryMapFunc(
      prg: FreeMapA[Fix, TernaryArg],
      arg1: Data, arg2: Data, arg3: Data,
      expected: Data
    ): Result = ignoreTernary(prg, arg1, arg2, arg3) {
      val xqyPlan = (asXqy(arg1) |@| asXqy(arg2) |@| asXqy(arg3)).tupled flatMap {
        case (a1, a2, a3) => planFreeMap[TernaryArg](prg)(_.fold(a1, a2, a3))
      }

      run(xqyPlan, expected)
    }

    private def distinguishable(d: Double) = {
      val a = abs(d - round(d))
      //NB the proper value of distinguishable is somewhere between 1E-308 and 1E-306
      (a == 0) || (a >= 1E-306)
    }

    def intDomain    = arbitrary[Long]   map (BigInt(_))

    // MarkLogic cannot handle doubles that are very close, but not equal to a whole number.
    // If not distinguishable then ceil/floor returns a result that is 1 off.
    def decDomain    = arbitrary[Double].filter(distinguishable).map(BigDecimal(_))

    def stringDomain = gen.printableAsciiString

    // Years 0-999 omitted for year zero disagreement involving millennium extract and trunc.
    val dateDomain: Gen[LocalDate] =
      Gen.choose(
        LocalDate.of(1000, 1, 1).toEpochDay,
        LocalDate.of(9999, 12, 31).toEpochDay
      ) ∘ (LocalDate.ofEpochDay(_))

    def timeDomain: Gen[LocalTime] = DateGenerators.genLocalTime
    def intervalDomain: Gen[DateTimeInterval] = DateGenerators.genDateTimeInterval
    def timezoneDomain: Gen[ZoneOffset] = DateGenerators.genZoneOffset

    ////

    private val cpColl = Prolog.defColl(DefaultCollationDecl(Collation.codepoint))

    private def planFreeMap[A](freeMap: FreeMapA[Fix, A])(recover: A => XQuery): F[XQuery] =
      planMapFunc[Fix, F, FMT, A](freeMap)(recover)

    private def run(plan: F[XQuery], expected: Data): Result = {
      val result = for {
        main <- toMain[Task](plan) map (MainModule.prologs.modify(_ insert cpColl))
        mr   <- testing.moduleResults[ReaderT[RunT[Task, ?], ContentSource, ?]](main)
                  .run(contentSource)
        r    =  mr.toOption.join
                  .fold(ko("No results found."))(_ must beCloseTo(expected))
                  .toResult
      } yield r

      result.run.unsafePerformSync.merge
    }

    private def asXqy(d: Data): F[XQuery] = DataPlanner[F, FMT](d)
  }

  TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
    contentSourceConnection[Task](uri).map(cs => backend.name.shows >> tests(runner(cs))).void
  }).unsafePerformSync
}
