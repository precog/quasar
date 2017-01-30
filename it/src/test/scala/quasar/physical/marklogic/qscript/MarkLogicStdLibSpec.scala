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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.{Data, TestConfig}
import quasar.contrib.scalacheck.gen
import quasar.effect._
import quasar.fp.ski._
import quasar.fp.tree._
import quasar.fp.eitherT._
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.testing
import quasar.physical.marklogic.xquery._
import quasar.qscript._
import quasar.std._

import java.time.LocalDate

import com.marklogic.xcc.ContentSource
import matryoshka._
import matryoshka.data.Fix
import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import org.specs2.execute._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

abstract class MarkLogicStdLibSpec[F[_]: Monad, FMT](
  implicit
  MP: Planner[F, FMT, MapFunc[Fix, ?]],
  DP: Planner[F, FMT, Const[Data, ?]]
) extends StdLibSpec {
  type RunT[X[_], A] = EitherT[X, Result, A]

  def toMain[G[_]: Monad: Capture](xqy: F[XQuery]): RunT[G, MainModule]

  def runner(contentSource: ContentSource) = new MapFuncStdLibTestRunner {
    def nullaryMapFunc(
      prg: FreeMapA[Fix, Nothing],
      expected: Data
    ): Result = {
      val xqyPlan = planFreeMap(prg)(absurd)

      run(xqyPlan, expected)
    }

    def unaryMapFunc(
      prg: FreeMapA[Fix, UnaryArg],
      arg: Data,
      expected: Data
    ): Result = {
      val xqyPlan = asXqy(arg) flatMap (a1 => planFreeMap(prg)(κ(a1)))

      run(xqyPlan, expected)
    }

    def binaryMapFunc(
      prg: FreeMapA[Fix, BinaryArg],
      arg1: Data, arg2: Data,
      expected: Data
    ): Result = {
      val xqyPlan = (asXqy(arg1) |@| asXqy(arg2)).tupled flatMap {
        case (a1, a2) => planFreeMap(prg)(_.fold(a1, a2))
      }

      run(xqyPlan, expected)
    }

    def ternaryMapFunc(
      prg: FreeMapA[Fix, TernaryArg],
      arg1: Data, arg2: Data, arg3: Data,
      expected: Data
    ): Result = {
      val xqyPlan = (asXqy(arg1) |@| asXqy(arg2) |@| asXqy(arg3)).tupled flatMap {
        case (a1, a2, a3) => planFreeMap(prg)(_.fold(a1, a2, a3))
      }

      run(xqyPlan, expected)
    }

    def intDomain    = arbitrary[Long]   map (BigInt(_))
    def decDomain    = arbitrary[Double] map (BigDecimal(_))
    def stringDomain = gen.printableAsciiString

    // Years 0-999 omitted for year zero disagreement involving millennium extract and trunc.
    val dateDomain: Gen[LocalDate] =
      Gen.choose(
        LocalDate.of(1000, 1, 1).toEpochDay,
        LocalDate.of(9999, 12, 31).toEpochDay
      ) ∘ (LocalDate.ofEpochDay(_))

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

    private def asXqy(d: Data): F[XQuery] = DP.plan(Const(d))
  }

  TestConfig.fileSystemConfigs(FsType).flatMap(_ traverse_ { case (backend, uri, _) =>
    contentSourceConnection[Task](uri).map(cs => backend.name.shows >> tests(runner(cs))).void
  }).unsafePerformSync
}
