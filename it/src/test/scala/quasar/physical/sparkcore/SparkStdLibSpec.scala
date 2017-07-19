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

package quasar.physical.sparkcore

import quasar._
import slamdata.Predef._
import quasar.Planner.PlannerError
import quasar.fp.ski._
import quasar.fp.tree._
import quasar.qscript._, MapFuncsCore._, MapFuncsDerived._
import quasar.physical.sparkcore.fs.CoreMap
import quasar.std._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka.patterns._
import org.scalacheck.Arbitrary.arbitrary
import org.specs2.execute._
import scalaz._, Scalaz._

class SparkStdLibSpec extends StdLibSpec {
  val TODO: Result \/ Unit = Skipped("TODO").left

  def ignoreSome(prg: FreeMapA[Fix, BinaryArg], arg1: Data, arg2: Data)(run: => Result): Result =
    (prg, arg1, arg2) match {
      case (Embed(CoEnv(\/-(MFC(MapFuncsCore.Eq(_,_))))), Data.Date(_), Data.Timestamp(_)) => Skipped("TODO")
      case _ => run
    }

  /** Identify constructs that are expected not to be implemented. */
  val shortCircuit: AlgebraM[Result \/ ?, MapFunc[Fix, ?], Unit] = {
    case MFC(ExtractIsoYear(_))  => TODO
    case MFC(ExtractWeek(_))     => TODO
    case MFD(Trunc(_))           => TODO
    case MFC(Power(_, _))        => Pending("TODO: handle large value").left
    case MFC(ConcatArrays(_, _)) => Pending("TODO: handle mixed string/array").left
    case _                       => ().right
  }

  // TODO: figure out how to pass the args to shortCircuit so they can be inspected
  def check[A](fm: Free[MapFunc[Fix, ?], A], args: List[Data]): Option[Result] =
    fm.cataM(interpretM[Result \/ ?, MapFunc[Fix, ?], A, Unit](κ(().right), shortCircuit)).swap.toOption

  /** Compile/execute on this backend, and compare with the expected value. */
  // TODO: this signature might not work for other implementations.
  def run[A](fm: Free[MapFunc[Fix, ?], A], args: A => Data, expected: Data): Result = {
    val run = fm.cataM(interpretM[PlannerError \/ ?, MapFunc[Fix, ?], A, Data => Data](
      a => κ(args(a)).right, CoreMap.change))
    (run.map(_(Data.NA)) must beRightDisjunction.like { case d => d must beCloseTo(expected) }).toResult
  }

  val runner = new MapFuncStdLibTestRunner {
    def nullaryMapFunc(
      prg: FreeMapA[Fix, Nothing],
      expected: Data
    ): Result =
      failure

    def unaryMapFunc(
      prg: FreeMapA[Fix, UnaryArg],
      arg: Data,
      expected: Data
    ): Result =
      check(prg, List(arg)) getOrElse
        run(prg, κ(arg), expected)

    def binaryMapFunc(
      prg: FreeMapA[Fix, BinaryArg],
      arg1: Data, arg2: Data,
      expected: Data
    ): Result =
      ignoreSome(prg, arg1, arg2)(check(prg, List(arg1, arg2)) getOrElse
       run[BinaryArg](prg, _.fold(arg1, arg2), expected))

    def ternaryMapFunc(
      prg: FreeMapA[Fix, TernaryArg],
      arg1: Data, arg2: Data, arg3: Data,
      expected: Data
    ): Result =
      check(prg, List(arg1, arg2, arg3)) getOrElse
       run[TernaryArg](prg, _.fold(arg1, arg2, arg3), expected)

    def intDomain = arbitrary[BigInt]

    // NB: BigDecimal parsing cannot handle values that are too close to the
    // edges of its range.
    def decDomain = arbitrary[BigDecimal].filter(i => i.scale > Int.MinValue && i.scale < Int.MaxValue)

    def stringDomain = arbitrary[String]

    def dateDomain = DateArbitrary.genDate
  }

  tests(runner)
}
