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

package quasar.std

import slamdata.Predef._
import quasar.{Data, DateArbitrary, GenericFunc}
import quasar.RenderTree.ops._
import quasar.fp.ski._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}
import quasar.std.StdLib._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute._
import org.scalacheck.Arbitrary, Arbitrary._
import scalaz.{Failure => _, _}, Scalaz._
import shapeless.Nat

/** Test the typers and simplifiers defined in the std lib functions themselves.
  */
class SimplifyStdLibSpec extends StdLibSpec {
  val lpf = new LogicalPlanR[Fix[LP]]

  val notHandled: Result \/ Unit = Skipped("not simplified").left

  def shortCircuit[N <: Nat](func: GenericFunc[N], args: List[Data]): Result \/ Unit = (func, args) match {
    case (relations.Between, _) => notHandled

    case (relations.Eq, List(Data.Date(_), Data.Timestamp(_))) => notHandled
    case (relations.Lt, List(Data.Date(_), Data.Timestamp(_))) => notHandled
    case (relations.Lte, List(Data.Date(_), Data.Timestamp(_))) => notHandled
    case (relations.Gt, List(Data.Date(_), Data.Timestamp(_))) => notHandled
    case (relations.Gte, List(Data.Date(_), Data.Timestamp(_))) => notHandled

    case (date.ExtractCentury, _) => notHandled
    case (date.ExtractDayOfMonth, _) => notHandled
    case (date.ExtractDecade, _) => notHandled
    case (date.ExtractDayOfWeek, _) => notHandled
    case (date.ExtractDayOfYear, _) => notHandled
    case (date.ExtractEpoch, _) => notHandled
    case (date.ExtractHour, _) => notHandled
    case (date.ExtractIsoDayOfWeek, _) => notHandled
    case (date.ExtractIsoYear, _) => notHandled
    case (date.ExtractMicroseconds, _) => notHandled
    case (date.ExtractMillennium, _) => notHandled
    case (date.ExtractMilliseconds, _) => notHandled
    case (date.ExtractMinute, _) => notHandled
    case (date.ExtractMonth, _) => notHandled
    case (date.ExtractQuarter, _) => notHandled
    case (date.ExtractSecond, _) => notHandled
    case (date.ExtractWeek, _) => notHandled
    case (date.ExtractYear, _) => notHandled

    case _ => ().right
  }

  /** Identify constructs that are expected not to be implemented. */
  def shortCircuitLP(args: List[Data]): AlgebraM[Result \/ ?, LP, Unit] = {
    case Invoke(func, _)     => shortCircuit(func, args)
    case TemporalTrunc(_, _) => notHandled
    case _                   => ().right
  }

  def check(args: List[Data], prg: List[Fix[LP]] => Fix[LP]): Option[Result] =
    prg((0 until args.length).toList.map(idx => lpf.free(Symbol("arg" + idx))))
      .cataM[Result \/ ?, Unit](shortCircuitLP(args)).swap.toOption

  def run(lp: Fix[LP], expected: Data): Result =
    lpf.ensureCorrectTypes(lp).disjunction match {
      case  \/-(Embed(Constant(d))) => (d must beCloseTo(expected)).toResult
      case  \/-(v) => Failure("not a constant", v.render.shows)
      case -\/ (err) => Failure("simplification failed", err.toString)
    }

  val runner = new StdLibTestRunner {
    def nullary(prg: Fix[LP], expected: Data) =
      check(Nil, κ(prg)) getOrElse
        run(prg, expected)

    def unary(prg: Fix[LP] => Fix[LP], arg: Data, expected: Data) =
      check(List(arg), { case List(arg1) => prg(arg1) }) getOrElse
        run(prg(lpf.constant(arg)), expected)

    def binary(prg: (Fix[LP], Fix[LP]) => Fix[LP], arg1: Data, arg2: Data, expected: Data) =
      check(List(arg1, arg2), { case List(arg1, arg2) => prg(arg1, arg2) }) getOrElse
        run(prg(lpf.constant(arg1), lpf.constant(arg2)), expected)

    def ternary(prg: (Fix[LP], Fix[LP], Fix[LP]) => Fix[LP], arg1: Data, arg2: Data, arg3: Data, expected: Data) =
      check(List(arg1, arg2, arg3), { case List(arg1, arg2, arg3) => prg(arg1, arg2, arg3) }) getOrElse
        run(prg(lpf.constant(arg1), lpf.constant(arg2), lpf.constant(arg3)), expected)

    def intDomain = arbitrary[BigInt]

    // NB: BigDecimal parsing cannot handle values that are too close to the
    // edges of its range.
    def decDomain = arbitrary[BigDecimal].filter(i => i.scale > Int.MinValue && i.scale < Int.MaxValue)

    def stringDomain = arbitrary[String]

    def dateDomain = DateArbitrary.genDate
  }

  tests(runner)
}
