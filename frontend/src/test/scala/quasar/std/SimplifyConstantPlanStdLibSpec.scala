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

package quasar.std

import slamdata.Predef._
import quasar.{Data, GenericFunc}
import quasar.RenderTree.ops._
import quasar.fp.ski._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}
import quasar.std.StdLib._
import quasar.time.DateGenerators

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute._
import org.scalacheck.Arbitrary
import Arbitrary._

import scalaz.{Failure => _, _}
import Scalaz._
import shapeless.Nat

/** Test the typers defined in the std lib functions themselves.
  */
class SimplifyConstantPlanStdLibSpec extends StdLibSpec {
  private val lpf = new LogicalPlanR[Fix[LP]]
  private val optimizer = new Optimizer[Fix[LP]]

  val notHandled: Result \/ Unit = Skipped("not simplified").left

  def shortCircuit[N <: Nat](func: GenericFunc[N], args: List[Data]): Result \/ Unit = (func, args) match {
    case (relations.IfUndefined, _) => notHandled

    case (date.Now, _) => notHandled
    case (date.NowTime, _) => notHandled
    case (date.NowDate, _) => notHandled
    case (date.CurrentTimeZone, _) => notHandled

    case (math.Divide, List(_, Data.Number(n)))
      if (n === BigDecimal(0)) => notHandled

    case (structural.MapProject, List(Data.Obj(fields), Data.Str(field))) if !fields.contains(field) => notHandled

    case (StdLib.set.Range, List(Data.Int(a), Data.Int(b))) if a > b =>
      notHandled

    case _ =>  ().right
  }

  /** Identify constructs that are expected not to be implemented. */
  def shortCircuitLP(args: List[Data]): AlgebraM[Result \/ ?, LP, Unit] = {
    case Invoke(func, _)     => shortCircuit(func, args)
    case _                   => ().right
  }

  def check(args: List[Data], prg: List[Fix[LP]] => Fix[LP]): Option[Result] =
    prg((0 until args.length).toList.map(idx => lpf.free(Symbol("arg" + idx))))
      .cataM[Result \/ ?, Unit](shortCircuitLP(args)).swap.toOption

  def run(lp: Fix[LP], expected: Data): Result =
    lpf.ensureCorrectTypes(optimizer.simplify(lp)).run.run._2 match {
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

    def dateDomain = DateGenerators.genLocalDate

    def timeDomain = DateGenerators.genLocalTime

    def timezoneDomain = DateGenerators.genZoneOffset

    def intervalDomain = DateGenerators.genInterval
  }

  tests(runner)
}
