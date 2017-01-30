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

package quasar.physical.mongodb

import quasar.Predef._
import quasar._, Planner.{PlannerError, InternalError}
import quasar.std.StdLib._
import quasar.jscore._
import quasar.frontend.logicalplan.LogicalPlan
import quasar.physical.mongodb.planner.MongoDbPlanner
import quasar.physical.mongodb.workflow._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute._
import scalaz.{Name => _, _}, Scalaz._
import shapeless.Nat

/** Test the implementation of the standard library for MongoDb's map-reduce
  * (i.e. JavaScript).
  */
class MongoDbJsStdLibSpec extends MongoDbStdLibSpec {
  val notHandled = Skipped("not implemented in JS")

  /** Identify constructs that are expected not to be implemented in JS. */
  def shortCircuit[N <: Nat](backend: BackendName, func: GenericFunc[N], args: List[Data]): Result \/ Unit = (func, args) match {
    case (string.Lower, _)   => Skipped("TODO").left
    case (string.Upper, _)   => Skipped("TODO").left

    case (string.ToString, Data.Dec(_) :: Nil) =>
      Skipped("Dec printing doesn't match precisely").left
    case (string.ToString, Data.Date(_) :: Nil) =>
      Skipped("Date printing doesn't match").left
    case (string.ToString, Data.Interval(_) :: Nil) =>
      Skipped("Interval prints numeric representation").left

    case (math.Power, Data.Number(x) :: Data.Number(y) :: Nil)
        if x == 0 && y < 0 =>
      Skipped("Infinity is not translated properly?").left

    case (relations.Cond, _)           => Skipped("TODO").left

    case (date.ExtractDayOfYear, _)    => Skipped("TODO").left
    // case (date.ExtractIsoDayOfWeek, _) => Skipped("TODO").left
    case (date.ExtractIsoYear, _)      => Skipped("TODO").left
    case (date.ExtractWeek, _)         => Skipped("TODO").left
    case (date.ExtractQuarter, _)      => Skipped("TODO").left

    case (structural.ConcatOp, _)      => Skipped("TODO").left

    case _                             => ().right
  }

  def shortCircuitTC(args: List[Data]): Result \/ Unit = Skipped("TODO").left

  def compile(queryModel: MongoQueryModel, coll: Collection, lp: Fix[LogicalPlan])
      : PlannerError \/ (Crystallized[WorkflowF], BsonField.Name) = {

    for {
      t  <- lp.cata(MongoDbPlanner.jsExprƒ)
      (pj, ifs) = t
      js <- pj.lift(List.fill(ifs.length)(JsFn.identity)) \/> InternalError.fromMsg("no JS compilation")
      wf =  chain[Fix[WorkflowF]](
              $read(coll),
              $simpleMap(NonEmptyList(MapExpr(js)), ListMap.empty))
    } yield (Crystallize[WorkflowF].crystallize(wf), BsonField.Name("value"))
  }
}
