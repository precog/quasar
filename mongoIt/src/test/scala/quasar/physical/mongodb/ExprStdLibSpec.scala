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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._, Planner.PlannerError
import quasar.contrib.scalaz._
import quasar.fs.FileSystemError, FileSystemError.qscriptPlanningFailed
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.FuncHandler
import quasar.physical.mongodb.workflow._
import quasar.physical.mongodb.WorkflowBuilder._
import quasar.qscript.{Coalesce => _, _}
import quasar.std.StdLib._

import java.time.Instant
import matryoshka._
import matryoshka.data.Fix
import org.specs2.execute._
import scalaz.{Name => _, _}, Scalaz._
import shapeless.Nat

/** Test the implementation of the standard library for MongoDb's aggregation
  * pipeline (aka ExprOp).
  */
class MongoDbExprStdLibSpec extends MongoDbStdLibSpec {
  val notHandled = Skipped("not implemented in aggregation")

  /** Identify constructs that are expected not to be implemented in the pipeline. */
  def shortCircuit[N <: Nat](backend: BackendName, func: GenericFunc[N], args: List[Data]): Result \/ Unit = (func, args) match {
    case (string.Length, _) if !is3_4(backend) => Skipped("not implemented in aggregation on MongoDB < 3.4").left
    case (string.Integer, _)  => notHandled.left
    case (string.Decimal, _)  => notHandled.left

    case (string.Search, _) => Skipped("compiles to a map/reduce, so can't be run in tests").left
    case (string.Split, _) if (!is3_4(backend)) => Skipped("not implemented in aggregation on MongoDB < 3.4").left
    case (string.Substring, List(Data.Str(s), _, _)) if (!is3_4(backend) && !isPrintableAscii(s)) =>
      Skipped("only printable ascii supported on MongoDB < 3.4").left
    case (string.ToString, List(Data.Timestamp(_) | Data.Date(_))) => Skipped("Implemented, but formatted incorrectly").left

    case (quasar.std.SetLib.Within, _)      => notHandled.left

    case (date.ExtractIsoYear, _) => notHandled.left
    case (date.ExtractWeek, _)    => Skipped("Implemented, but not ISO compliant").left
    case (date.Now, _)            => Skipped("Returns correct result, but wrapped into Data.Dec instead of Data.Interval").left

    case (date.StartOfDay, _) => notHandled.left
    case (date.TimeOfDay, _) if is2_6(backend) => Skipped("not implemented in aggregation on MongoDB 2.6").left

    //FIXME modulo and trunc (which is defined in terms of modulo) cause the
    //mongo docker container to crash (with quite high frequency but not always).
    //One or more of the other tests that are now marked as skipped also seem to
    //cause failures when marked as pending (but with low frequency)
    case (math.Modulo, _) => Skipped("sometimes causes mongo container crash").left
    case (math.Trunc, _) => Skipped("sometimes causes mongo container crash").left
    case (math.Power, _) if lt3_2(backend) => Skipped("not implemented in aggregation on MongoDB < 3.2").left

    case (relations.Eq, List(Data.Date(_), Data.Timestamp(_))) => notHandled.left
    case (relations.Lt, List(Data.Date(_), Data.Timestamp(_))) => notHandled.left
    case (relations.Lte, List(Data.Date(_), Data.Timestamp(_))) => notHandled.left
    case (relations.Gt, List(Data.Date(_), Data.Timestamp(_))) => notHandled.left
    case (relations.Gte, List(Data.Date(_), Data.Timestamp(_))) => notHandled.left
    case (relations.IfUndefined, _) => notHandled.left

    case (structural.ConcatOp, _)   => notHandled.left
    case (structural.DeleteField, _) => notHandled.left
    case (structural.ObjectProject, _) => notHandled.left

    case _                  => ().right
  }

  def shortCircuitTC(args: List[Data]): Result \/ Unit = notHandled.left

  def build[WF[_]: Coalesce: Inject[WorkflowOpCoreF, ?[_]]](
    expr: Fix[ExprOp], coll: Collection)(
    implicit RT: RenderTree[WorkflowBuilder[WF]]
  ) =
    WorkflowBuilder.build[PlannerError \/ ?, WF](
      WorkflowBuilder.DocBuilder(WorkflowBuilder.Ops[WF].read(coll),
        ListMap(BsonField.Name("value") -> \&/-(expr))))
      .leftMap(qscriptPlanningFailed.reverseGet(_))

  def compile(queryModel: MongoQueryModel, coll: Collection, mf: FreeMap[Fix]
  ) : FileSystemError \/ (Crystallized[WorkflowF], BsonField.Name) = {
    type PlanStdT[A] = ReaderT[FileSystemError \/ ?, Instant, A]

    val bsonVersion = MongoQueryModel.toBsonVersion(queryModel)
    queryModel match {
      case MongoQueryModel.`3.4` =>
        (MongoDbPlanner.getExpr[Fix, PlanStdT, Expr3_4](FuncHandler.handle3_4(bsonVersion))(mf).run(runAt) >>= (build[Workflow3_2F](_, coll)))
          .map(wf => (Crystallize[Workflow3_2F].crystallize(wf).inject[WorkflowF], BsonField.Name("value")))

      case MongoQueryModel.`3.2` =>
        (MongoDbPlanner.getExpr[Fix, PlanStdT, Expr3_2](FuncHandler.handle3_2(bsonVersion))(mf).run(runAt) >>= (build[Workflow3_2F](_, coll)))
          .map(wf => (Crystallize[Workflow3_2F].crystallize(wf).inject[WorkflowF], BsonField.Name("value")))

      case MongoQueryModel.`3.0` =>
        (MongoDbPlanner.getExpr[Fix, PlanStdT, Expr3_0](FuncHandler.handle3_0(bsonVersion))(mf).run(runAt) >>= (build[Workflow2_6F](_, coll)))
          .map(wf => (Crystallize[Workflow2_6F].crystallize(wf).inject[WorkflowF], BsonField.Name("value")))

      case _                     =>
        (MongoDbPlanner.getExpr[Fix, PlanStdT, Expr2_6](FuncHandler.handle2_6(bsonVersion))(mf).run(runAt) >>= (build[Workflow2_6F](_, coll)))
          .map(wf => (Crystallize[Workflow2_6F].crystallize(wf).inject[WorkflowF], BsonField.Name("value")))

    }
  }
}
