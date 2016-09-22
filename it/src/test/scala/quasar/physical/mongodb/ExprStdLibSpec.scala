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
import quasar.std._
import quasar.fp._
import quasar.fs.DataCursor
import quasar.std.{StdLibSpec, StdLibTestRunner}
import quasar.physical.mongodb.fs._, bsoncursor._
import quasar.physical.mongodb.planner.MongoDbPlanner
import quasar.physical.mongodb.workflow._
import WorkflowExecutor.WorkflowCursor

import matryoshka._, Recursive.ops._
import org.specs2.execute._
import org.specs2.main.ArgProperty
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import shapeless.{Nat}

/** Test the implementation of the standard library for MongoDb's aggregation
  * pipeline (aka ExprOp).
  *
  * NB: because compilation to the Expression AST can't currently be separated
  * from the rest of the MongoDb planner, this test runs the whole planner and
  * then simply fails if it finds that the generated plan required map-reduce.
  */
class MongoDbExprStdLibSpec extends StdLibSpec {
  args.report(showtimes = ArgProperty(true))

  MongoDbSpec.clientShould { (backend, prefix, setupClient, testClient) =>
    import MongoDbIO._

    val notHandled = Skipped("not implemented in aggregation")

    def is2_6 = backend == TestConfig.MONGO_2_6

    /** Identify constructs that are expected not to be implemented in the pipeline. */
    def shortCircuitInvoke[N <: Nat](func: GenericFunc[N]): Result \/ Unit = func match {
      case StringLib.Length   => notHandled.left
      case StringLib.Integer  => notHandled.left
      case StringLib.Decimal  => notHandled.left
      case StringLib.ToString => notHandled.left

      case DateLib.TimeOfDay if is2_6 => Skipped("not implemented for MongoDB 2.6").left

      case _                  => ().right
    }

    /** Identify constructs that are expected not to be implemented in the pipeline. */
    def shortCircuit: AlgebraM[Result \/ ?, LogicalPlan, Unit] = {
      case LogicalPlan.InvokeF(func, _) => shortCircuitInvoke(func)
      case _ => ().right
    }

    /** Intercept and transform expected values into the form that's actually
      * produced by the MongoDB backend, in cases where MongoDB cannot represent
      * the type natively. */
    def massage(expected: Data): Data = expected match {
      case Data.Time(time) => Data.Str(time.toString)
      case _               => expected
    }


    def evaluate(wf: Crystallized[WorkflowF], tmp: Collection): MongoDbIO[List[Data]] =
      for {
        exc <- WorkflowExecutor.mongoDb.run.unattempt
        v   <- exc.evaluate(wf, None).run.run(tmp.collection).eval(0).unattempt
        rez <- v.fold(
                _.map(BsonCodec.toData(_)).point[MongoDbIO],
                c => DataCursor[MongoDbIO, WorkflowCursor[BsonCursor]].process(c).runLog.map(_.toList))
      } yield rez

    // TODO: this should probably have access to the arguments so it can inspect them as well
    def check(arity: Int, prg: List[Fix[LogicalPlan]] => Fix[LogicalPlan]): Option[Result] =
      prg((0 until arity).toList.map(idx => LogicalPlan.Free(Symbol("arg" + idx))))
        .cataM[Result \/ ?, Unit](shortCircuit).swap.toOption

    def compile(queryModel: MongoQueryModel, coll: Collection, arity: Int, prg: List[Fix[LogicalPlan]] => Fix[LogicalPlan], resultField: BsonField.Name)
        : PlannerError \/ Crystallized[WorkflowF] = {
      val lp =
        Fix(StructuralLib.MakeObject(
          LogicalPlan.Constant(Data.Str("result")),
          prg(
            (0 until arity).toList.map(idx =>
              Fix(StructuralLib.ObjectProject(
                LogicalPlan.Read(coll.asFile),
                LogicalPlan.Constant(Data.Str("arg" + idx))))))))

      val ctx = QueryContext(queryModel, κ(None), κ(None))

      MongoDbPlanner.plan(lp, ctx).run.value
        .flatMap { wf =>
          val singlePipeline = wf.op.cata[Boolean] {
            case IsSource(_)   => true
            case IsPipeline(p) => p.src
            case _             => false
          }
          if (singlePipeline) wf.right else InternalError("compiled to map-reduce").left
        }
    }

    def run(args: List[Data], prg: List[Fix[LogicalPlan]] => Fix[LogicalPlan], expected: Data): Result =
      check(args.length, prg).getOrElse(
        (for {
          coll <- MongoDbSpec.tempColl(prefix)
          argsBson <- args.zipWithIndex.traverse { case (arg, idx) =>
                      BsonCodec.fromData(arg).point[Task].unattempt.strengthL("arg" + idx) }
          _     <- insert(
                    coll,
                    List(Bson.Doc(argsBson.toListMap)).map(_.repr)).run(setupClient)

          qm  <- serverVersion.map(MongoQueryModel(_)).run(testClient)

          wf  <- compile(qm, coll, args.length, prg, BsonField.Name("result")).point[Task].unattempt

          rez <- evaluate(wf, coll).run(testClient)

          _     <- dropCollection(coll).run(setupClient)
        } yield {
          rez must_= List(Data.Obj(ListMap("result" -> massage(expected))))
        }).unsafePerformSync.toResult)


    val runner = new StdLibTestRunner with MongoDbDomain {
      def nullary(
        prg: Fix[LogicalPlan],
        expected: Data): Result =
        run(Nil, { case Nil => prg; case _ => scala.sys.error("") }, expected)

      def unary(
        prg: Fix[LogicalPlan] => Fix[LogicalPlan],
        arg: Data,
        expected: Data): Result =
        run(List(arg), { case List(arg) => prg(arg) }, expected)

      def binary(
        prg: (Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan],
        arg1: Data, arg2: Data,
        expected: Data): Result =
        run(List(arg1, arg2), { case List(arg1, arg2) => prg(arg1, arg2) }, expected)

      def ternary(
        prg: (Fix[LogicalPlan], Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan],
        arg1: Data, arg2: Data, arg3: Data,
        expected: Data): Result =
        run(List(arg1, arg2, arg3), { case List(arg1, arg2, arg3) => prg(arg1, arg2, arg3) }, expected)
    }

    backend.name should tests(runner)
  }
}
