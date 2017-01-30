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
import quasar._, Planner.PlannerError
import quasar.std._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.DataCursor
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP, LogicalPlanR}
import quasar.physical.mongodb.fs._, bsoncursor._
import quasar.physical.mongodb.workflow._
import WorkflowExecutor.WorkflowCursor

import scala.concurrent.duration._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute._
import org.specs2.matcher._
import org.specs2.main.ArgProperty
import scalaz._, Scalaz._
import scalaz.concurrent.{Strategy, Task}
import shapeless.{Nat}

/** Test the implementation of the standard library for one of MongoDb's
  * evaluators.
  */
abstract class MongoDbStdLibSpec extends StdLibSpec {
  val lpf = new LogicalPlanR[Fix[LP]]

  args.report(showtimes = ArgProperty(true))

  def shortCircuit[N <: Nat](backend: BackendName, func: GenericFunc[N], args: List[Data]): Result \/ Unit

  def shortCircuitTC(args: List[Data]): Result \/ Unit

  def compile(queryModel: MongoQueryModel, coll: Collection, lp: Fix[LP])
      : PlannerError \/ (Crystallized[WorkflowF], BsonField.Name)

  def is2_6(backend: BackendName): Boolean = backend === TestConfig.MONGO_2_6.name
  def is3_2(backend: BackendName): Boolean = backend === TestConfig.MONGO_3_2.name

  MongoDbSpec.clientShould(FsType) { (backend, prefix, setupClient, testClient) =>
    import MongoDbIO._

    /** Intercept and transform expected values into the form that's actually
      * produced by the MongoDB backend, in cases where MongoDB cannot represent
      * the type natively. */
    def massage(expected: Data): Data = expected match {
      case Data.Time(time) => Data.Str(time.format(DataCodec.timeFormatter))
      case _               => expected
    }

    /** Identify constructs that are expected not to be implemented. */
    def shortCircuitLP(args: List[Data]): AlgebraM[Result \/ ?, LP, Unit] = {
      case lp.Invoke(func, _)     => shortCircuit(backend, func, args)
      case lp.TemporalTrunc(_, _) => shortCircuitTC(args)
      case _ => ().right
    }

    def evaluate(wf: Crystallized[WorkflowF], tmp: Collection): MongoDbIO[List[Data]] =
      for {
        exc <- WorkflowExecutor.mongoDb.run.unattempt
        v   <- exc.evaluate(wf, None).run.run(tmp.collection).eval(0).unattempt
        rez <- v.fold(
                _.map(BsonCodec.toData(_)).point[MongoDbIO],
                c => DataCursor[MongoDbIO, WorkflowCursor[BsonCursor]].process(c).runLog.map(_.toList))
      } yield rez

    def check(args: List[Data], prg: List[Fix[LP]] => Fix[LP]): Option[Result] =
      prg((0 until args.length).toList.map(idx => lpf.free(Symbol("arg" + idx))))
        .cataM[Result \/ ?, Unit](shortCircuitLP(args)).swap.toOption

    final case class SingleResultCheckedMatcher(check: ValueCheck[Data]) extends OptionLikeCheckedMatcher[List, Data, Data](
      "a single result",
      {
        case Data.Obj(m) :: Nil =>
          m.toList match {
            case (_, v) :: Nil => v.some
            case _             => None
          }
        case _ => None
      },
      check)

    def beSingleResult(t: ValueCheck[Data]) = SingleResultCheckedMatcher(t)

    def run(args: List[Data], prg: List[Fix[LP]] => Fix[LP], expected: Data): Result =
      check(args, prg).getOrElse(
        (for {
          coll <- MongoDbSpec.tempColl(prefix)
          argsBson <- args.zipWithIndex.traverse { case (arg, idx) =>
                      BsonCodec.fromData(arg).point[Task].unattempt.strengthL("arg" + idx) }
          _     <- insert(
                    coll,
                    List(Bson.Doc(argsBson.toListMap)).map(_.repr)).run(setupClient)

          qm  <- serverVersion.map(MongoQueryModel(_)).run(testClient)

          lp = prg(
                (0 until args.length).toList.map(idx =>
                  Fix(StructuralLib.ObjectProject(
                    lpf.read(coll.asFile),
                    lpf.constant(Data.Str("arg" + idx))))))
          t  <- compile(qm, coll, lp).point[Task].unattempt
          (wf, resultField) = t

          rez <- evaluate(wf, coll).run(testClient)

          _     <- dropCollection(coll).run(setupClient)
        } yield {
          rez must beSingleResult(beCloseTo(massage(expected)))
        }).timed(5.seconds)(Strategy.DefaultTimeoutScheduler).unsafePerformSync.toResult)

    val runner = new StdLibTestRunner with MongoDbDomain {
      def nullary(
        prg: Fix[LP],
        expected: Data): Result =
        run(Nil, κ(prg), expected)

      def unary(
        prg: Fix[LP] => Fix[LP],
        arg: Data,
        expected: Data): Result =
        run(List(arg), { case List(arg) => prg(arg) }, expected)

      def binary(
        prg: (Fix[LP], Fix[LP]) => Fix[LP],
        arg1: Data, arg2: Data,
        expected: Data): Result =
        run(List(arg1, arg2), { case List(arg1, arg2) => prg(arg1, arg2) }, expected)

      def ternary(
        prg: (Fix[LP], Fix[LP], Fix[LP]) => Fix[LP],
        arg1: Data, arg2: Data, arg3: Data,
        expected: Data): Result =
        run(List(arg1, arg2, arg3), { case List(arg1, arg2, arg3) => prg(arg1, arg2, arg3) }, expected)
    }

    backend.name should tests(runner)
  }
}
