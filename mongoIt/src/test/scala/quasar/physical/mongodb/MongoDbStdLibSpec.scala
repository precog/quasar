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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._
import quasar.contrib.scalaz.catchable._
import quasar.fp._
import quasar.fp.ski._
import quasar.fp.tree._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}
import quasar.fs.{DataCursor, FileSystemError}
import quasar.physical.mongodb.WorkflowExecutor.WorkflowCursor
import quasar.physical.mongodb.fs._, bsoncursor._
import quasar.physical.mongodb.workflow._
import quasar.qscript._
import quasar.std._
import quasar.time.TemporalPart

import scala.sys

import java.time.{Instant, OffsetDateTime => JOffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

import matryoshka.{Hole => _, _}
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute._
import org.specs2.matcher._
import org.specs2.main.ArgProperty
import scala.concurrent.duration._
import scalaz._, Scalaz._
import scalaz.concurrent.{Strategy, Task}
import shapeless.{Nat}

/** Test the implementation of the standard library for one of MongoDb's
  * evaluators.
  */
abstract class MongoDbStdLibSpec extends StdLibSpec {
  val lpf = new lp.LogicalPlanR[Fix[LP]]

  val runAt = Instant.parse("2015-01-26T00:00:00Z")

  val noTimeZoneSupport = Pending("Mongo does not support time zones.")

  args.report(showtimes = ArgProperty(true))

  def shortCircuit[N <: Nat](backend: BackendName, func: GenericFunc[N], args: List[Data]): Result \/ Unit

  def temporalTruncSupported(backend: BackendName, part: TemporalPart): Boolean

  def compile(queryModel: MongoQueryModel, coll: Collection, lp: FreeMap[Fix])
      : FileSystemError \/ (Crystallized[WorkflowF], BsonField.Name)

  def advertisedVersion(backend: BackendName): Option[MongoQueryModel] =
    if (backend ≟ TestConfig.MONGO_3_2.name) MongoQueryModel.`3.2`.some
    else if (backend ≟ TestConfig.MONGO_3_4.name) MongoQueryModel.`3.4`.some
    else if ((backend ≟ TestConfig.MONGO_3_4_13.name) || (backend ≟ TestConfig.MONGO_READ_ONLY.name))
      MongoQueryModel.`3.4.4`.some
    else if (backend ≟ TestConfig.MONGO_3_6.name) MongoQueryModel.`3.6`.some
    else None

  MongoDbSpec.clientShould(MongoDb.Type) { (backend, prefix, setupClient, testClient) =>
    import MongoDbIO._

    /** Intercept and transform expected values into the form that's actually
      * produced by the MongoDB backend, in cases where MongoDB cannot represent
      * the type natively. */
    def massageExpected(expected: Data): Data = expected match {
      case Data.LocalDate(time) =>
        Data.OffsetDateTime(JOffsetDateTime.of(time.atStartOfDay, ZoneOffset.UTC))
      case Data.LocalTime(time) =>
        Data.Str(time.toString)
      case Data.LocalDateTime(time) =>
        Data.OffsetDateTime(JOffsetDateTime.of(
          // NB withNano(scala.math.round(...)) fails for a corner case:
          // Invalid value for NanoOfSecond (valid values 0 - 999999999): 1000000000
          time.truncatedTo(ChronoUnit.SECONDS)
            .plusNanos(scala.math.round(time.getNano.toDouble / 1000000).toLong * 1000000), // round to millis
          ZoneOffset.UTC))
      case _ => expected
    }

    def containsOffset(args: List[Data]): Boolean =
      args.filter {
        case Data.OffsetLike(_) => true
        case _ => false
      }.nonEmpty

    /** Identify constructs that are expected not to be implemented. */
    def shortCircuitLP(args: List[Data]): AlgebraM[Result \/ ?, LP, Unit] = {
      case lp.Invoke(_, _) if containsOffset(args) => noTimeZoneSupport.left
      case lp.Invoke(func, _) => shortCircuit(backend, func, args)
      case lp.TemporalTrunc(_, _) if containsOffset(args) => noTimeZoneSupport.left
      case lp.TemporalTrunc(_, _)
        if args.exists(Data._localTime.getOption(_).isDefined) =>
          Pending(s"TemporalTrunc for LocalTime not implemented.").left
      case lp.TemporalTrunc(part, _) if !temporalTruncSupported(backend, part) =>
        Pending(s"TemporalTrunc for $part not implemented.").left
      case _ => ().right
    }

    def evaluate(wf: Crystallized[WorkflowF], tmp: Collection): MongoDbIO[List[Data]] =
      for {
        exc <- WorkflowExecutor.mongoDb.run.unattemptRuntime
        v   <- exc.evaluate(wf).run.run(tmp.collection).eval(0).unattemptRuntime
        rez <- v.fold(
                _.map(BsonCodec.toData(_)).point[MongoDbIO],
                c => DataCursor[MongoDbIO, WorkflowCursor[BsonCursor]].process(c).runLog.map(_.toList))
      } yield rez

    def check(args: List[Data], prg: List[Fix[LP]] => Fix[LP]): Option[Result] =
      prg((0 until args.length).toList.map(idx => lpf.free(Symbol("arg" + idx))))
        .cataM[Result \/ ?, Unit](shortCircuitLP(args)).swap.toOption

    final case class SingleResultCheckedMatcher(check: ValueCheck[Data])
        extends OptionLikeCheckedMatcher[List, Data, Data](
          "a single result",
          {
            case v :: Nil => Some(v)
            case _        => None
          },
          check)

    def beSingleResult(t: ValueCheck[Data]) = SingleResultCheckedMatcher(t)

    val runner = new MapFuncStdLibTestRunner with MongoDbDomain {
      def run(args: List[Data], prg: List[Fix[LP]] => Fix[LP], expected: Data): Result =
        check(args, prg).getOrElse(
          (for {
            coll <- MongoDbSpec.tempColl(prefix)

            qm  <- serverVersion.map(MongoQueryModel(_)).run(testClient)
            bsonVersion = MongoQueryModel.toBsonVersion(qm)

            argsBson <- args.zipWithIndex.traverse { case (arg, idx) =>
              BsonCodec.fromData(bsonVersion, arg).point[Task].unattemptRuntime.strengthL("arg" + idx) }
            _     <- insert(
              coll,
              List(Bson.Doc(argsBson.toListMap)).map(_.repr)).run(setupClient)

            lp = prg(
              (0 until args.length).toList.map(idx =>
                Fix(StructuralLib.MapProject(
                  lpf.free('hole),
                  lpf.constant(Data.Str("arg" + idx))))))
            t  <- compile(qm, coll, translate[Hole](lp, _ => SrcHole)).point[Task].unattemptRuntime
            (wf, resultField) = t
            rez <- evaluate(wf, coll).run(testClient)

            _     <- dropCollection(coll).run(setupClient)
          } yield {
            rez must beSingleResult(beCloseTo(massageExpected(expected)))
          }).timed(5.seconds)(Strategy.DefaultTimeoutScheduler).unsafePerformSync.toResult)

      // TODO: Currently still using the old MapFuncStdLibSpec API. The new one
      //       doesn’t mesh well with the general approach of the Mongo version.
      //       Need to revisit later, and hopefully come up with a better one.

      def nullaryMapFunc(prg: FreeMapA[Fix, Nothing], expected: Data) =
        sys.error("impossible!")

      def unaryMapFunc(prg: FreeMapA[Fix, UnaryArg], arg: Data, expected: Data) =
        sys.error("impossible!")

      def binaryMapFunc
        (prg: FreeMapA[Fix, BinaryArg], arg1: Data, arg2: Data, expected: Data) =
        sys.error("impossible!")

      def ternaryMapFunc
        (prg: FreeMapA[Fix, TernaryArg],
          arg1: Data, arg2: Data, arg3: Data,
          expected: Data) =
        sys.error("impossible!")

      override def nullary(
        prg: Fix[LP],
        expected: Data): Result =
        run(Nil, κ(prg), expected)

      override def unary(
        prg: Fix[LP] => Fix[LP],
        arg: Data,
        expected: Data): Result =
        run(List(arg), { case List(arg) => prg(arg) }, expected)

      override def binary(
        prg: (Fix[LP], Fix[LP]) => Fix[LP],
        arg1: Data, arg2: Data,
        expected: Data): Result =
        run(List(arg1, arg2), { case List(arg1, arg2) => prg(arg1, arg2) }, expected)

      override def ternary(
        prg: (Fix[LP], Fix[LP], Fix[LP]) => Fix[LP],
        arg1: Data, arg2: Data, arg3: Data,
        expected: Data): Result =
        run(List(arg1, arg2, arg3), { case List(arg1, arg2, arg3) => prg(arg1, arg2, arg3) }, expected)
    }

    backend.name should tests(runner)

    s"advertised version of ${backend.name}" should {
      "match with real version" in {
        advertisedVersion(BackendName(backend.name)) must_===
          MongoDbIO.serverVersion.run(testClient).map(MongoQueryModel(_).some).unsafePerformSync
      }
    }
  }
}
