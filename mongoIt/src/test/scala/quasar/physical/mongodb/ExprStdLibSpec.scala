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
import quasar.contrib.scalaz._
import quasar.fs.FileSystemError, FileSystemError.qscriptPlanningFailed
import quasar.fs.Planner.PlannerError
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.workflow._
import quasar.physical.mongodb.WorkflowBuilder._
import quasar.qscript._
import quasar.qscript.rewrites.{Coalesce => _}
import quasar.std.StdLib._
import quasar.time.TemporalPart

import java.time.{Instant, LocalDateTime => JLocalDateTime}
import java.time.temporal.ChronoUnit

import matryoshka._
import matryoshka.data.Fix
import org.specs2.execute._
import scalaz.{Name => _, _}, Scalaz._
import shapeless.Nat

/** Test the implementation of the standard library for MongoDb's aggregation
  * pipeline (aka ExprOp).
  */
class MongoDbExprStdLibSpec extends MongoDbStdLibSpec {
  import MongoQueryModel._

  val notHandled = Skipped("Not implemented in aggregation.")
  def notImplBefore(v: MongoQueryModel) = Pending(s"not implemented in aggregation on MongoDB < ${v.shows}")
  def notImplBeforeSkipped(v: MongoQueryModel) = Skipped(s"not implemented in aggregation on MongoDB < ${v.shows}")

  /** Identify constructs that are expected not to be implemented in the pipeline. */
  def shortCircuit[N <: Nat](backend: BackendName, func: GenericFunc[N], args: List[Data]): Result \/ Unit = (func, args) match {
    /* DATE */
    case (date.ExtractIsoYear, _) if advertisedVersion(backend) lt `3.6`.some =>
      notImplBefore(`3.6`).left
    // Not working for year < 1 for any date-like types, but we just have tests for LocalDate
    case (date.ExtractIsoYear, List(Data.LocalDate(d)))
      if d.getYear < 1 && advertisedVersion(backend) === `3.6`.some =>
        Pending("TODO").left

    case (date.ExtractWeek, _) if advertisedVersion(backend) lt `3.6`.some  =>
      notImplBefore(`3.6`).left

    case (date.ExtractHour, Data.LocalTime(_) :: Nil) => Pending("TODO").left
    case (date.ExtractMicrosecond, Data.LocalTime(_) :: Nil) => Pending("TODO").left
    case (date.ExtractMillisecond, Data.LocalTime(_) :: Nil) => Pending("TODO").left
    case (date.ExtractMinute, Data.LocalTime(_) :: Nil) => Pending("TODO").left
    case (date.ExtractSecond, Data.LocalTime(_) :: Nil) => Pending("TODO").left

    // Not working for year < 1 for any date-like types, but we just have tests for LocalDate
    case (date.ExtractCentury, List(Data.LocalDate(d))) if d.getYear < 1 => Pending("TODO").left
    case (date.ExtractDecade, List(Data.LocalDate(d))) if d.getYear < 1 => Pending("TODO").left
    case (date.ExtractMillennium, List(Data.LocalDate(d))) if d.getYear < 1 => Pending("TODO").left

    case (date.StartOfDay, _) if advertisedVersion(backend) lt `3.6`.some =>
      notImplBefore(`3.6`).left

    case (date.Now, _) => Pending("Returns correct result, but wrapped into Data.Dec instead of Data.Interval").left
    case (date.NowDate, _) => Pending("TODO").left
    case (date.NowTime, _) => Pending("TODO").left
    case (date.CurrentTimeZone, _) => noTimeZoneSupport.left

    case (date.SetTimeZone, _) => noTimeZoneSupport.left
    case (date.SetTimeZoneHour, _) => noTimeZoneSupport.left
    case (date.SetTimeZoneMinute, _) => noTimeZoneSupport.left

    case (date.OffsetDate, _) => noTimeZoneSupport.left
    case (date.OffsetDateTime, _) => noTimeZoneSupport.left
    case (date.OffsetTime, _) => noTimeZoneSupport.left

    case (date.LocalDate, _) if advertisedVersion(backend) lt `3.6`.some =>
      notImplBefore(`3.6`).left
    case (date.LocalDateTime, _) if advertisedVersion(backend) lt `3.6`.some =>
      notImplBefore(`3.6`).left
    case (date.LocalDateTime, List(Data.Str(s)))
      if JLocalDateTime.parse(s) != JLocalDateTime.parse(s).truncatedTo(ChronoUnit.MILLIS) =>
        Pending("LocalDateTime(s) does not support precision beyond millis in MongoDb 3.6").left
    case (date.LocalTime, _) => notHandled.left

    case (date.Interval, _) => notHandled.left

    /* MATH */
    case (math.Add, List(Data.DateTimeLike(_), Data.DateTimeLike(_))) => Pending("TODO").left
    case (math.Subtract, List(Data.DateTimeLike(_), Data.DateTimeLike(_))) => Pending("TODO").left
    case (math.Multiply, List(Data.Interval(_), _)) => Pending("TODO").left
    case (math.Multiply, List(_, Data.Interval(_))) => Pending("TODO").left

    //FIXME modulo and trunc (which is defined in terms of modulo) cause the
    //mongo docker container to crash (with quite high frequency but not always).
    //One or more of the other tests that are now marked as skipped also seem to
    //cause failures when marked as pending (but with low frequency)
    case (math.Modulo, _) => Skipped("sometimes causes mongo container crash").left
    case (math.Trunc, _) => Skipped("sometimes causes mongo container crash").left

    /* RELATIONS */
    case (relations.And, List(Data.NA, _)) => Pending("TODO handle and/or with outer semantics").left
    case (relations.And, List(_, Data.NA)) => Pending("TODO handle and/or with outer semantics").left
    case (relations.Or, List(Data.NA, _)) => Pending("TODO handle and/or with outer semantics").left
    case (relations.Or, List(_, Data.NA)) => Pending("TODO handle and/or with outer semantics").left

    case (relations.IfUndefined, _) => notHandled.left

    case (relations.Eq, List(Data.Interval(_), Data.Interval(_))) => Pending("TODO").left
    case (relations.Neq, List(Data.Interval(_), Data.Interval(_))) => Pending("TODO").left

    case (relations.Between, List(Data.Interval(_), Data.Interval(_), Data.Interval(_))) =>
      Pending("TODO").left

    /* SET */
    case (quasar.std.SetLib.Range, _) => notHandled.left
    case (quasar.std.SetLib.Within, _) => notHandled.left

    /* STRING */
    case (string.Length, _) if advertisedVersion(backend) lt `3.4`.some =>
      notImplBeforeSkipped(`3.4`).left
    case (string.Integer, _) => notHandled.left
    case (string.Decimal, _) => notHandled.left

    case (string.ToString, List(Data.DateTimeLike(_))) =>
      Pending("implemented but isn't formatted as specified").left

    case (string.Search, _) => notHandled.left
    case (string.Split, _) if advertisedVersion(backend) lt `3.4`.some =>
      notImplBeforeSkipped(`3.4`).left
    case (string.Substring, List(Data.Str(s), _, _)) if
      ((advertisedVersion(backend) lt `3.4`.some) && !isPrintableAscii(s)) =>
        Skipped("only printable ascii supported on MongoDB < 3.4").left

    /* STRUCTURAL */
    case (structural.ConcatOp, _) => notHandled.left
    case (structural.DeleteKey, _) => notHandled.left
    case (structural.MapProject, _) => notHandled.left

    case _ => ().right
  }

  def temporalTruncSupported(backend: BackendName, part: TemporalPart): Boolean =
    if (advertisedVersion(backend) lt MongoQueryModel.`3.6`.some) false
    else
      part match {
        case TemporalPart.Microsecond => false
        case _ => true
      }

  def build[WF[_]: Coalesce: Inject[WorkflowOpCoreF, ?[_]]](
      expr: Fix[ExprOp], queryModel: MongoQueryModel, coll: Collection)(
      implicit RT: RenderTree[WorkflowBuilder[WF]]) =
    WorkflowBuilder.build[PlannerError \/ ?, WF](
      WorkflowBuilder.DocBuilder(WorkflowBuilder.Ops[WF].read(coll),
        ListMap(QuasarSigilName -> \&/-(expr))), queryModel)
      .leftMap(qscriptPlanningFailed.reverseGet(_))

  def compile(queryModel: MongoQueryModel, coll: Collection, mf: FreeMap[Fix])
      : FileSystemError \/ (Crystallized[WorkflowF], BsonField.Name) = {
    type PlanStdT[A] = ReaderT[FileSystemError \/ ?, Instant, A]

    val bsonVersion = MongoQueryModel.toBsonVersion(queryModel)
    queryModel match {
      case MongoQueryModel.`3.6` =>
        (exprOp.getExpr[Fix, PlanStdT, Expr3_6](
          FuncHandler.handle3_6(bsonVersion), StaticHandler.handle).apply(mf).run(runAt) >>= (build[Workflow3_2F](_, queryModel, coll)))
          .map(wf => (Crystallize[Workflow3_2F].crystallize(wf).inject[WorkflowF], QuasarSigilName))

      case MongoQueryModel.`3.4.4` =>
        (exprOp.getExpr[Fix, PlanStdT, Expr3_4_4](
          FuncHandler.handle3_4_4(bsonVersion), StaticHandler.handle).apply(mf).run(runAt) >>= (build[Workflow3_2F](_, queryModel, coll)))
          .map(wf => (Crystallize[Workflow3_2F].crystallize(wf).inject[WorkflowF], QuasarSigilName))

      case MongoQueryModel.`3.4` =>
        (exprOp.getExpr[Fix, PlanStdT, Expr3_4](
          FuncHandler.handle3_4(bsonVersion), StaticHandler.handle).apply(mf).run(runAt) >>= (build[Workflow3_2F](_, queryModel, coll)))
          .map(wf => (Crystallize[Workflow3_2F].crystallize(wf).inject[WorkflowF], QuasarSigilName))

      case MongoQueryModel.`3.2` =>
        (exprOp.getExpr[Fix, PlanStdT, Expr3_2](
          FuncHandler.handle3_2(bsonVersion), StaticHandler.handle).apply(mf).run(runAt) >>= (build[Workflow3_2F](_, queryModel, coll)))
          .map(wf => (Crystallize[Workflow3_2F].crystallize(wf).inject[WorkflowF], QuasarSigilName))
    }
  }
}
