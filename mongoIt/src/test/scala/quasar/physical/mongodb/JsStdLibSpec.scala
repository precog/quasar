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
import quasar.fs.FileSystemError
import quasar.std.StdLib._
import quasar.physical.mongodb.workflow._
import quasar.physical.mongodb.planner.{javascript => js}
import quasar.qscript._
import quasar.time.TemporalPart

import java.time.{
  Instant,
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime
}
import matryoshka._
import matryoshka.data.Fix
import org.specs2.execute.{Pending, Result, Success}
import scalaz.{Name => _, Success => _, _}, Scalaz._
import shapeless.Nat

/** Test the implementation of the standard library for MongoDb's map-reduce
  * (i.e. JavaScript).
  */
class MongoDbJsStdLibSpec extends MongoDbStdLibSpec {
  /** Identify constructs that are expected not to be implemented in JS. */
  def shortCircuit[N <: Nat](backend: BackendName, func: GenericFunc[N], args: List[Data]): Result \/ Unit = (func, args) match {
    /* DATE */
    case (date.ExtractDayOfYear, _) => Pending("TODO").left
    case (date.ExtractIsoYear, _) => Pending("TODO").left
    case (date.ExtractWeek, _) => Pending("TODO").left

    case (date.ExtractHour, Data.LocalTime(_) :: Nil) => Pending("TODO").left
    case (date.ExtractMicrosecond, Data.LocalTime(_) :: Nil) => Pending("TODO").left
    case (date.ExtractMillisecond, Data.LocalTime(_) :: Nil) => Pending("TODO").left
    case (date.ExtractMinute, Data.LocalTime(_) :: Nil) => Pending("TODO").left
    case (date.ExtractSecond, Data.LocalTime(_) :: Nil) => Pending("TODO").left

    // Not working for year < 1 for any date-like types, but we just have tests for LocalDate
    case (date.ExtractCentury, List(Data.LocalDate(d))) if d.getYear < 1 => Pending("TODO").left
    case (date.ExtractDecade, List(Data.LocalDate(d))) if d.getYear < 1 => Pending("TODO").left
    case (date.ExtractMillennium, List(Data.LocalDate(d))) if d.getYear < 1 => Pending("TODO").left

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

    // https://jira.mongodb.org/browse/SERVER-8164
    // mongo 3.5.13 fixes this issue
    // we Success these so the property tests don't abort early
    case (date.LocalDate, Data.Str(date) :: Nil)
        if JLocalDate.parse(date).getYear < 100 &&
          (advertisedVersion(backend) lt MongoQueryModel.`3.6`.some) =>
      Success().updateExpected("Actually skipped due to mongo bug SERVER-8164 (Fixed in 3.5.13).").left

    case (date.LocalDateTime, Data.Str(date) :: Nil)
        if JLocalDateTime.parse(date).getYear < 100 &&
          (advertisedVersion(backend) lt MongoQueryModel.`3.6`.some) =>
      Success().updateExpected("Actually skipped due to mongo bug SERVER-8164 (Fixed in 3.5.13).").left

    case (date.Interval, _) => Pending("TODO").left

    /* MATH */
    case (math.Power, Data.Number(x) :: Data.Number(y) :: Nil)
        if x == 0 && y < 0 =>
      Pending("Infinity is not translated properly?").left

    case (math.Add, List(Data.DateTimeLike(_), Data.DateTimeLike(_))) => Pending("TODO").left
    case (math.Subtract, List(Data.DateTimeLike(_), Data.DateTimeLike(_))) => Pending("TODO").left
    case (math.Multiply, List(Data.Interval(_), _)) => Pending("TODO").left
    case (math.Multiply, List(_, Data.Interval(_))) => Pending("TODO").left

    /* RELATIONS */
    case (relations.IfUndefined, _) => Pending("TODO").left

    case (relations.Eq, List(Data.Interval(_), Data.Interval(_))) => Pending("TODO").left
    case (relations.Neq, List(Data.Interval(_), Data.Interval(_))) => Pending("TODO").left

    case (relations.Between, List(Data.Interval(_), Data.Interval(_), Data.Interval(_))) =>
      Pending("TODO").left

    case (relations.And, List(Data.NA, _)) => Pending("TODO handle and/or with outer semantics").left
    case (relations.And, List(_, Data.NA)) => Pending("TODO handle and/or with outer semantics").left
    case (relations.Or, List(Data.NA, _)) => Pending("TODO handle and/or with outer semantics").left
    case (relations.Or, List(_, Data.NA)) => Pending("TODO handle and/or with outer semantics").left

    /* SET */
    case (quasar.std.SetLib.Range, _) => Pending("TODO").left
    case (quasar.std.SetLib.Within, _) => Pending("TODO").left

    /* STRING */
    case (string.Lower, _) => Pending("TODO").left
    case (string.Upper, _) => Pending("TODO").left

    case (string.ToString, Data.Dec(_) :: Nil) =>
      Pending("Dec printing doesn't match precisely").left

    case (string.ToString, List(Data.DateTimeLike(_))) =>
      Pending("Works but isn't formatted as expected.").left

    /* STRUCTURAL */
    case (structural.ConcatOp, _) => Pending("TODO").left
    case (structural.MapProject, _) => Pending("TODO").left

    case _ => ().right
  }

  def temporalTruncSupported(backend: BackendName, part: TemporalPart): Boolean =
    part match {
      case TemporalPart.Microsecond => false
      case _ => true
    }

  def compile(queryModel: MongoQueryModel, coll: Collection, mf: FreeMap[Fix])
      : FileSystemError \/ (Crystallized[WorkflowF], BsonField.Name) = {
    js.getJsFn[Fix, ReaderT[FileSystemError \/ ?, Instant, ?]].apply(mf).run(runAt) ∘
      (js =>
        (Crystallize[WorkflowF].crystallize(
          chain[Fix[WorkflowF]](
            $read(coll),
            $simpleMap(NonEmptyList(MapExpr(js)), ListMap.empty))),
          BsonField.Name("value")))
  }
}
