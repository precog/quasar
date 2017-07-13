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
import quasar._
import quasar.fs.FileSystemError
import quasar.std.StdLib._
import quasar.physical.mongodb.workflow._
import quasar.qscript._

import matryoshka._
import matryoshka.data.Fix
import org.specs2.execute._
import scalaz.{Name => _, _}, Scalaz._
import shapeless.Nat

/** Test the implementation of the standard library for MongoDb's map-reduce
  * (i.e. JavaScript).
  */
class MongoDbQJsStdLibSpec extends MongoDbQStdLibSpec {
  /** Identify constructs that are expected not to be implemented in JS. */
  def shortCircuit[N <: Nat](backend: BackendName, func: GenericFunc[N], args: List[Data]): Result \/ Unit = (func, args) match {
    case (string.ToString, Data.Dec(_) :: Nil) =>
      Skipped("Dec printing doesn't match precisely").left
    case (string.ToString, Data.Date(_) :: Nil) =>
      Skipped("Date prints timestamp").left
    case (string.ToString, Data.Interval(_) :: Nil) =>
      Skipped("Interval prints numeric representation").left

    case (math.Power, Data.Number(x) :: Data.Number(y) :: Nil)
        if x == 0 && y < 0 =>
      Skipped("Infinity is not translated properly?").left

    case (date.ExtractDayOfYear, _)    => Skipped("TODO").left
    case (date.ExtractIsoYear, _)      => Skipped("TODO").left
    case (date.ExtractWeek, _)         => Skipped("TODO").left

    case (structural.ConcatOp, _)      => Skipped("TODO").left

    case _                             => ().right
  }

  def shortCircuitTC(args: List[Data]): Result \/ Unit = args match {
    case Data.Date(_) :: Nil => Skipped("TODO").left
    case Data.Time(_) :: Nil => Skipped("TODO").left
    case _                   => ().right
  }

  def compile(queryModel: MongoQueryModel, coll: Collection, mf: FreeMap[Fix])
      : FileSystemError \/ (Crystallized[WorkflowF], BsonField.Name) = {
    MongoDbQScriptPlanner.getJsFn[Fix, FileSystemError \/ ?](mf) ∘
      (js =>
        (Crystallize[WorkflowF].crystallize(
          chain[Fix[WorkflowF]](
            $read(coll),
            $simpleMap(NonEmptyList(MapExpr(js)), ListMap.empty))),
          BsonField.Name("value")))
  }
}
