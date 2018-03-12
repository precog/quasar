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

package quasar.fs

import slamdata.Predef._
import quasar.Data
import quasar.common.PhaseResult
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fs.QueryFile._
import quasar.frontend.logicalplan.{Constant, LogicalPlan}

import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

object constantPlans {

  /** Identify plans which reduce to a (set of) constant value(s). */
  def asConstant(lp: Fix[LogicalPlan]): Option[List[Data]] =
    lp.project match {
      case Constant(value)             => List(value).some
      case _                           => none
    }

  type State[A] = KeyValueStore[QueryFile.ResultHandle, Vector[Data], A]

  val constantPhase =
    PhaseResult.detail("Intercept Constant", "This plan is constant and can be evaluated in memory")

  def queryFile[S[_]](
    implicit
    S0: QueryFile :<: S,
    S1: ManageFile :<: S,
    seq: MonotonicSeq.Ops[S],
    write: WriteFile.Ops[S],
    state: KeyValueStore.Ops[QueryFile.ResultHandle, Vector[Data], S]
  ): QueryFile ~> Free[S, ?] = {

    val query = QueryFile.Ops[S]
    val queryUnsafe = QueryFile.Unsafe[S]

    def dataHandle(data: List[Data]): Free[S, ResultHandle] =
      for {
        h <- seq.next.map(ResultHandle(_))
        _ <- state.put(h, data.toVector)
      } yield h

    λ[QueryFile ~> Free[S, ?]] {
      case ExecutePlan(lp, out) =>
        asConstant(lp).fold(
          query.execute(lp, out).run.run)(
          data => write.saveThese(out, data.toVector).run.strengthL(Vector(constantPhase)))

      case EvaluatePlan(lp) =>
        asConstant(lp).fold(
          queryUnsafe.eval(lp).run.run)(
          data => dataHandle(data).map(h => (Vector(constantPhase), h.right)))

      case More(handle) =>
        state.get(handle).run.flatMap {
          case Some(data) => state.put(handle, Vector.empty).as(data.right)
          case None       => queryUnsafe.more(handle).run
        }

      case Close(handle) =>
        state.contains(handle).ifM(state.delete(handle), queryUnsafe.close(handle))

      case Explain(lp) =>
        val constantExecutionPlan =
          ExecutionPlan(FileSystemType("constant"), "none", ISet.empty)
        asConstant(lp).fold(
          query.explain(lp).run.run)(
          data => (Vector(constantPhase), constantExecutionPlan.right[FileSystemError]).point[Free[S, ?]])

      case ListContents(dir) =>
        query.listContents(dir).run

      case FileExists(file) =>
        query.fileExists(file)
    }
  }
}