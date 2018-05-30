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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.{RenderTree, RenderTreeT}
import quasar.contrib.pathy.AFile
import quasar.fs.MonadFsErr
import quasar.fs.{Planner => QPlanner, _}, QPlanner._
import quasar.physical.mongodb._
import quasar.physical.mongodb.WorkflowBuilder._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.planner.workflow._
import quasar.physical.mongodb.workflow.{ExcludeId => _, IncludeId => _, _}
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

class ShiftedReadPlanner[T[_[_]]: BirecursiveT: ShowT: RenderTreeT] extends
  Planner[Const[ShiftedRead[AFile], ?]] {

  type IT[G[_]] = T[G]

  def plan
    [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
    (cfg: PlannerConfig[T, EX, WF, M])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: RenderTree[WorkflowBuilder[WF]],
      WB: WorkflowBuilder.Ops[WF],
      ev3: ExprOpCoreF :<: EX,
      ev4: EX :<: ExprOp) =
    qs => Collection
      .fromFile(qs.getConst.path)
      .fold(
        e => raisePlannerError(PlanPathError(e)),
        coll => {
          val dataset = WB.read(coll)
          // TODO: exclude `_id` from the value here?
          qs.getConst.idStatus match {
            case IdOnly    =>
              getExprBuilder[T, M, WF, EX](
                cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(
                dataset,
                  Free.roll(MFC(MapFuncsCore.ProjectKey[T, FreeMap[T]](HoleF[T], MapFuncsCore.StrLit("_id")))))
            case IncludeId =>
              getExprBuilder[T, M, WF, EX](
                cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(
                dataset,
                  MapFuncCore.StaticArray(List(
                    Free.roll(MFC(MapFuncsCore.ProjectKey[T, FreeMap[T]](HoleF[T], MapFuncsCore.StrLit("_id")))),
                    HoleF)))
            case ExcludeId => dataset.point[M]
          }
        })
}
