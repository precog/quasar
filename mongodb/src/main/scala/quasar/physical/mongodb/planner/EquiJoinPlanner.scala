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
import quasar._
import quasar.fs.MonadFsErr
import quasar.physical.mongodb._
import quasar.physical.mongodb.WorkflowBuilder._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.planner.workflow._
import quasar.physical.mongodb.workflow._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

class EquiJoinPlanner[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends
    Planner[EquiJoin[T, ?]] {

  type IT[G[_]] = T[G]

  def plan
    [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
    (cfg: PlannerConfig[T, EX, WF, M])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: RenderTree[WorkflowBuilder[WF]],
      ev2: WorkflowBuilder.Ops[WF],
      ev3: ExprOpCoreF :<: EX,
      ev4: EX :<: ExprOp) =
    qs =>
  (rebaseWB[T, M, WF, EX](cfg, qs.lBranch, qs.src) ⊛
    rebaseWB[T, M, WF, EX](cfg, qs.rBranch, qs.src))(
    (lb, rb) => {
      val (lKey, rKey) = Unzip[List].unzip(qs.key)

      (lKey.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _)) ⊛
        rKey.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _)))(
        (lk, rk) =>
        liftM[M, WorkflowBuilder[WF]](cfg.joinHandler.run(
          qs.f,
          JoinSource(lb, lk),
          JoinSource(rb, rk))) >>=
          (getExprBuilder[T, M, WF, EX](cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(_, qs.combine >>= {
            case LeftSide => Free.roll(MFC(MapFuncsCore.ProjectKey(HoleF, MapFuncsCore.StrLit("left"))))
            case RightSide => Free.roll(MFC(MapFuncsCore.ProjectKey(HoleF, MapFuncsCore.StrLit("right"))))
          }))).join
    }).join
}
