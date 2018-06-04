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
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.iota.mkInject
import quasar.fp.ski._
import quasar.fs.MonadFsErr
import quasar.physical.mongodb.WorkflowBuilder, WorkflowBuilder._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.workflow._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

trait Planner[F[_]] {
  type IT[G[_]]

  def plan
    [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
    (cfg: PlannerConfig[IT, EX, WF, M])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: RenderTree[WorkflowBuilder[WF]],
      ev2: WorkflowBuilder.Ops[WF],
      ev3: ExprOpCoreF :<: EX,
      ev4: EX :<: ExprOp):
      AlgebraM[M, F, WorkflowBuilder[WF]]
}

object Planner {
  type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }

  def apply[T[_[_]], F[_]](implicit ev: Planner.Aux[T, F]) = ev

  implicit def shiftedReadFile[T[_[_]]: BirecursiveT: ShowT: RenderTreeT]
      : Planner.Aux[T, Const[ShiftedRead[AFile], ?]] =
    new ShiftedReadPlanner[T]

  implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]:
      Planner.Aux[T, QScriptCore[T, ?]] =
    new QScriptCorePlanner[T]

  implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]:
      Planner.Aux[T, EquiJoin[T, ?]] =
    new EquiJoinPlanner[T]

  implicit def copk[T[_[_]], LL <: TListK](implicit M: Materializer[T, LL]): Planner.Aux[T, CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[T[_[_]], LL <: TListK] {
    def materialize(offset: Int): Planner.Aux[T, CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T[_[_]], F[_]](
      implicit
      F: Planner.Aux[T, F]
    ): Materializer[T, F ::: TNilK] = new Materializer[T, F ::: TNilK] {
      override def materialize(offset: Int): Planner.Aux[T, CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new Planner[CopK[F ::: TNilK, ?]] {
          type IT[G[_]] = T[G]
          def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF, M])
            (implicit
              ev0: WorkflowOpCoreF :<: WF,
              ev1: RenderTree[WorkflowBuilder[WF]],
              ev2: WorkflowBuilder.Ops[WF],
              ev3: ExprOpCoreF :<: EX,
              ev4: EX :<: ExprOp) = {
            case I(fa) => F.plan[M, WF, EX](cfg).apply(fa)
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T[_[_]], F[_], LL <: TListK](
      implicit
      F: Planner.Aux[T, F],
      LL: Materializer[T, LL]
    ): Materializer[T, F ::: LL] = new Materializer[T, F ::: LL] {
      override def materialize(offset: Int): Planner.Aux[T, CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new Planner[CopK[F ::: LL, ?]] {
          type IT[G[_]] = T[G]
          def plan
            [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
            (cfg: PlannerConfig[T, EX, WF, M])
            (implicit
              ev0: WorkflowOpCoreF :<: WF,
              ev1: RenderTree[WorkflowBuilder[WF]],
              ev2: WorkflowBuilder.Ops[WF],
              ev3: ExprOpCoreF :<: EX,
              ev4: EX :<: ExprOp) = {
                case I(fa) => F.plan[M, WF, EX](cfg).apply(fa)
                case other => LL.materialize(offset + 1).plan[M, WF, EX](cfg).apply(other.asInstanceOf[CopK[LL, WorkflowBuilder[WF]]])
              }
        }
      }
    }
  }

  // TODO: All instances below here only need to exist because of `FreeQS`,
  //       but can’t actually be called.

  def default[T[_[_]], F[_]](label: String): Planner.Aux[T, F] =
    new Planner[F] {
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
        κ(raiseInternalError(s"should not be reached: $label"))
    }

  implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] =
    default("DeadEnd")

  implicit def read[T[_[_]], A]: Planner.Aux[T, Const[Read[A], ?]] =
    default("Read")

  implicit def shiftedReadDir[T[_[_]]]: Planner.Aux[T, Const[ShiftedRead[ADir], ?]] =
    default("ShiftedRead[ADir]")

  implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] =
    default("ThetaJoin")

  implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] =
    default("ProjectBucket")
}
