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

package quasar.physical.marklogic

import quasar.SKI.κ
import quasar.NameGenerator
import quasar.fp.{freeCataM, interpretM, ShowT}
import quasar.qscript._
import quasar.physical.marklogic.xquery.{PrologW, XQuery}

import matryoshka.Recursive
import scalaz._, Scalaz._

package object qscript {
  type MonadPlanErr[F[_]]            = MonadError_[F, MarkLogicPlannerError]
  type MarkLogicPlanErrT[F[_], A]    = EitherT[F, MarkLogicPlannerError, A]
  type MarkLogicPlanner[F[_], QS[_]] = Planner[F, QS, XQuery]

  object MarkLogicPlanner {
    def apply[F[_], QS[_]](implicit MLP: MarkLogicPlanner[F, QS]): MarkLogicPlanner[F, QS] = MLP

    implicit def qScriptCore[F[_]: NameGenerator: PrologW: MonadPlanErr, T[_[_]]: Recursive: ShowT]: MarkLogicPlanner[F, QScriptCore[T, ?]] =
      new QScriptCorePlanner[F, T]

    implicit def constDeadEnd[F[_]: Applicative]: MarkLogicPlanner[F, Const[DeadEnd, ?]] =
      new DeadEndPlanner[F]

    implicit def constRead[F[_]: Applicative]: MarkLogicPlanner[F, Const[Read, ?]] =
      new ReadPlanner[F]

    implicit def constShiftedRead[F[_]: NameGenerator: PrologW]: MarkLogicPlanner[F, Const[ShiftedRead, ?]] =
      new ShiftedReadPlanner[F]

    implicit def projectBucket[F[_]: Applicative, T[_[_]]]: MarkLogicPlanner[F, ProjectBucket[T, ?]] =
      new ProjectBucketPlanner[F, T]

    implicit def thetajoin[F[_]: NameGenerator: PrologW: MonadPlanErr, T[_[_]]: Recursive: ShowT]: MarkLogicPlanner[F, ThetaJoin[T, ?]] =
      new ThetaJoinPlanner[F, T]

    implicit def equiJoin[F[_]: Applicative, T[_[_]]]: MarkLogicPlanner[F, EquiJoin[T, ?]] =
      new EquiJoinPlanner[F, T]
  }

  def mapFuncXQuery[T[_[_]]: Recursive: ShowT, F[_]: NameGenerator: PrologW: MonadPlanErr](fm: FreeMap[T], src: XQuery): F[XQuery] =
    planMapFunc[T, F, Hole](fm)(κ(src))

  def mergeXQuery[T[_[_]]: Recursive: ShowT, F[_]: NameGenerator: PrologW: MonadPlanErr](jf: JoinFunc[T], l: XQuery, r: XQuery): F[XQuery] =
    planMapFunc[T, F, JoinSide](jf) {
      case LeftSide  => l
      case RightSide => r
    }

  def planMapFunc[T[_[_]]: Recursive: ShowT, F[_]: NameGenerator: PrologW: MonadPlanErr, A](
    freeMap: Free[MapFunc[T, ?], A])(
    recover: A => XQuery
  ): F[XQuery] =
    freeCataM(freeMap)(interpretM(a => recover(a).point[F], MapFuncPlanner[T, F]))

  def rebaseXQuery[T[_[_]]: Recursive: ShowT, F[_]: NameGenerator: PrologW: MonadPlanErr](
    fqs: FreeQS[T], src: XQuery
  ): F[XQuery] = {
    import MarkLogicPlanner._
    freeCataM(fqs)(interpretM(κ(src.point[F]), Planner[F, QScriptTotal[T, ?], XQuery].plan))
  }
}
