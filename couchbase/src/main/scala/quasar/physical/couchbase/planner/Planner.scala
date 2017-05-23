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

package quasar.physical.couchbase.planner

import quasar.NameGenerator
import quasar.common.PhaseResultT
import quasar.contrib.pathy.{ADir, AFile}
import quasar.physical.couchbase._, common._
import quasar.qscript._

import matryoshka._
import scalaz._

abstract class Planner[T[_[_]], F[_], QS[_]] {
  type M[A]  = CBPhaseLog[F, A]
  type PR[A] = PhaseResultT[F, A]

  def plan: AlgebraM[M, QS, T[N1QL]]
}

object Planner {
  def apply[T[_[_]], F[_], QS[_]](implicit ev: Planner[T, F, QS]): Planner[T, F, QS] = ev

  implicit def coproduct[T[_[_]], N[_]: Monad, F[_], G[_]](
    implicit F: Planner[T, N, F], G: Planner[T, N, G]
  ): Planner[T, N, Coproduct[F, G, ?]] =
    new Planner[T, N, Coproduct[F, G, ?]] {
      val plan: AlgebraM[M, Coproduct[F, G, ?], T[N1QL]] =
        _.run.fold(F.plan, G.plan)
    }

  implicit def constDeadEndPlanner[T[_[_]], F[_]: Monad]
    : Planner[T, F, Const[DeadEnd, ?]] =
    new UnreachablePlanner[T, F, Const[DeadEnd, ?]]

  implicit def constReadPlanner[T[_[_]], F[_]: Monad, A]
    : Planner[T, F, Const[Read[A], ?]] =
    new UnreachablePlanner[T, F, Const[Read[A], ?]]

  implicit def constShiftedReadDirPlanner[T[_[_]], F[_]: Monad]
    : Planner[T, F, Const[ShiftedRead[ADir], ?]] =
    new UnreachablePlanner[T, F, Const[ShiftedRead[ADir], ?]]

  implicit def constShiftedReadFile[T[_[_]]: CorecursiveT, F[_]: Monad: ContextReader: NameGenerator]
    : Planner[T, F, Const[ShiftedRead[AFile], ?]] =
    new ShiftedReadFilePlanner[T, F]

  implicit def equiJoinPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: ContextReader: NameGenerator]
    : Planner[T, F, EquiJoin[T, ?]] =
    new EquiJoinPlanner[T, F]

  def mapFuncPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: NameGenerator]
    : Planner[T, F, MapFunc[T, ?]] =
    new MapFuncPlanner[T, F]

  implicit def projectBucketPlanner[T[_[_]]: RecursiveT: ShowT, F[_]: Monad: NameGenerator]
    : Planner[T, F, ProjectBucket[T, ?]] =
    new UnreachablePlanner[T, F, ProjectBucket[T, ?]]

  implicit def qScriptCorePlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: ContextReader: NameGenerator]
    : Planner[T, F, QScriptCore[T, ?]] =
    new QScriptCorePlanner[T, F]

  def reduceFuncPlanner[T[_[_]]: CorecursiveT, F[_]: Monad]
    : Planner[T, F, ReduceFunc] =
    new ReduceFuncPlanner[T, F]

  implicit def thetaJoinPlanner[T[_[_]]: RecursiveT: ShowT, F[_]: Monad: NameGenerator]
    : Planner[T, F, ThetaJoin[T, ?]] =
    new UnreachablePlanner[T, F, ThetaJoin[T, ?]]

}
