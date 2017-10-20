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

package quasar.physical.rdbms.planner

import slamdata.Predef._
import quasar.fp.ski._
import quasar.Planner._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.qscript._
import quasar.NameGenerator
import quasar.physical.rdbms.planner.sql.{MapFuncCorePlanner, MapFuncDerivedPlanner, QScriptCorePlanner, SqlExpr}
import matryoshka._

import scalaz._

trait Planner[T[_[_]], F[_], QS[_]] extends Serializable {

  def plan: AlgebraM[F, QS, T[SqlExpr]]
}

object Planner {

  def apply[T[_[_]], F[_], QS[_]](
      implicit P: Planner[T, F, QS]): Planner[T, F, QS] = P

  implicit def constDeadEndPlanner[T[_[_]], F[_]: PlannerErrorME]
    : Planner[T, F, Const[DeadEnd, ?]] =
    unreachable("deadEnd")

  implicit def constReadPlanner[T[_[_]], F[_]: PlannerErrorME, A]
    : Planner[T, F, Const[Read[A], ?]] =
    unreachable("read")

  implicit def constShiftedReadDirPlanner[T[_[_]], F[_]: PlannerErrorME]
    : Planner[T, F, Const[ShiftedRead[ADir], ?]] =
    unreachable("shifted read of a dir")

  implicit def constShiftedReadFilePlanner[
  T[_[_]]: CorecursiveT,
  F[_]: Applicative: NameGenerator]
  : Planner[T, F, Const[ShiftedRead[AFile], ?]] =
    new ShiftedReadPlanner[T, F]

  implicit def projectBucketPlanner[T[_[_]]: RecursiveT: ShowT, F[_]: PlannerErrorME]
  : Planner[T, F, ProjectBucket[T, ?]] =
    unreachable("projectBucket")


  implicit def thetaJoinPlanner[T[_[_]]: RecursiveT: ShowT, F[_]: PlannerErrorME]
  : Planner[T, F, ThetaJoin[T, ?]] = unreachable("thetajoin")

  def mapFuncPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Applicative: Monad: NameGenerator: PlannerErrorME]
  : Planner[T, F, MapFunc[T, ?]] = {
    val core = new MapFuncCorePlanner[T, F]
    coproduct(core, new MapFuncDerivedPlanner(core))
  }

  implicit def qScriptCorePlanner[
  T[_[_]]: BirecursiveT: ShowT,
  F[_]: Monad: NameGenerator: PlannerErrorME]
: Planner[T, F, QScriptCore[T, ?]] = new QScriptCorePlanner[T, F](mapFuncPlanner)

  implicit def equiJoinPlanner[
  T[_[_]]: BirecursiveT: ShowT,
  F[_]: Monad: NameGenerator: PlannerErrorME]
  : Planner[T, F, EquiJoin[T, ?]] = unreachable("equijoin")

  implicit def coproduct[T[_[_]], N[_], F[_], G[_]](
                                                     implicit F: Planner[T, N, F], G: Planner[T, N, G]
                                                   ): Planner[T, N, Coproduct[F, G, ?]] =
    new Planner[T, N, Coproduct[F, G, ?]] {
      val plan: AlgebraM[N, Coproduct[F, G, ?], T[SqlExpr]] =
        _.run.fold(F.plan, G.plan)
    }

  private def unreachable[T[_[_]], F[_]: PlannerErrorME, QS[_]](
      what: String): Planner[T, F, QS] =
    new Planner[T, F, QS] {
      override def plan: AlgebraM[F, QS, T[SqlExpr]] =
        κ(
          PlannerErrorME[F].raiseError(
            InternalError.fromMsg(s"unreachable $what")))
    }

}
