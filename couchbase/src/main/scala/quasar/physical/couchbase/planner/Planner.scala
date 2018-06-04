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

package quasar.physical.couchbase.planner

import slamdata.Predef._

import quasar.effect.NameGenerator
import quasar.contrib.pathy.{ADir, AFile}
import quasar.physical.couchbase._, common._
import quasar.fs.Planner.PlannerErrorME
import quasar.qscript._
import quasar.contrib.iota.mkInject

import matryoshka._
import scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

abstract class Planner[T[_[_]], F[_], QS[_]] {
  def plan: AlgebraM[F, QS, T[N1QL]]
}

object Planner {
  def apply[T[_[_]], F[_], QS[_]](implicit ev: Planner[T, F, QS]): Planner[T, F, QS] = ev

  implicit def copk[T[_[_]], N[_], LL <: TListK](implicit M: Materializer[T, N, LL]): Planner[T, N, CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[T[_[_]], N[_], LL <: TListK] {
    def materialize(offset: Int): Planner[T, N, CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T[_[_]], N[_], F[_]](
      implicit
      F: Planner[T, N, F]
    ): Materializer[T, N, F ::: TNilK] = new Materializer[T, N, F ::: TNilK] {
      override def materialize(offset: Int): Planner[T, N, CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new Planner[T, N, CopK[F ::: TNilK, ?]] {
          val plan: AlgebraM[N, CopK[F ::: TNilK, ?], T[N1QL]] = {
            case I(fa) => F.plan(fa)
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T[_[_]], N[_], F[_], LL <: TListK](
      implicit
      F: Planner[T, N, F],
      LL: Materializer[T, N, LL]
    ): Materializer[T, N, F ::: LL] = new Materializer[T, N, F ::: LL] {
      override def materialize(offset: Int): Planner[T, N, CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new Planner[T, N, CopK[F ::: LL, ?]] {
          val plan: AlgebraM[N, CopK[F ::: LL, ?], T[N1QL]] = {
            case I(fa) => F.plan(fa)
            case other => LL.materialize(offset + 1).plan(other.asInstanceOf[CopK[LL, T[N1QL]]])
          }
        }
      }
    }
  }

  implicit def constDeadEndPlanner[T[_[_]], F[_]: PlannerErrorME]
    : Planner[T, F, Const[DeadEnd, ?]] =
    new UnreachablePlanner[T, F, Const[DeadEnd, ?]]

  implicit def constReadPlanner[T[_[_]], F[_]: PlannerErrorME, A]
    : Planner[T, F, Const[Read[A], ?]] =
    new UnreachablePlanner[T, F, Const[Read[A], ?]]

  implicit def constShiftedReadDirPlanner[T[_[_]], F[_]: PlannerErrorME]
    : Planner[T, F, Const[ShiftedRead[ADir], ?]] =
    new UnreachablePlanner[T, F, Const[ShiftedRead[ADir], ?]]

  implicit def constShiftedReadFilePlanner[
    T[_[_]]: CorecursiveT,
    F[_]: Applicative: ContextReader: NameGenerator]
    : Planner[T, F, Const[ShiftedRead[AFile], ?]] =
    new ShiftedReadFilePlanner[T, F]

  implicit def equiJoinPlanner[
    T[_[_]]: BirecursiveT: ShowT,
    F[_]: Monad: ContextReader: NameGenerator: PlannerErrorME]
    : Planner[T, F, EquiJoin[T, ?]] =
    new EquiJoinPlanner[T, F]

  def mapFuncPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Applicative: Monad: NameGenerator: PlannerErrorME]
    : Planner[T, F, MapFunc[T, ?]] = {
    val core = new MapFuncCorePlanner[T, F]
    val derived = new MapFuncDerivedPlanner(core)
    copk(Materializer.induct(core, Materializer.base(derived)))
  }

  implicit def projectBucketPlanner[T[_[_]]: RecursiveT: ShowT, F[_]: PlannerErrorME]
    : Planner[T, F, ProjectBucket[T, ?]] =
    new UnreachablePlanner[T, F, ProjectBucket[T, ?]]

  implicit def qScriptCorePlanner[
    T[_[_]]: BirecursiveT: ShowT,
    F[_]: Monad: ContextReader: NameGenerator: PlannerErrorME]
    : Planner[T, F, QScriptCore[T, ?]] =
    new QScriptCorePlanner[T, F]

  def reduceFuncPlanner[T[_[_]]: CorecursiveT, F[_]: Applicative]
    : Planner[T, F, ReduceFunc] =
    new ReduceFuncPlanner[T, F]

  implicit def thetaJoinPlanner[T[_[_]]: RecursiveT: ShowT, F[_]: PlannerErrorME]
    : Planner[T, F, ThetaJoin[T, ?]] =
    new UnreachablePlanner[T, F, ThetaJoin[T, ?]]
}
