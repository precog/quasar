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

package quasar.physical.marklogic.qscript

import quasar.contrib.pathy.{ADir, AFile}
import quasar.physical.marklogic.cts.Query
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xcc._
import quasar.ejson.EJson
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._
import shapeless.Lazy

trait Planner[M[_], FMT, F[_], J] {
  def plan[Q](
    implicit Q: Birecursive.Aux[Q, Query[J, ?]]
  ): AlgebraM[M, F, Search[Q] \/ XQuery]

  def planXQuery[Q](
    implicit
    M0: Monad[M],
    M1: PrologW[M],
    O : SearchOptions[FMT],
    SP: StructuralPlanner[M, FMT],
    F : Functor[F],
    J : Birecursive.Aux[J, EJson],
    Q : Birecursive.Aux[Q, Query[J, ?]]
  ): AlgebraM[M, F, XQuery] = {
    Kleisli(elimSearch[Q] _) <==< (plan[Q] <<< F.lift((_: XQuery).right[Search[Q]]))
  }

  protected def elimSearch[Q](x: Search[Q] \/ XQuery)(
    implicit
    Q : Recursive.Aux[Q, Query[J, ?]],
    J : Recursive.Aux[J, EJson],
    M0: Monad[M],
    M1: PrologW[M],
    O : SearchOptions[FMT],
    SP: StructuralPlanner[M, FMT]
  ): M[XQuery] =
    x.fold(Search.plan[M, Q, J, FMT](_, EJsonPlanner.plan[J, M, FMT]), _.point[M])
}

object Planner extends PlannerInstances {

  def apply[M[_], FMT, F[_], J](implicit ev: Planner[M, FMT, F, J]): Planner[M, FMT, F, J] = ev
}

sealed abstract class PlannerInstances extends PlannerInstances0 {
  implicit def coproduct[M[_], FMT, F[_], G[_], J](
    implicit F: Lazy[Planner[M, FMT, F, J]], G: Lazy[Planner[M, FMT, G, J]]
  ): Planner[M, FMT, Coproduct[F, G, ?], J] =
    new Planner[M, FMT, Coproduct[F, G, ?], J] {
      def plan[Q](
        implicit Q: Birecursive.Aux[Q, Query[J, ?]]
      ): AlgebraM[M, Coproduct[F, G, ?], Search[Q] \/ XQuery] =
        _.run.fold(F.value.plan, G.value.plan)
    }
}

sealed abstract class PlannerInstances0 extends PlannerInstances1 {
  implicit def constReadFile[F[_]: Applicative: MonadPlanErr, FMT, J](
    implicit SP: StructuralPlanner[F, FMT]
  ): Planner[F, FMT, Const[Read[AFile], ?], J] =
    new ReadFilePlanner[F, FMT, J]

  implicit def constShiftedReadDir[F[_]: Applicative: MonadPlanErr, FMT, J](
    implicit SP: StructuralPlanner[F, FMT]
  ): Planner[F, FMT, Const[ShiftedRead[ADir], ?], J] =
    new ShiftedReadDirPlanner[F, FMT, J]

  implicit def qScriptCore[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc, FMT: SearchOptions: FilterPlanner[T, ?], T[_[_]]: BirecursiveT](
    implicit
    SP : StructuralPlanner[F, FMT],
    QTP: Lazy[Planner[F, FMT, QScriptTotal[T, ?], T[EJson]]]
  ): Planner[F, FMT, QScriptCore[T, ?], T[EJson]] = {
    implicit val qtp: Planner[F, FMT, QScriptTotal[T, ?], T[EJson]] = QTP.value
    new QScriptCorePlanner[F, FMT, T]
  }

  implicit def thetaJoin[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT: SearchOptions, T[_[_]]: BirecursiveT](
    implicit
    SP : StructuralPlanner[F, FMT],
    QTP: Lazy[Planner[F, FMT, QScriptTotal[T, ?], T[EJson]]]
  ): Planner[F, FMT, ThetaJoin[T, ?], T[EJson]] = {
    implicit val qtp: Planner[F, FMT, QScriptTotal[T, ?], T[EJson]] = QTP.value
    new ThetaJoinPlanner[F, FMT, T]
  }

  // The rest are "Unreachable" due to the lack of mutual-recursion necessitating
  // QScriptTotal[T, ?]. It is a bug if any of these are ever needed at runtime.

  implicit def constDeadEnd[F[_]: MonadPlanErr, FMT, J]: Planner[F, FMT, Const[DeadEnd, ?], J] =
    new UnreachablePlanner[F, FMT, Const[DeadEnd, ?], J]("DeadEnd")

  implicit def constReadDir[F[_]: MonadPlanErr, FMT, J]: Planner[F, FMT, Const[Read[ADir], ?], J] =
    new UnreachablePlanner[F, FMT, Const[Read[ADir], ?], J]("Read[ADir]")

  implicit def constShiftedReadFile[F[_]: MonadPlanErr, FMT, J]: Planner[F, FMT, Const[ShiftedRead[AFile], ?], J] =
    new UnreachablePlanner[F, FMT, Const[ShiftedRead[AFile], ?], J]("ShiftedRead[AFile]")

  implicit def equiJoin[F[_]: MonadPlanErr, FMT, T[_[_]], J]: Planner[F, FMT, EquiJoin[T, ?], J] =
    new UnreachablePlanner[F, FMT, EquiJoin[T, ?], J]("EquiJoin")

  implicit def projectBucket[F[_]: MonadPlanErr, FMT, T[_[_]], J]: Planner[F, FMT, ProjectBucket[T, ?], J] =
    new UnreachablePlanner[F, FMT, ProjectBucket[T, ?], J]("ProjectBucket")
}

sealed abstract class PlannerInstances1 {
  implicit def eitherTPlanner[M[_]: Functor, FMT, F[_], E, J](implicit P: Planner[M, FMT, F, J]): Planner[EitherT[M, E, ?], FMT, F, J] =
    new Planner[EitherT[M, E, ?], FMT, F, J] {
      def plan[Q](implicit Q: Birecursive.Aux[Q, Query[J, ?]]) =
        fx => EitherT(P.plan.apply(fx) map (_.right[E]))
    }

  implicit def writerTPlanner[M[_]: Functor, FMT, F[_], W: Monoid, J](implicit P: Planner[M, FMT, F, J]): Planner[WriterT[M, W, ?], FMT, F, J] =
    new Planner[WriterT[M, W, ?], FMT, F, J] {
      def plan[Q](implicit Q: Birecursive.Aux[Q, Query[J, ?]]) =
        fx => WriterT(P.plan.apply(fx) strengthL mzero[W])
    }
}
