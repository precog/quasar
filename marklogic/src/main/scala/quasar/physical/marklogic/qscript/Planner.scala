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

package quasar.physical.marklogic.qscript

import quasar.Data
import quasar.physical.marklogic.xquery._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._
import shapeless.Lazy

trait Planner[M[_], FMT, F[_]] {
  def plan: AlgebraM[M, F, XQuery]
}

object Planner extends PlannerInstances {
  def apply[M[_], FMT, F[_]](implicit ev: Planner[M, FMT, F]): Planner[M, FMT, F] = ev
}

sealed abstract class PlannerInstances extends PlannerInstances0 {
  implicit def coproduct[M[_], FMT, F[_], G[_]](
    implicit F: Lazy[Planner[M, FMT, F]], G: Lazy[Planner[M, FMT, G]]
  ): Planner[M, FMT, Coproduct[F, G, ?]] =
    new Planner[M, FMT, Coproduct[F, G, ?]] {
      val plan: AlgebraM[M, Coproduct[F, G, ?], XQuery] =
        _.run.fold(F.value.plan, G.value.plan)
    }

  implicit def constDataPlanner[M[_]: Monad, FMT](
    implicit SP: StructuralPlanner[M, FMT]
  ): Planner[M, FMT, Const[Data, ?]] =
    new DataPlanner[M, FMT]
}

sealed abstract class PlannerInstances0 extends PlannerInstances1 {
  implicit def mapFunc[M[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT, T[_[_]]: RecursiveT](
    implicit
    DP: Planner[M, FMT, Const[Data, ?]],
    SP: StructuralPlanner[M, FMT]
  ): Planner[M, FMT, MapFunc[T, ?]] =
    new MapFuncPlanner[M, FMT, T]
}

sealed abstract class PlannerInstances1 extends PlannerInstances2 {
  implicit def qScriptCore[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT, T[_[_]]: BirecursiveT](
    implicit
    SP : StructuralPlanner[F, FMT],
    QTP: Lazy[Planner[F, FMT, QScriptTotal[T, ?]]],
    MFP: Planner[F, FMT, MapFunc[T, ?]]
  ): Planner[F, FMT, QScriptCore[T, ?]] = {
    implicit val qtp = QTP.value
    new QScriptCorePlanner[F, FMT, T]
  }

  implicit def constShiftedRead[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT: SearchOptions](
    implicit SP: StructuralPlanner[F, FMT]
  ): Planner[F, FMT, Const[ShiftedRead, ?]] =
    new ShiftedReadPlanner[F, FMT]

  implicit def thetaJoin[F[_]: Monad: QNameGenerator, FMT, T[_[_]]: RecursiveT](
    implicit
    QTP: Lazy[Planner[F, FMT, QScriptTotal[T, ?]]],
    MFP: Planner[F, FMT, MapFunc[T, ?]]
  ): Planner[F, FMT, ThetaJoin[T, ?]] = {
    implicit val qtp = QTP.value
    new ThetaJoinPlanner[F, FMT, T]
  }

  // The rest are "Unreachable" due to the lack of mutual-recursion necessitating
  // QScriptTotal[T, ?]. It is a bug if any of these are ever needed at runtime.

  implicit def constDeadEnd[F[_]: MonadPlanErr, FMT]: Planner[F, FMT, Const[DeadEnd, ?]] =
    new UnreachablePlanner[F, FMT, Const[DeadEnd, ?]]("DeadEnd")

  implicit def constRead[F[_]: MonadPlanErr, FMT]: Planner[F, FMT, Const[Read, ?]] =
    new UnreachablePlanner[F, FMT, Const[Read, ?]]("Read")

  implicit def projectBucket[F[_]: MonadPlanErr, FMT, T[_[_]]]: Planner[F, FMT, ProjectBucket[T, ?]] =
    new UnreachablePlanner[F, FMT, ProjectBucket[T, ?]]("ProjectBucket")

  implicit def equiJoin[F[_]: MonadPlanErr, FMT, T[_[_]]]: Planner[F, FMT, EquiJoin[T, ?]] =
    new UnreachablePlanner[F, FMT, EquiJoin[T, ?]]("EquiJoin")
}

sealed abstract class PlannerInstances2 {
  implicit def eitherTPlanner[M[_]: Functor, FMT, F[_], E](implicit P: Planner[M, FMT, F]): Planner[EitherT[M, E, ?], FMT, F] =
    new Planner[EitherT[M, E, ?], FMT, F] {
      val plan: AlgebraM[EitherT[M, E, ?], F, XQuery] =
        fx => EitherT(P.plan(fx) map (_.right[E]))
    }

  implicit def writerTPlanner[M[_]: Functor, FMT, F[_], W: Monoid](implicit P: Planner[M, FMT, F]): Planner[WriterT[M, W, ?], FMT, F] =
    new Planner[WriterT[M, W, ?], FMT, F] {
      val plan: AlgebraM[WriterT[M, W, ?], F, XQuery] =
        fx => WriterT(P.plan(fx) strengthL mzero[W])
    }
}
