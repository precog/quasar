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

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.physical.marklogic.cts.Query
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr.emptySeq
import quasar.qscript._
import quasar.fp.ski.κ

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._
import shapeless.Lazy

trait Planner[M[_], FMT, F[_]] {
  def plan[Q, V](implicit Q: Birecursive.Aux[Q, Query[V, ?]]): AlgebraM[M, F, Search[Q] \/ XQuery]

  def planXQuery(
    implicit
    M0: Monad[M],
    M1: PrologW[M],
    O : SearchOptions[FMT],
    SP: StructuralPlanner[M, FMT],
    F : Functor[F]
  ): AlgebraM[M, F, XQuery] = {
    type Q = Fix[Query[Unit, ?]]
    Kleisli(elimSearch[Q, Unit] _) <==< (plan[Q, Unit] <<< F.lift((_: XQuery).right[Search[Q]]))
  }

  protected def elimSearch[Q, V](x: Search[Q] \/ XQuery)(
    implicit
    Q: Recursive.Aux[Q, Query[V, ?]],
    M0: Monad[M],
    M1: PrologW[M],
    O : SearchOptions[FMT],
    SP: StructuralPlanner[M, FMT]
  ): M[XQuery] =
    x.fold(Search.plan[M, Q, V, FMT](_, κ(emptySeq.point[M])), _.point[M])
}

object Planner extends PlannerInstances {
  def apply[M[_], FMT, F[_]](implicit ev: Planner[M, FMT, F]): Planner[M, FMT, F] = ev
}

sealed abstract class PlannerInstances extends PlannerInstances0 {
  implicit def coproduct[M[_], FMT, F[_], G[_]](
    implicit F: Lazy[Planner[M, FMT, F]], G: Lazy[Planner[M, FMT, G]]
  ): Planner[M, FMT, Coproduct[F, G, ?]] =
    new Planner[M, FMT, Coproduct[F, G, ?]] {
      def plan[Q, V](implicit Q: Birecursive.Aux[Q, Query[V, ?]]): AlgebraM[M, Coproduct[F, G, ?], Search[Q] \/ XQuery] =
        _.run.fold(F.value.plan, G.value.plan)
    }
}

sealed abstract class PlannerInstances0 extends PlannerInstances1 {

  implicit def constReadFile[F[_]: Applicative: MonadPlanErr, FMT](
    implicit SP: StructuralPlanner[F, FMT]
  ): Planner[F, FMT, Const[Read[AFile], ?]] =
    new ReadFilePlanner[F, FMT]

  implicit def constShiftedReadDir[F[_]: Applicative: MonadPlanErr, FMT](
    implicit SP: StructuralPlanner[F, FMT]
  ): Planner[F, FMT, Const[ShiftedRead[ADir], ?]] =
    new ShiftedReadDirPlanner[F, FMT]

  implicit def qScriptCore[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT: SearchOptions, T[_[_]]: BirecursiveT](
    implicit
    SP : StructuralPlanner[F, FMT],
    QTP: Lazy[Planner[F, FMT, QScriptTotal[T, ?]]]
  ): Planner[F, FMT, QScriptCore[T, ?]] = {
    implicit val qtp: Planner[F, FMT, QScriptTotal[T, ?]] = QTP.value
    new QScriptCorePlanner[F, FMT, T]
  }

  implicit def thetaJoin[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT: SearchOptions, T[_[_]]: BirecursiveT](
    implicit
    SP : StructuralPlanner[F, FMT],
    QTP: Lazy[Planner[F, FMT, QScriptTotal[T, ?]]]
  ): Planner[F, FMT, ThetaJoin[T, ?]] = {
    implicit val qtp: Planner[F, FMT, QScriptTotal[T, ?]] = QTP.value
    new ThetaJoinPlanner[F, FMT, T]
  }

  // The rest are "Unreachable" due to the lack of mutual-recursion necessitating
  // QScriptTotal[T, ?]. It is a bug if any of these are ever needed at runtime.

  implicit def constDeadEnd[F[_]: MonadPlanErr, FMT]: Planner[F, FMT, Const[DeadEnd, ?]] =
    new UnreachablePlanner[F, FMT, Const[DeadEnd, ?]]("DeadEnd")

  implicit def constReadDir[F[_]: MonadPlanErr, FMT]: Planner[F, FMT, Const[Read[ADir], ?]] =
    new UnreachablePlanner[F, FMT, Const[Read[ADir], ?]]("Read[ADir]")

  implicit def constShiftedReadFile[F[_]: MonadPlanErr, FMT]: Planner[F, FMT, Const[ShiftedRead[AFile], ?]] =
    new UnreachablePlanner[F, FMT, Const[ShiftedRead[AFile], ?]]("ShiftedRead[AFile]")

  implicit def equiJoin[F[_]: MonadPlanErr, FMT, T[_[_]]]: Planner[F, FMT, EquiJoin[T, ?]] =
    new UnreachablePlanner[F, FMT, EquiJoin[T, ?]]("EquiJoin")

  implicit def projectBucket[F[_]: MonadPlanErr, FMT, T[_[_]]]: Planner[F, FMT, ProjectBucket[T, ?]] =
    new UnreachablePlanner[F, FMT, ProjectBucket[T, ?]]("ProjectBucket")
}

sealed abstract class PlannerInstances1 {
  implicit def eitherTPlanner[M[_]: Functor, FMT, F[_], E](implicit P: Planner[M, FMT, F]): Planner[EitherT[M, E, ?], FMT, F] =
    new Planner[EitherT[M, E, ?], FMT, F] {
      def plan[Q, V](implicit Q: Birecursive.Aux[Q, Query[V, ?]]) =
        fx => EitherT(P.plan.apply(fx) map (_.right[E]))
    }

  implicit def writerTPlanner[M[_]: Functor, FMT, F[_], W: Monoid](implicit P: Planner[M, FMT, F]): Planner[WriterT[M, W, ?], FMT, F] =
    new Planner[WriterT[M, W, ?], FMT, F] {
      def plan[Q, V](implicit Q: Birecursive.Aux[Q, Query[V, ?]]) =
        fx => WriterT(P.plan.apply(fx) strengthL mzero[W])
    }
}
