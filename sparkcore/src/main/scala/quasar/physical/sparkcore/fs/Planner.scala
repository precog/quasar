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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar._, quasar.Planner._
import quasar.contrib.pathy.{AFile, ADir}
import quasar.qscript._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import scalaz._, Scalaz._


trait Planner[F[_], S[_]] extends Serializable {
  def plan(
    fromFile: AFile => Free[S, RDD[Data]],
    first: RDD[Data] => Free[S, Data]
  ): AlgebraM[Planner.SparkState[S, ?], F, RDD[Data]]
}

object Planner {

  def apply[F[_],S[_]](implicit P: Planner[F, S]): Planner[F, S] = P

  type SparkState[S[_], A] = StateT[EitherT[Free[S, ?], PlannerError, ?], SparkContext, A]
  type SparkStateT[F[_], A] = StateT[F, SparkContext, A]

  implicit def deadEnd[S[_]]: Planner[Const[DeadEnd, ?], S] = unreachable("deadEnd")
  implicit def read[A, S[_]]: Planner[Const[Read[A], ?], S] = unreachable("read")
  implicit def shiftedReadPath[S[_]]: Planner[Const[ShiftedRead[ADir], ?], S] = unreachable("shifted read of a dir")
  implicit def projectBucket[T[_[_]], S[_]]: Planner[ProjectBucket[T, ?], S] = unreachable("projectBucket")
  implicit def thetaJoin[T[_[_]], S[_]]: Planner[ThetaJoin[T, ?], S] = unreachable("thetajoin")
  implicit def shiftedread[S[_]]: Planner[Const[ShiftedRead[AFile], ?], S] = new ShiftedReadPlanner[S]
  implicit def qscriptCore[T[_[_]]: BirecursiveT: ShowT, S[_]]: Planner[QScriptCore[T, ?], S] = new QScriptCorePlanner[T, S]
  implicit def equiJoin[T[_[_]]: BirecursiveT: ShowT, S[_]]: Planner[EquiJoin[T, ?], S] = new EquiJoinPlanner[T, S]

  implicit def coproduct[F[_], G[_], S[_]](
    implicit F: Planner[F, S], G: Planner[G, S]):
      Planner[Coproduct[F, G, ?], S] =
    new Planner[Coproduct[F, G, ?], S] {
      def plan(fromFile: AFile => Free[S, RDD[Data]], first: RDD[Data] => Free[S, Data]): AlgebraM[SparkState[S, ?], Coproduct[F, G, ?], RDD[Data]] = _.run.fold(F.plan(fromFile, first), G.plan(fromFile, first))
    }

  private def unreachable[F[_], S[_]](what: String): Planner[F, S] =
    new Planner[F, S] {
      def plan(fromFile: AFile => Free[S, RDD[Data]], first: RDD[Data] => Free[S,Data]): AlgebraM[SparkState[S, ?], F, RDD[Data]] =
        _ =>  StateT((sc: SparkContext) => {
          EitherT(InternalError.fromMsg(s"unreachable $what").left[(SparkContext, RDD[Data])].point[Free[S, ?]])
        })
    }

}
