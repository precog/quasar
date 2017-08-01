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
import scalaz.concurrent.Task

trait Planner[F[_]] extends Serializable {
  def plan(fromFile: (SparkContext, AFile) => Task[RDD[Data]]): AlgebraM[Planner.SparkState, F, RDD[Data]]
}

object Planner {

  def apply[F[_]](implicit P: Planner[F]): Planner[F] = P

  type SparkState[A] = StateT[EitherT[Task, PlannerError, ?], SparkContext, A]
  type SparkStateT[F[_], A] = StateT[F, SparkContext, A]


  implicit def deadEnd: Planner[Const[DeadEnd, ?]] = unreachable("deadEnd")
  implicit def read[A]: Planner[Const[Read[A], ?]] = unreachable("read")
  implicit def shiftedReadPath: Planner[Const[ShiftedRead[ADir], ?]] = unreachable("shifted read of a dir")
  implicit def projectBucket[T[_[_]]]: Planner[ProjectBucket[T, ?]] = unreachable("projectBucket")
  implicit def thetaJoin[T[_[_]]]: Planner[ThetaJoin[T, ?]] = unreachable("thetajoin")
  implicit def shiftedread: Planner[Const[ShiftedRead[AFile], ?]] = ShiftedReadPlanner
  implicit def qscriptCore[T[_[_]]: BirecursiveT: ShowT]: Planner[QScriptCore[T, ?]] = new QScriptCorePlanner
  implicit def equiJoin[T[_[_]]: BirecursiveT: ShowT]: Planner[EquiJoin[T, ?]] = new EquiJoinPlanner

  implicit def coproduct[F[_], G[_]](
    implicit F: Planner[F], G: Planner[G]):
      Planner[Coproduct[F, G, ?]] =
    new Planner[Coproduct[F, G, ?]] {
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[Data]]): AlgebraM[SparkState, Coproduct[F, G, ?], RDD[Data]] = _.run.fold(F.plan(fromFile), G.plan(fromFile))
    }

  private def unreachable[F[_]](what: String): Planner[F] =
    new Planner[F] {
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[Data]]): AlgebraM[SparkState, F, RDD[Data]] =
        _ =>  StateT((sc: SparkContext) => {
          EitherT(InternalError.fromMsg(s"unreachable $what").left[(SparkContext, RDD[Data])].point[Task])
        })
    }

}
