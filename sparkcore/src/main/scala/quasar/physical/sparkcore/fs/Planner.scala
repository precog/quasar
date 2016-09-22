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

package quasar.physical.sparkcore.fs

import simulacrum.typeclass
import quasar.Predef._
import quasar.Data
import quasar.DataCodec
import quasar._
import quasar.Planner._
import quasar.fp._
import quasar.qscript._
import quasar.contrib.pathy.AFile

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import scalaz._, Scalaz._
import scalaz.concurrent.Task
// 

@typeclass trait Planner[F[_]] {
  type IT[G[_]]

  def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[StateT[EitherT[Task, PlannerError, ?], SparkContext, ?], F, RDD[Data]]

}

object Planner {
  
  type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }
  
  // NB: Shouldn’t need this once we convert to paths.
  implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] =
    new Planner[Const[DeadEnd, ?]] {
      type IT[G[_]] = T[G]
      
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[StateT[EitherT[Task, PlannerError, ?], SparkContext, ?], Const[DeadEnd, ?], RDD[Data]] =
        (qs: Const[DeadEnd, RDD[Data]]) =>
      StateT((sc: SparkContext) => {
        EitherT((sc, sc.parallelize(List(Data.Null: Data))).right[PlannerError].point[Task])
      })
    }

  implicit def read[T[_[_]]]: Planner.Aux[T, Const[Read, ?]] =
    new Planner[Const[Read, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]) =
        (qs: Const[Read, RDD[Data]]) => {
          StateT((sc: SparkContext) => {
            val filePath = qs.getConst.path
            EitherT(fromFile(sc, filePath).map { initRDD =>
              val rdd = initRDD.map { raw =>
                DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι)
              }
                (sc, rdd).right[PlannerError]

            })
          })
        }
    }

  implicit def shiftedread[T[_[_]]]: Planner.Aux[T, Const[ShiftedRead, ?]] =
    new Planner[Const[ShiftedRead, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]) = ???
    }

  implicit def qscriptCore[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, QScriptCore[T, ?]] =
    new Planner[QScriptCore[T, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[StateT[EitherT[Task, PlannerError, ?], SparkContext, ?], QScriptCore[T, ?], RDD[Data]] = {
        case qscript.Map(src, f) =>
          StateT((sc: SparkContext) =>
            EitherT {
              val maybeFunc =
                freeCataM(f)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))
              maybeFunc.map(df => (sc, src.map(df))).point[Task]
            }
          )
        case Reduce(src, bucket, reducers, repair) =>
          ??? // select [sum(pop), max(pop)] from cities group by state
        case Sort(src, bucket, order) =>
          ???
        case Filter(src, f) =>
          StateT((sc: SparkContext) =>
            EitherT {
              val maybeFunc =
                freeCataM(f)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))
              maybeFunc.map(df => (sc, src.filter{
                df >>> {
                  case Data.Bool(b) => b
                  case _ => false
                }
              })).point[Task]
            }
          )
        case Take(src, from, count) =>
          ???
        case Drop(src, from, count) =>
          ???
        case LeftShift(src, struct, repair) => StateT((sc: SparkContext) => {
          EitherT((sc, src).right[PlannerError].point[Task])
        })
        case Union(src, lBranch, rBranch) => ???
        case Unreferenced() =>
          StateT((sc: SparkContext) => {
            EitherT((sc, sc.parallelize(List(Data.Null: Data))).right[PlannerError].point[Task])
          })
      }
    }
  
  implicit def equiJoin[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, EquiJoin[T, ?]] =
    new Planner[EquiJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[StateT[EitherT[Task, PlannerError, ?], SparkContext, ?], EquiJoin[T, ?], RDD[Data]] = _ => ???      
    }
  
  // TODO: Remove this instance
  implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] =
    new Planner[ThetaJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[StateT[EitherT[Task, PlannerError, ?], SparkContext, ?], ThetaJoin[T, ?], RDD[Data]] = _ => ???
    }
  
  // TODO: Remove this instance
  implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] =
    new Planner[ProjectBucket[T, ?]] {
      type IT[G[_]] = T[G]
            def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[StateT[EitherT[Task, PlannerError, ?], SparkContext, ?], ProjectBucket[T, ?], RDD[Data]] = _ => ???
    }
  
  implicit def coproduct[T[_[_]]: Recursive: ShowT, F[_], G[_]](
    implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
      Planner.Aux[T, Coproduct[F, G, ?]] =
    new Planner[Coproduct[F, G, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[StateT[EitherT[Task, PlannerError, ?], SparkContext, ?], Coproduct[F, G, ?], RDD[Data]] = _.run.fold(F.plan(fromFile), G.plan(fromFile))
    }
}
