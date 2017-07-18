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

import quasar._
import quasar.common.JoinType
import quasar.contrib.pathy.AFile
import quasar.fp._, ski._
import quasar.qscript._

import scala.math.{Ordering => SOrdering}, SOrdering.Implicits._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import scalaz.concurrent.Task


class EquiJoinPlanner[T[_[_]]: BirecursiveT: ShowT]  extends Planner[EquiJoin[T, ?]] {

  import Planner.{SparkState, SparkStateT}

  def plan(fromFile: (SparkContext, AFile) => Task[RDD[Data]]): AlgebraM[SparkState, EquiJoin[T, ?], RDD[Data]] = {
    case EquiJoin(src, lBranch, rBranch, lKey, rKey, jt, combine) =>
      val algebraM = Planner[QScriptTotal[T, ?]].plan(fromFile)
      val srcState = src.point[SparkState]

      def genKey(kf: FreeMap[T]): SparkState[Data => Data] =
        EitherT(CoreMap.changeFreeMap(kf).point[Task]).liftM[SparkStateT]

      val merger: SparkState[(Data, Data) => Data] =
        EitherT(CoreMap.changeJoinFunc(combine).point[Task]).liftM[SparkStateT]

      for {
        lk <- genKey(lKey)
        rk <- genKey(rKey)
        lRdd <- lBranch.cataM(interpretM(κ(srcState), algebraM))
        rRdd <- rBranch.cataM(interpretM(κ(srcState), algebraM))
        merge <- merger
      } yield {
        val klRdd = lRdd.map(d => (lk(d), d))
        val krRdd = rRdd.map(d => (rk(d), d))

        jt match {
          case JoinType.Inner => klRdd.join(krRdd).map {
            case (_, (l, r)) => merge(l, r)
          }
          case JoinType.LeftOuter => klRdd.leftOuterJoin(krRdd).map {
            case (_, (l, r)) => merge(l, r.getOrElse(Data.NA))
          }
          case JoinType.RightOuter => klRdd.rightOuterJoin(krRdd).map {
            case (_, (l, r)) => merge(l.getOrElse(Data.NA), r)
          }
          case JoinType.FullOuter => klRdd.fullOuterJoin(krRdd).map {
            case (_, (l, r)) => merge(l.getOrElse(Data.NA), r.getOrElse(Data.NA))
          }
        }
      }
  }
}
