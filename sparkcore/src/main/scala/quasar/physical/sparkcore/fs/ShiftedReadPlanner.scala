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
import quasar.contrib.pathy.AFile
import quasar.qscript._

import org.apache.spark._
import org.apache.spark.rdd._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object ShiftedReadPlanner extends Planner[Const[ShiftedRead[AFile], ?]] {

  def plan(fromFile: (SparkContext, AFile) => Task[RDD[Data]]) =
    (qs: Const[ShiftedRead[AFile], RDD[Data]]) => {
      StateT((sc: SparkContext) => {
        val filePath = qs.getConst.path
        val idStatus = qs.getConst.idStatus

        EitherT(fromFile(sc, filePath).map { rdd =>
          (sc,
            idStatus match {
              case IdOnly => rdd.zipWithIndex.map[Data](p => Data.Int(p._2))
              case IncludeId =>
                rdd.zipWithIndex.map[Data](p =>
                  Data.Arr(List(Data.Int(p._2), p._1)))
              case ExcludeId => rdd
            }).right[PlannerError]
        })
      })
    }
}
