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

package quasar.physical.couchbase.planner

import slamdata.Predef._
import quasar.physical.couchbase._
import quasar.physical.couchbase.N1QL._
import quasar.qscript, qscript.{ReduceFunc, ReduceFuncs => RF}

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

final class ReduceFuncPlanner[T[_[_]]: CorecursiveT, F[_]: Applicative] extends Planner[T, F, ReduceFunc] {

  def plan: AlgebraM[F, ReduceFunc, T[N1QL]] = planʹ >>> (_.embed.η[F])

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val planʹ: Transform[T[N1QL], ReduceFunc, N1QL] = {
    case RF.Arbitrary(a1)      => Min(a1)
    case RF.Avg(a1)            => Avg(a1)
    case RF.Count(a1)          => Count(a1)
    case RF.First(a1)          => ???
    case RF.Last(a1)           => ???
    case RF.Max(a1)            => Max(a1)
    case RF.Min(a1)            => Min(a1)
    case RF.Sum(a1)            => Sum(a1)
    case RF.UnshiftArray(a1)   => ArrAgg(a1)
    case RF.UnshiftMap(a1, a2) => Obj(List(a1 -> a2))
  }

}
