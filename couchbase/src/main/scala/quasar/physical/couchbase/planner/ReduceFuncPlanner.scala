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

package quasar.physical.couchbase.planner

import quasar.Predef._
import quasar.fp.eitherT._
import quasar.common.PhaseResult.detail
import quasar.physical.couchbase._
import quasar.physical.couchbase.N1QL._
import quasar.qscript, qscript._

import matryoshka._
import scalaz._, Scalaz._

final class ReduceFuncPlanner[F[_]: Monad] extends Planner[F, ReduceFunc] {
  import ReduceFuncs._

  def plan: AlgebraM[M, ReduceFunc, N1QL] = {
    case Arbitrary(a)     =>
      val aN1ql   = n1ql(a)
      val n1qlStr = s"min($aN1ql)"
      prtell[M](Vector(detail(
        "N1QL Arbitrary",
        s"""  a:    $aN1ql
           |  n1ql: $n1qlStr""".stripMargin('|')
      ))).as(
        partialQueryString(n1qlStr)
      )
    case Avg(a)           =>
      partialQueryString(s"avg(${n1ql(a)})").point[M]
    case Count(a)         =>
      partialQueryString(s"count(${n1ql(a)})").point[M]
    case Max(a)           =>
      partialQueryString(s"max(${n1ql(a)})").point[M]
    case Min(a)           =>
      partialQueryString(s"min(${n1ql(a)})").point[M]
    case Sum(a)           =>
      partialQueryString(s"sum(${n1ql(a)})").point[M]
    case UnshiftArray(a)  =>
      partialQueryString(s"array_agg(${n1ql(a)})").point[M]
    case UnshiftMap(k, v) =>
      val kN1ql   = n1ql(k)
      val vN1ql   = n1ql(v)
      val n1qlStr = s"object_add({}, $kN1ql, $vN1ql)"
      prtell[M](Vector(detail(
        "N1QL UnshiftMap",
        s"""  k:    $kN1ql
           |  v:    $vN1ql
           |  n1ql: $n1qlStr""".stripMargin('|')
      ))).as(
        partialQueryString(n1qlStr)
      )
  }
}
