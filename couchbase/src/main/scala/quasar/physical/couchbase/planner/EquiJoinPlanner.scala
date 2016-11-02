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
import quasar.NameGenerator
import quasar.common.PhaseResult.detail
import quasar.contrib.matryoshka._
import quasar.fp._, eitherT._
import quasar.fp.ski.κ
import quasar.physical.couchbase._, N1QL._
import quasar.physical.couchbase.planner.Planner._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

// Join document by field that is not a primary key?
// ╰─ http://stackoverflow.com/a/39264955/343274
// ╰─ https://forums.couchbase.com/t/n1ql-joins-on-document-fields/8418/3

// When falling back to Map/Reduce can quickly arrive at "error (reduction too large)"
// ╰─ https://github.com/couchbase/couchstore/search?utf8=%E2%9C%93&q=MAX_REDUCTION_SIZE

final class EquiJoinPlanner[F[_]: Monad: NameGenerator, T[_[_]]: Recursive: Corecursive: ShowT]
  extends Planner[F, EquiJoin[T, ?]] {

  def plan: AlgebraM[M, EquiJoin[T, ?], N1QL] = {
    case EquiJoin(src, lBranch, rBranch, lKey, rKey, f, combine) =>
    (for {
      tmpName <- genName[M]
      sN1ql   =  n1ql(src)
      lbN1ql  <- freeCataM(lBranch)(interpretM(
                   κ(partialQueryString(tmpName).point[M]),
                   Planner[F, QScriptTotal[T, ?]].plan))
      rbN1ql  <- freeCataM(rBranch)(interpretM(
                   κ(partialQueryString(tmpName).point[M]),
                   Planner[F, QScriptTotal[T, ?]].plan))
      lkN1ql  <- freeCataM(lKey)(interpretM(
                   i => partialQueryString(i.shows).point[M],
                   mapFuncPlanner[F, T].plan))
      rkN1ql  <- freeCataM(rKey)(interpretM(
                   i => partialQueryString(i.shows).point[M],
                   mapFuncPlanner[F, T].plan))
      cN1ql   <- freeCataM(combine)(interpretM(
                   i => partialQueryString(i.shows).point[M],
                   mapFuncPlanner[F, T].plan))
      _       <- prtell[M](Vector(detail(
                   "N1QL EquiJoin",
                   s"""  src:     $sN1ql
                      |  lBranch: $lbN1ql
                      |  rBranch: $rbN1ql
                      |  lKey:    $lkN1ql
                      |  rKey:    $rkN1ql
                      |  f:       $f
                      |  combine: $cN1ql
                      |  n1ql:    ???""".stripMargin('|'))))
    } yield ()) *>
    unimplementedP("EquiJoin")

  }
}
