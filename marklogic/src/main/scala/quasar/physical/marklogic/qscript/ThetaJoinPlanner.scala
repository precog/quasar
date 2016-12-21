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

import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class ThetaJoinPlanner[F[_]: QNameGenerator: PrologW: MonadPlanErr, T[_[_]]: BirecursiveT]
  extends MarkLogicPlanner[F, ThetaJoin[T, ?]] {
  import expr.let_

  // FIXME: Handle `JoinType`
  // TODO:  When `src` is unreferenced, should be able to elide the outer `let`,
  //        may also be able to fuse into a single FLWOR depending on branches.
  val plan: AlgebraM[F, ThetaJoin[T, ?], XQuery] = {
    case ThetaJoin(src, lBranch, rBranch, on, f, combine) =>
      for {
        l      <- freshName[F]
        r      <- freshName[F]
        s      <- freshName[F]
        lhs    <- rebaseXQuery(lBranch, ~s)
        rhs    <- rebaseXQuery(rBranch, ~s)
        filter <- mergeXQuery(on, ~l, ~r)
        body   <- mergeXQuery(combine, ~l, ~r)
      } yield let_ (s := src) for_ (l in lhs, r in rhs) where_ filter return_ body
  }
}
