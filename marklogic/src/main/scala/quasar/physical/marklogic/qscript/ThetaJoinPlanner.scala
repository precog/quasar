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
import quasar.physical.marklogic.cts.Query
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class ThetaJoinPlanner[
  F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr,
  FMT: SearchOptions,
  T[_[_]]: BirecursiveT
](implicit
  QTP: Planner[F, FMT, QScriptTotal[T, ?]],
  SP : StructuralPlanner[F, FMT]
) extends Planner[F, FMT, ThetaJoin[T, ?]] {
  import expr.for_

  // FIXME: Handle `JoinType`
  // TODO:  Is it more performant to inline src into each branch than it is to
  //        assign it to a variable? It may be necessary in order to be able to
  //        use the cts:query features, but we'll need to profile otherwise.
  def plan[Q, V](implicit Q: Birecursive.Aux[Q, Query[V, ?]]): AlgebraM[F, ThetaJoin[T, ?], Search[Q] \/ XQuery] = {
    case ThetaJoin(src, lBranch, rBranch, on, _, combine) =>
      for {
        l      <- freshName[F]
        r      <- freshName[F]
        lhs0   <- rebaseXQuery[T, F, FMT, Q, V](lBranch, src)
        rhs0   <- rebaseXQuery[T, F, FMT, Q, V](rBranch, src)
        lhs    <- elimSearch[Q, V](lhs0)
        rhs    <- elimSearch[Q, V](rhs0)
        filter <- mergeXQuery[T, F, FMT](on, ~l, ~r)
        body   <- mergeXQuery[T, F, FMT](combine, ~l, ~r)
      } yield ((lhs, rhs) match {
        case (IterativeFlwor(lcs, None, INil(), _, lr), IterativeFlwor(rcs, None, INil(), _, rr)) =>
          thetaJoinFlwor(
            lcs                                        |+|
            NonEmptyList(BindingClause.let_(l := lr))  |+|
            rcs                                        |+|
            NonEmptyList(BindingClause.let_(r := rr)),
            filter,
            body)

        case (IterativeFlwor(lcs, None, INil(), _, lr), _                                        ) =>
          thetaJoinFlwor(
            lcs                                        |+|
            NonEmptyList(
              BindingClause.let_(l := lr),
              BindingClause.for_(r in rhs)),
            filter,
            body)

        case (_                                       , IterativeFlwor(rcs, None, INil(), _, rr)) =>
          thetaJoinFlwor(
            NonEmptyList(BindingClause.for_(l := lhs)) |+|
            rcs                                        |+|
            NonEmptyList(BindingClause.let_(r := rr)),
            filter,
            body)

        case _ => for_ (l in lhs, r in rhs) where_ filter return_ body
      }).right
  }

  def thetaJoinFlwor(bindings: NonEmptyList[BindingClause], filter: XQuery, body: XQuery): XQuery =
    XQuery.Flwor(bindings, Some(filter), IList.empty, false, body)
}
