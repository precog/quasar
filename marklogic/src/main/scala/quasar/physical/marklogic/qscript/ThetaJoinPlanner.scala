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

import quasar.Predef._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class ThetaJoinPlanner[F[_]: Monad: QNameGenerator, FMT, T[_[_]]: RecursiveT](
  implicit
  QTP: Planner[F, FMT, QScriptTotal[T, ?]],
  MFP: Planner[F, FMT, MapFunc[T, ?]]
) extends Planner[F, FMT, ThetaJoin[T, ?]] {
  import expr.let_

  // FIXME: Handle `JoinType`
  // TODO:  Is it more performant to inline src into each branch than it is to
  //        assign it to a variable? It may be necessary in order to be able to
  //        use the cts:query features, but we'll need to profile otherwise.
  val plan: AlgebraM[F, ThetaJoin[T, ?], XQuery] = {
    case ThetaJoin(src, lBranch, rBranch, on, _, combine) =>
      for {
        l      <- freshName[F]
        r      <- freshName[F]
        s      <- freshName[F]
        lhs    <- rebaseXQuery[T, F, FMT](lBranch, ~s)
        rhs    <- rebaseXQuery[T, F, FMT](rBranch, ~s)
        filter <- mergeXQuery[T, F, FMT](on, ~l, ~r)
        body   <- mergeXQuery[T, F, FMT](combine, ~l, ~r)
      } yield (lhs, rhs) match {
        case (IterativeFlwor(lcs, None, INil(), _, lr), IterativeFlwor(rcs, None, INil(), _, rr)) =>
          thetaJoinFlwor(
            NonEmptyList(BindingClause.let_(s := src)) |+|
            lcs                                        |+|
            NonEmptyList(BindingClause.let_(l := lr))  |+|
            rcs                                        |+|
            NonEmptyList(BindingClause.let_(r := rr)),
            filter,
            body)

        case (IterativeFlwor(lcs, None, INil(), _, lr), _                                        ) =>
          thetaJoinFlwor(
            NonEmptyList(BindingClause.let_(s := src)) |+|
            lcs                                        |+|
            NonEmptyList(
              BindingClause.let_(l := lr),
              BindingClause.for_(r in rhs)),
            filter,
            body)

        case (_                                       , IterativeFlwor(rcs, None, INil(), _, rr)) =>
          thetaJoinFlwor(
            NonEmptyList(
              BindingClause.let_(s := src),
              BindingClause.for_(l := lhs))            |+|
            rcs                                        |+|
            NonEmptyList(BindingClause.let_(r := rr)),
            filter,
            body)

        case _ => let_ (s := src) for_ (l in lhs, r in rhs) where_ filter return_ body
      }
  }

  def thetaJoinFlwor(bindings: NonEmptyList[BindingClause], filter: XQuery, body: XQuery): XQuery =
    XQuery.Flwor(bindings, Some(filter), IList.empty, false, body)
}
