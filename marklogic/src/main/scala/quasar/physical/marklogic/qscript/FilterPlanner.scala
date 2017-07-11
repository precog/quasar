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
import quasar.ejson.EJson
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._
import quasar.qscript.{MapFuncsCore => MFC}

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns.CoEnv
import scalaz._, Scalaz._
import xml.name._

private[qscript] final class FilterPlanner[
  F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr,
  FMT: SearchOptions,
  T[_[_]]: BirecursiveT
](implicit SP: StructuralPlanner[F, FMT]) {

  def plan[Q](src: Search[Q] \/ XQuery, f: FreeMap[T])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Search[Q] \/ XQuery] = src match {
    case \/-(src) => xqueryFilter(src, f) map (_.right)
    case -\/(src) => planPredicate[T, Q].lift(f).fold(planFilter(src, f))(planFilterSearch(src, f))
  }

  private def planFilter[Q](src: Search[Q], f: FreeMap[T])(
    implicit Q: Recursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Search[Q] \/ XQuery] =
    (interpretSearch[Q](src) >>= (xqueryFilter(_: XQuery, f))) map (_.right)

  private def planFilterSearch[Q](src: Search[Q], f: FreeMap[T])(
    implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
  ): Q => F[Search[Q] \/ XQuery] = ((q: Q) =>
    Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src).left.point[F])

  private def xqueryFilter(src: XQuery, fm: FreeMap[T]): F[XQuery] =
    for {
      x   <- freshName[F]
      p   <- mapFuncXQuery[T, F, FMT](fm, ~x) map (xs.boolean)
    } yield src match {
      case IterativeFlwor(bindings, filter, order, isStable, result) =>
        XQuery.Flwor(
          bindings :::> IList(BindingClause.let_(x := result)),
          Some(filter.fold(p)(_ and p)),
          order,
          isStable,
          ~x)

      case _ =>
        for_(x in src) where_ p return_ ~x
    }

  private def interpretSearch[Q](s: Search[Q])(implicit Q: Recursive.Aux[Q, Query[T[EJson], ?]]): F[XQuery] =
    Search.plan[F, Q, T[EJson], FMT](s, EJsonPlanner.plan[T[EJson], F, FMT])

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def planPredicate[T[_[_]]: RecursiveT, Q](
    implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
  ): PartialFunction[FreeMap[T], Q] = {
    case Embed(CoEnv(\/-(MFC.Eq(Embed(CoEnv(\/-(MFC.ProjectField(Embed(CoEnv(\/-(_))), MFC.StrLit(key))))), Embed(CoEnv(\/-(MFC.Constant(v)))))))) =>
      Query.ElementRange[T[EJson], Q](IList(QName.unprefixed(NCName.fromString(key).toOption.get)), ComparisonOp.EQ, IList(v)).embed
  }
}
