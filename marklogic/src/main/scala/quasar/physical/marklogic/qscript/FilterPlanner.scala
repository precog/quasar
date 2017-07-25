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
import quasar.contrib.pathy._
import quasar.ejson.EJson
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.xcc.Xcc
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._
import quasar.qscript.{MapFuncsCore => MFCore, MFC => _, _}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.patterns._
import matryoshka.implicits._
import pathy.Path.depth
import xml.name._

import scalaz._, Scalaz._

private[qscript] final class FilterPlanner[
  F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
  FMT: SearchOptions,
  T[_[_]]: BirecursiveT: ShowT
](implicit SP: StructuralPlanner[F, FMT]) {
  def plan[Q](src0: Search[Q] \/ XQuery, f: FreeMap[T])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Search[Q] \/ XQuery] = {
    lazy val pathQuery: F[Option[Q]] = validQuery(planPathIndex(f))

    (src0.point[F] |@| pathQuery) {
      case (\/-(src), _)       => xqueryFilter(src, f) map (_.right[Search[Q]])
      case (-\/(src), None)    => fallbackFilter(src, f) map (_.right[Search[Q]])
      case (-\/(src), Some(q)) => indexFilter(src, q).left[XQuery].point[F]
    }.join
  }

  private def fallbackFilter[Q](src: Search[Q], f: FreeMap[T])(
    implicit Q: Recursive.Aux[Q, Query[T[EJson], ?]]
  ): F[XQuery] = {
    def interpretSearch(s: Search[Q]): F[XQuery] =
      Search.plan[F, Q, T[EJson], FMT](s, EJsonPlanner.plan[T[EJson], F, FMT])

    interpretSearch(src) >>= (xqueryFilter(_: XQuery, f))
  }

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


  private def indexFilter[Q](src: Search[Q], q: Q)(
    implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
  ): Search[Q] =
    Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src)


  private object PathProjection {
    def unapply[T[_[_]]](fpm: FreePathMap[T]): Option[(ADir, T[EJson])] = fpm match {
      case Embed(CoEnv(\/-(MFPath(MFCore.Eq(Embed(CoEnv(\/-(PathProject(pp)))), Embed(CoEnv(\/-(MFPath(MFCore.Constant(v)))))))))) =>
        (pp.path, v).some
    }
  }

  private object QNamePath {
    def unapply(path: ADir): Option[QName] = {
      if(depth(path) === 1) {
        NCName.fromString(prettyPrint(path).drop(1).dropRight(1))
          .toOption map (QName.unprefixed(_))
      } else none
    }
  }

  /* Discards nested projection guards. The existence of a path range index a/b/c
   * guarantees that the nested projection a/b/c is valid. */
  private def planPathIndex[Q](fm: FreeMap[T])(
    implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
  ): Option[Q] = ProjectPath.elideGuards(ProjectPath.foldProjectField(fm)) match {
    case PathProjection(path, const) =>
      Query.PathRange[T[EJson], Q](IList(prettyPrint(path).dropRight(1)), ComparisonOp.EQ, IList(const)).embed.some
  }

  private def planElementIndex[Q](fm: FreeMap[T])(
    implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
  ): Option[Q] = ProjectPath.elideGuards(ProjectPath.foldProjectField(fm)) match {
    case PathProjection(QNamePath(qname), const) =>
      Query.ElementRange[T[EJson], Q](IList(qname), ComparisonOp.EQ, IList(const)).embed.some
  }

  private def validQuery[Q](q: Option[Q])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Option[Q]] = q match {
    case Some(qry) => queryIsValid[F, Q, T[EJson], FMT](qry).ifM(qry.some.point[F], none.point[F])
    case None      => none.point[F]
  }
}
