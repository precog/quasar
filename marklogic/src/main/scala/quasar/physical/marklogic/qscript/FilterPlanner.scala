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
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._
import quasar.qscript.{MapFuncsCore => MFCore, MFC => _, _}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.patterns._
import matryoshka.implicits._
import pathy.Path._
import xml.name._

import scalaz._, Scalaz._

private[qscript] final class FilterPlanner[
  F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
  FMT: SearchOptions,
  T[_[_]]: BirecursiveT: ShowT
](implicit SP: StructuralPlanner[F, FMT]) {
  def plan[Q](src0: Search[Q] \/ XQuery, f: FreeMap[T])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]],
             P: FormatFilterPlanner[FMT]
  ): F[Search[Q] \/ XQuery] = {
    src0 match {
      case (\/-(src)) =>
        xqueryFilter(src, f) map (_.right[Search[Q]])
      case (-\/(src)) if anyDocument(src.query) =>
        fallbackFilter(src, f) map (_.right[Search[Q]])
      case (-\/(src)) =>
        P.plan[F, FMT, T, Q](src, f) >>= {
          case Some(search) => search.left[XQuery].point[F]
          case None         => fallbackFilter(src, f).map(_.right[Search[Q]])
        }
    }
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

  private def anyDocument[T[_[_]]: BirecursiveT, Q](q: Q)(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): Boolean = {
    val f: Algebra[Query[T[EJson], ?], Boolean] = {
      case Query.Document(_) => true
      case other             => false
    }

    Q.cata(q)(f)
  }

  private object PathProjection {
    object MFComp {
      def unapply[T[_[_]], A](mfc: MapFuncCore[T, A]): Option[(ComparisonOp, A, A)] = mfc match {
        case MFCore.Eq(a1, a2)  => (ComparisonOp.EQ, a1, a2).some
        case MFCore.Neq(a1, a2) => (ComparisonOp.NE, a1, a2).some
        case MFCore.Lt(a1, a2)  => (ComparisonOp.LT, a1, a2).some
        case MFCore.Lte(a1, a2) => (ComparisonOp.LE, a1, a2).some
        case MFCore.Gt(a1, a2)  => (ComparisonOp.GT, a1, a2).some
        case MFCore.Gte(a1, a2) => (ComparisonOp.GE, a1, a2).some
        case _                  => none
      }
    }

    def unapply[T[_[_]]](fpm: FreePathMap[T]): Option[(ComparisonOp, ADir, T[EJson])] = fpm match {
      case Embed(CoEnv(\/-(MFPath(MFComp(op, Embed(CoEnv(\/-(PathProject(pp)))), Embed(CoEnv(\/-(MFPath(MFCore.Constant(v)))))))))) =>
        (op, pp.path, v).some
      case _ => none
    }
  }

  private object QNamePath {
    def unapply(path: ADir): Option[QName] = {
      if(depth(path) === 1) {
        // Use Pathy to get the string, not prettyprint.
        NCName.fromString(prettyPrint(path).drop(1).dropRight(1))
          .toOption map (QName.unprefixed(_))
      } else none
    }
  }

  /* Discards nested projection guards. The existence of a path range index a/b/c
   * guarantees that the nested projection a/b/c is valid. */
  private def rewrite(fm: FreeMap[T]): FreePathMap[T] =
    ProjectPath.elideGuards(ProjectPath.foldProjectField(fm))

  object StarIndexPlanner {
    private def planPathStarIndex[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Search[Q]] = rewrite(fm) match {
      case PathProjection(op, path, const) => {
        val starPath = rebaseA(rootDir[Sandboxed] </> dir("*"))(path)
        val q = Query.PathRange[T[EJson], Q](
          IList(prettyPrint(starPath).dropRight(1)), op, IList(const)).embed

        Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src).some
      }
      case _ => none
    }

    def apply[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Search[Q]] = planPathStarIndex[Q](src, fm)
  }

  object PathIndexPlanner {
    private def planPathIndex[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Search[Q]] = rewrite(fm) match {
      case PathProjection(op, path, const) => {
        val q = Query.PathRange[T[EJson], Q](
          IList(prettyPrint(path).dropRight(1)),
          op, IList(const)).embed

        Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src).some
      }
      case _ => none
    }

    def apply[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Search[Q]] = planPathIndex[Q](src, fm)
  }

  object ElementIndexPlanner {
    import axes.child

    private def planElementIndex[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Search[Q]] = rewrite(fm) match {
      case PathProjection(op, QNamePath(qname), const) => {
        val q = Query.ElementRange[T[EJson], Q](IList(qname), op, IList(const)).embed
        val src0 = Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src)

        Search.pred.modify {
          case Some(pred) => None
          case None       => (child.* `/` child.elementNamed(QName.string(qname))).some
        }(src0).some
      }
      case _ => none
    }

    def apply[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Search[Q]] = planElementIndex[Q](src, fm)
  }

  def validSearch[Q](src: Option[Search[Q]])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Option[Search[Q]]] = src match {
    case Some(search) =>
      queryIsValid[F, Q, T[EJson], FMT](search.query).ifM(src.point[F], none.point[F])
    case None =>
      none.point[F]
  }
}
