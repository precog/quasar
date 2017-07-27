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
import quasar.fp.ski.ι
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
import pathy.Path._
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
    src0 match {
      case (\/-(src)) => xqueryFilter(src, f) map (_.right[Search[Q]])
      case (-\/(src)) => {
        lazy val starQuery    = StarPlanner.plan(src, f)
        lazy val pathQuery    = PathIndexPlanner.plan(src, f)
        lazy val elementQuery = ElementIndexPlanner.plan(src, f)

        (pathQuery |@| starQuery |@| elementQuery) {
          case (Some(q), _, _)    => q.left[XQuery].point[F]
          case (_, Some(q), _)    => q.left[XQuery].point[F]
          case (_, _, Some(q))    => q.left[XQuery].point[F]
          case (None, None, None) => fallbackFilter(src, f) map (_.right[Search[Q]])
        }.join
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


  private object PathProjection {
    def unapply[T[_[_]]](fpm: FreePathMap[T]): Option[(ADir, T[EJson])] = fpm match {
      case Embed(CoEnv(\/-(MFPath(MFCore.Eq(Embed(CoEnv(\/-(PathProject(pp)))), Embed(CoEnv(\/-(MFPath(MFCore.Constant(v)))))))))) =>
        (pp.path, v).some
      case _ => none
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

  private object StarPlanner {
    private def starFilter[Q](src: Search[Q])(path: ADir, q: Q)(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Search[Q]] =
      (Search.query split[Search[Q], Search[Q], Option[XQuery], Option[XQuery]] Search.pred)
        .modifyF[Option] { search: (Q, Option[XQuery]) =>
          search match {
            case (_, Some(pred)) =>
              None
            case (qr, None) =>
              (Q.embed(Query.And(IList(qr, q))),
                axes.child.elementNamed(prettyPrint(path)).some).some
          }
        }(src.squared) map (_._2)

    private def planPathStarIndex[Q](fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[(ADir, Q)] = rewrite(fm) match {
      case PathProjection(path, const) => {
        val starPath = rebaseA(rootDir[Sandboxed] </> dir("*"))(path)
        val q = Query.PathRange[T[EJson], Q](
          IList(prettyPrint(starPath).dropRight(1)),
          ComparisonOp.EQ, IList(const)).embed

        (starPath, q).some
      }
      case _ => none
    }

    def plan[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
    ): F[Option[Search[Q]]] = {
      val nil: F[Option[Search[Q]]] = none[Search[Q]].point[F]

      val pl = for {
        plan <- planPathStarIndex[Q](fm)
        (dir0, qry) = plan
        search <- starFilter[Q](src)(dir0, qry)
        isValid = validQuery[Q](qry).map(_.isDefined)
      } yield (search, isValid)

      pl.fold(nil)(x => x._2.ifM(x._1.some.point[F], nil))
    }
  }

  private object PathIndexPlanner {
    private def planPathIndex[Q](fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Q] = rewrite(fm) match {
      case PathProjection(path, const) =>
        Query.PathRange[T[EJson], Q](IList(prettyPrint(path).dropRight(1)),
          ComparisonOp.EQ, IList(const)).embed.some
      case _ => none
    }

    def plan[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
    ): F[Option[Search[Q]]] = {
      val qry: Option[Q] = planPathIndex[Q](fm)
      val search: Option[Search[Q]] = qry.map(indexedFilter(src, _))
      val valid: F[Boolean] = distF(qry.map(validQuery[Q](_))).map(_.isDefined)

      valid.ifM(search.point[F], none[Search[Q]].point[F])
    }
  }

  private object ElementIndexPlanner {
    private def planElementIndex[Q](fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[Q] = rewrite(fm) match {
      case PathProjection(QNamePath(qname), const) =>
        Query.ElementRange[T[EJson], Q](IList(qname),
          ComparisonOp.EQ, IList(const)).embed.some
      case _ => none
    }

    def plan[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
    ): F[Option[Search[Q]]] = {
      val qry: Option[Q] = planElementIndex[Q](fm)
      val search: Option[Search[Q]] = qry.map(indexedFilter(src, _))
      val valid: F[Boolean] = distF(qry.map(validQuery[Q](_))).map(_.isDefined)

      valid.ifM(search.point[F], none[Search[Q]].point[F])
    }
  }

  private def distF[F[_]: Applicative, A](mfa: Option[F[Option[A]]]): F[Option[A]] =
    mfa.fold(none[A].point[F])(ι)

  private def indexedFilter[Q](src: Search[Q], q: Q)(
    implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
  ): Search[Q] =
    Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src)


  private def validQuery[Q](qry: Q)(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Option[Q]] = queryIsValid[F, Q, T[EJson], FMT](qry) >>= {
    case true  => qry.some.point[F]
    case false => none.point[F]
  }

  /* Discards nested projection guards. The existence of a path range index a/b/c
   * guarantees that the nested projection a/b/c is valid. */
  private def rewrite(fm: FreeMap[T]): FreePathMap[T] =
    ProjectPath.elideGuards(ProjectPath.foldProjectField(fm))

}
