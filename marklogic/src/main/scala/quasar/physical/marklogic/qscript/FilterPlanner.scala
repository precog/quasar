/*
 * Copyright 2014–2018 SlamData Inc.
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

import quasar.contrib.pathy._
import quasar.ejson.EJson
import quasar.physical.marklogic.DocType
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._
import quasar.qscript.{MapFuncsCore => MFCore, MFC => _, _}
import slamdata.Predef._
import quasar.contrib.iota.{copkTraverse, copkFunctor}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.patterns._
import matryoshka.implicits._
import pathy.Path._
import xml.name._

import scalaz._, Scalaz._

private[qscript] abstract class FilterPlanner[T[_[_]]: RecursiveT, FMT] {
  import FilterPlanner.flattenDir

  def plan[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
    FMT: SearchOptions: StructuralPlanner[F, ?],
    Q](src: Search[Q], f: FreeMap[T])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): F[Option[IndexPlan[Q]]]

  private def strPath(dir0: ADir): String =
    "/" ++ flattenDir(dir0).map(PathCodec.placeholder('/').escape(_)).intercalate("/")

  private object PathProjection {
    private object MFComp {
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

  /* Discards nested projection guards. The existence of a path range index a/b/c
   * guarantees that the nested projection a/b/c is valid. */
  private def rewrite(fm: FreeMap[T]): FreePathMap[T] =
    ProjectPath.elideGuards(ProjectPath.foldProjectKey(fm))

  object StarIndexPlanner {
    def apply[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[IndexPlan[Q]] = rewrite(fm) match {
      case PathProjection(op, path, const) => {
        val starPath = rebaseA(rootDir[Sandboxed] </> dir("*"))(path)
        val q = Query.PathRange[T[EJson], Q](
          IList(strPath(starPath)), op, IList(const)).embed
        val src0 = Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src)

        IndexPlan(src0, false).some
      }
      case _ => none
    }
  }

  object PathIndexPlanner {
    def apply[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[IndexPlan[Q]] = rewrite(fm) match {
      case PathProjection(op, path, const) => {
        val q = Query.PathRange[T[EJson], Q](IList(strPath(path)), op, IList(const)).embed
        val src0 = Search.query.modify((qr: Q) => Q.embed(Query.And(IList(qr, q))))(src)

        IndexPlan(src0, false).some
      }
      case _ => none
    }
  }

  object ElementIndexPlanner {
    import axes.child

    private object QNameDirName {
      def unapply(path: ADir): Option[QName] =
        dirName(path).map(_.value) >>= (QName.string.getOption(_))
    }

    private object DirName {
      def unapply(path: ADir): Option[DirName] =
        dirName(path)
    }

    private def xmlProjections(path: ADir): Option[XQuery] =
      if(depth(path) >= 1)
        flattenDir(path).map(child.elementNamed(_))
          .foldLeft(child.*)((path, segment) => path `/` segment).some
      else none

    private def jsonProjections(path: ADir): Option[XQuery] =
      flattenDir(path).map(child.nodeNamed(_))
        .foldLeft1Opt((path, segment) => path `/` segment)

    private def elementRange[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[(Search[Q], ADir)] = rewrite(fm) match {
      case PathProjection(op, dir0 @ QNameDirName(qname), const) => {
        val q = Query.ElementRange[T[EJson], Q](IList(qname), op, IList(const)).embed

        Search.query.modify((qr: Q) =>
          Q.embed(Query.And(IList(qr, q))))(src).some strengthR dir0
      }
      case _ => none
    }

    private def jsonPropertyRange[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[(Search[Q], ADir)] = rewrite(fm) match {
      case PathProjection(op, dir0 @ DirName(seg), const) => {
        val q = Query.JsonPropertyRange[T[EJson], Q](IList(seg.value), op, IList(const)).embed

        Search.query.modify((qr: Q) =>
          Q.embed(Query.And(IList(qr, q))))(src).some strengthR dir0
      }
      case _ => none
    }

    def planXml[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[IndexPlan[Q]] = elementRange(src, fm) >>= {
      case (src0, dir0) => {
        val src1 = Search.pred.modify(pred0 => pred0 ++ xmlProjections(dir0).map(IList(_))
          .getOrElse(IList()))(src0)

        IndexPlan(src1, true).some
      }
    }

    def planJson[Q](src: Search[Q], fm: FreeMap[T])(
      implicit Q: Corecursive.Aux[Q, Query[T[EJson], ?]]
    ): Option[IndexPlan[Q]] = jsonPropertyRange(src, fm) >>= {
      case (src0, dir0) => {
        val src1 = Search.pred.modify(pred0 => pred0 ++ jsonProjections(dir0).map(IList(_))
          .getOrElse(IList()))(src0)

        IndexPlan(src1, true).some
      }
    }
  }

}

object FilterPlanner {
  def plan[T[_[_]]: BirecursiveT,
    F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
    FMT: SearchOptions: StructuralPlanner[F, ?], Q](src0: Search[Q] \/ XQuery, f: FreeMap[T])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]],
             P: FilterPlanner[T, FMT]
  ): F[Search[Q] \/ XQuery] = {
    src0 match {
      case (\/-(src)) =>
        xqueryFilter[T, F, FMT, Q](src, f) map (_.right[Search[Q]])
      case (-\/(src)) if anyDocument(src.query) =>
        fallbackFilter[T, F, FMT, Q](src, f) map (_.right[Search[Q]])
      case (-\/(src)) =>
        P.plan[F, FMT, Q](src, f) >>= {
          case Some(IndexPlan(search, true))  => fallbackFilter[T, F, FMT, Q](search, f) map (_.right[Search[Q]])
          case Some(IndexPlan(search, false)) => search.left[XQuery].point[F]
          case None => fallbackFilter[T, F, FMT, Q](src, f).map(_.right[Search[Q]])
        }
    }
  }

  def flattenDir(dir0: ADir): IList[String] =
    flatten(None, None, None, Some(_), Some(_), dir0).toIList.unite

  def anyDocument[T[_[_]], Q](q: Q)(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): Boolean = {
    val f: AlgebraM[Option, Query[T[EJson], ?], Q] = {
      case Query.Document(_) => none
      case other             => Q.embed(other).some
    }

    Q.cataM(q)(f).isEmpty
  }

  implicit def xmlFilterPlanner[T[_[_]]: BirecursiveT]: FilterPlanner[T, DocType.Xml] = new FilterPlanner[T, DocType.Xml] {
    def plan[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
      FMT: SearchOptions: StructuralPlanner[F, ?],
      Q](src: Search[Q], f: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
    ): F[Option[IndexPlan[Q]]] = {
      lazy val starQuery    = validIndexPlan[T, F, FMT, Q](StarIndexPlanner(src, f))
      lazy val elementQuery = validIndexPlan[T, F, FMT, Q](ElementIndexPlanner.planXml(src, f))

      (starQuery ||| elementQuery).run
    }
  }

  implicit def jsonFilterPlanner[T[_[_]]: BirecursiveT]: FilterPlanner[T, DocType.Json] = new FilterPlanner[T, DocType.Json] {
    def plan[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
      FMT: SearchOptions: StructuralPlanner[F, ?],
      Q](src: Search[Q], f: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
    ): F[Option[IndexPlan[Q]]] = {
      lazy val pathQuery    = validIndexPlan[T, F, FMT, Q](PathIndexPlanner(src, f))
      lazy val elementQuery = validIndexPlan[T, F, FMT, Q](ElementIndexPlanner.planJson(src, f))

      (pathQuery ||| elementQuery).run
    }
  }

  //

  private def fallbackFilter[T[_[_]]: BirecursiveT,
    F[_]: Monad: PrologW: QNameGenerator: MonadPlanErr,
    FMT: StructuralPlanner[F, ?]: SearchOptions,
    Q](src: Search[Q], f: FreeMap[T])(
    implicit Q: Recursive.Aux[Q, Query[T[EJson], ?]]
  ): F[XQuery] = {
    def interpretSearch(s: Search[Q]): F[XQuery] =
      Search.plan[F, Q, T[EJson], FMT](s, EJsonPlanner.plan[T[EJson], F, FMT])

    interpretSearch(src) >>= (xqueryFilter[T, F, FMT, Q](_: XQuery, f))
  }

  private def xqueryFilter[T[_[_]]: BirecursiveT,
    F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr,
    FMT: StructuralPlanner[F, ?], Q](src: XQuery, fm: FreeMap[T]
  ): F[XQuery] =
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

  private def validIndexPlan[T[_[_]]: RecursiveT,
    F[_]: Monad: Xcc,
    FMT: StructuralPlanner[F, ?],
    Q](src: Option[IndexPlan[Q]])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]]
  ): OptionT[F, IndexPlan[Q]] = src match {
    case Some(indexPlan) =>
      OptionT(queryIsValid[F, Q, T[EJson], FMT](indexPlan.src.query)
        .ifM(src.point[F], none.point[F]))
    case None =>
      OptionT(none.point[F])
  }

}
