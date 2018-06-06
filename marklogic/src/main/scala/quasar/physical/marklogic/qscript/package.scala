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

package quasar.physical.marklogic

import slamdata.Predef._
import quasar.contrib.scalaz.MonadError_
import quasar.ejson.{EJson, Str}
import quasar.contrib.iota.{copkTraverse, copkShow}
import quasar.fp.ski.κ
import quasar.contrib.matryoshka.totally
import quasar.contrib.pathy.{AFile, UriPathCodec}
import quasar.contrib.scalaz.MonadError_
import quasar.physical.marklogic.cts.Query
import quasar.physical.marklogic.xquery.{cts => ctsfn, _}
import quasar.physical.marklogic.xquery.syntax._
import quasar.physical.marklogic.xcc._
import quasar.qscript._
import quasar.qscript.MapFuncsCore.{Eq, Neq, TypeOf, Constant}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import iotaz.CopK
import iotaz.TNilK
import iotaz.TListK.:::

package object qscript {
  type MarkLogicPlanErrT[F[_], A] = EitherT[F, MarkLogicPlannerError, A]

  type MonadPlanErr[F[_]] = MonadError_[F, MarkLogicPlannerError]

  type PathMapFunc[T[_[_]], A]   = CopK[ProjectPath ::: MapFuncCore[T, ?] ::: MapFuncDerived[T, ?] ::: TNilK, A]
  type FreePathMap[T[_[_]]]      = Free[PathMapFunc[T, ?], Hole]
  type CoMapFunc[T[_[_]], A]     = CoEnv[Hole, MapFunc[T, ?], A]
  type CoPathMapFunc[T[_[_]], A] = CoEnv[Hole, PathMapFunc[T, ?], A]

  object MonadPlanErr {
    def apply[F[_]](implicit F: MonadPlanErr[F]): MonadPlanErr[F] = F
  }

  /** Matches "iterative" FLWOR expressions, those involving at least one `for` clause. */
  object IterativeFlwor {
    def unapply(xqy: XQuery): Option[(NonEmptyList[BindingClause], Option[XQuery], IList[(XQuery, SortDirection)], Boolean, XQuery)] = xqy match {
      case XQuery.Flwor(clauses, filter, order, isStable, result) if clauses.any(BindingClause.forClause.nonEmpty) =>
        Some((clauses, filter, order, isStable, result))

      case _ => None
    }
  }

  val EJsonTypeKey  = "_ejson.type"
  val EJsonValueKey = "_ejson.value"

  /** XQuery evaluating to the documents having the specified format in the directory. */
  def directoryDocuments[FMT: SearchOptions](uri: XQuery, includeDescendants: Boolean): XQuery =
    ctsfn.search(
      expr    = fn.doc(),
      query   = ctsfn.directoryQuery(uri, (includeDescendants ? "infinity" | "1").xs),
      options = SearchOptions[FMT].searchOptions)

  /** XQuery evaluating to the document node at the given URI. */
  def documentNode[FMT: SearchOptions](uri: XQuery): XQuery =
    ctsfn.search(
      expr    = fn.doc(),
      query   = ctsfn.documentQuery(uri),
      options = SearchOptions[FMT].searchOptions)

  /** XQuery evaluating to the document node at the given path. */
  def fileNode[FMT: SearchOptions](file: AFile): XQuery =
    documentNode[FMT](UriPathCodec.printPath(file).xs)

  /** XQuery evaluating to the root node of the document at the given path. */
  def fileRoot[FMT: SearchOptions](file: AFile): XQuery =
    fileNode[FMT](file) `/` axes.child.node()

  def mapFuncXQuery[T[_[_]]: BirecursiveT, F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT](
    fm: FreeMap[T],
    src: XQuery
  )(implicit
    SP:  StructuralPlanner[F, FMT]
  ): F[XQuery] =
    fm.project match {
      case MapFuncCore.StaticArray(elements) =>
        for {
          xqyElts <- elements.traverse(planMapFunc[T, F, FMT, Hole](_)(κ(src)))
          arrElts <- xqyElts.traverse(SP.mkArrayElt)
          arr     <- SP.mkArray(mkSeq(arrElts))
        } yield arr

      case MapFuncCore.StaticMap(entries) =>
        for {
          xqyKV <- entries.traverse(_.bitraverse({
                     case Embed(MapFuncCore.EC(Str(s))) => s.xs.point[F]
                     case key                       => invalidQName[F, XQuery](key.convertTo[Fix[EJson]].shows)
                   },
                   planMapFunc[T, F, FMT, Hole](_)(κ(src))))
          elts  <- xqyKV.traverse((SP.mkObjectEntry _).tupled)
          map   <- SP.mkObject(mkSeq(elts))
        } yield map

      case other => planMapFunc[T, F, FMT, Hole](other.embed)(κ(src))
    }

  def mergeXQuery[T[_[_]]: BirecursiveT, F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT](
    jf: JoinFunc[T],
    l: XQuery,
    r: XQuery
  )(implicit
    SP: StructuralPlanner[F, FMT]
  ): F[XQuery] =
    planMapFunc[T, F, FMT, JoinSide](jf) {
      case LeftSide  => l
      case RightSide => r
    }

  def planMapFunc[T[_[_]]: BirecursiveT, F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT, A](
    freeMap: FreeMapA[T, A])(
    recover: A => XQuery
  )(implicit
    SP: StructuralPlanner[F, FMT]
  ): F[XQuery] =
    freeMap.transCata[FreeMapA[T, A]](rewriteNullCheck[T, FreeMapA[T, A], A])
      .cataM(interpretM(recover(_).point[F], MapFuncPlanner[F, FMT, MapFunc[T, ?]].plan))

  /** Returns whether the query is valid and can be executed.
    *
    * TODO: Return any missing indexes when invalid.
    */
  def queryIsValid[F[_]: Monad: Xcc, Q, V, FMT](query: Q)(
    implicit Q:  Recursive.Aux[Q, Query[V, ?]],
             V:  Recursive.Aux[V, EJson],
             SP: StructuralPlanner[F, FMT]
  ): F[Boolean] = {
    val err = axes.descendant.elementNamed("error:error")
    val search = ((inr: XQuery) => fn.empty(xdmp.plan(ctsfn.search(fn.doc(), inr)) `//` err))
    val xqy = query.cataM(Query.toXQuery[V, F](EJsonPlanner.plan[V, F, FMT])) map search

    xqy >>= (Xcc[F].queryResults(_) map booleanResult)
  }

  def rebaseXQuery[T[_[_]]: BirecursiveT, F[_]: Monad, FMT, Q](
    fqs: FreeQS[T],
    src: Search[Q] \/ XQuery
  )(implicit
    Q  : Birecursive.Aux[Q, Query[T[EJson], ?]],
    QTP: Planner[F, FMT, QScriptTotal[T, ?], T[EJson]]
  ): F[Search[Q] \/ XQuery] =
    fqs.cataM(interpretM(κ(src.point[F]), QTP.plan[Q]))

  def rewriteNullCheck[T[_[_]]: BirecursiveT, U, E](
    implicit UR: Recursive.Aux[U, CoEnv[E, MapFunc[T, ?], ?]],
             UC: Corecursive.Aux[U, CoEnv[E, MapFunc[T, ?], ?]]
  ): CoEnv[E, MapFunc[T, ?], U] => CoEnv[E, MapFunc[T, ?], U] = {

    object NullLit {
      def unapply[A](mfc: CoEnv[E, MapFunc[T, ?], A]): Boolean =
        mfc.run.exists[MapFunc[T, A]] {
          case MFC(Constant(ej)) => EJson.isNull(ej)
          case _                 => false
        }
    }

    val nullString: U =
      UC.embed(CoEnv(MFC(Constant[T, U](EJson.fromCommon(Str[T[EJson]]("null")))).right))

    fa => CoEnv(fa.run.map (totally {
      case MFC(Eq(lhs, Embed(NullLit())))  => MFC(Eq(UC.embed(CoEnv(\/-(MFC(TypeOf(lhs))))), nullString))
      case MFC(Eq(Embed(NullLit()), rhs))  => MFC(Eq(UC.embed(CoEnv(MFC[T, U](TypeOf(rhs)).right)), nullString))
      case MFC(Neq(lhs, Embed(NullLit()))) => MFC(Neq(UC.embed(CoEnv(MFC[T, U](TypeOf(lhs)).right)), nullString))
      case MFC(Neq(Embed(NullLit()), rhs)) => MFC(Neq(UC.embed(CoEnv(MFC[T, U](TypeOf(rhs)).right)), nullString))
    }))
  }

  ////

  private def invalidQName[F[_]: MonadPlanErr, A](s: String): F[A] =
    MonadError_[F, MarkLogicPlannerError].raiseError(
      MarkLogicPlannerError.invalidQName(s))
}
