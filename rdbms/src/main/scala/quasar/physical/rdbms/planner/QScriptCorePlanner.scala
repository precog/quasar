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

package quasar.physical.rdbms.planner

import slamdata.Predef._
import quasar.common.SortDir
import quasar.fp.ski._
import quasar.fp._
import quasar.{NameGenerator, qscript}
import quasar.Planner.PlannerErrorME
import Planner._
import sql._
import sql.Indirections._
import sql.SqlExpr._
import sql.{SqlExpr, _}
import sql.SqlExpr.Select._
import quasar.qscript.{ExcludeId, FreeMap, MapFunc, QScriptCore, QScriptTotal, Reduce, ReduceFuncs, ShiftType}
import quasar.qscript.{MapFuncCore => MFC}
import quasar.qscript.{MapFuncDerived => MFD}
import MFC._
import MFD._

import matryoshka._
import matryoshka.data._
import matryoshka.data.free.freeEqual
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._
import Scalaz._

class QScriptCorePlanner[T[_[_]]: BirecursiveT: ShowT: EqualT,
F[_]: Monad: NameGenerator: PlannerErrorME](
    mapFuncPlanner: Planner[T, F, MapFunc[T, ?]])
    extends Planner[T, F, QScriptCore[T, ?]] {

  private def processFreeMap(f: FreeMap[T],
                     alias: SqlExpr[T[SqlExpr]]): F[T[SqlExpr]] =
    f.cataM(interpretM(κ(alias.embed.η[F]), mapFuncPlanner.plan))

  private def take(fromExpr: F[T[SqlExpr]], countExpr: F[T[SqlExpr]]): F[T[SqlExpr]] =
    (fromExpr |@| countExpr)(Limit(_, _).embed)

  private def drop(fromExpr: F[T[SqlExpr]], countExpr: F[T[SqlExpr]]): F[T[SqlExpr]] =
    (fromExpr |@| countExpr)(Offset(_, _).embed)

  private def compile(expr: qscript.FreeQS[T], src: T[SqlExpr]): F[T[SqlExpr]] = {
    val compiler = Planner[T, F, QScriptTotal[T, ?]].plan
    expr.cataM(interpretM(κ(src.point[F]), compiler))
  }

  val unref: T[SqlExpr] = SqlExpr.Unreferenced[T[SqlExpr]]().embed

  def plan: AlgebraM[F, QScriptCore[T, ?], T[SqlExpr]] = {
    case qscript.Map(`unref`, f) =>
      processFreeMap(f, SqlExpr.Null[T[SqlExpr]])
    case qscript.Map(src, f) =>
      for {
        fromAlias <- genId[T[SqlExpr], F](deriveIndirection(src))
        selection <- processFreeMap(f, fromAlias)
          .map(idToWildcard[T])
      } yield {
        Select(
          Selection(selection, none, deriveIndirection(src)),
          From(src, fromAlias),
          join = none,
          filter = none,
          groupBy = none,
          orderBy = nil
        ).embed
      }
    case qscript.Sort(src, bucket, order) =>
      def createOrderBy(id: SqlExpr.Id[T[SqlExpr]]):
      ((FreeMap[T], SortDir)) => F[OrderBy[T[SqlExpr]]] = {
        case (qs, dir) =>
          processFreeMap(qs, id).map { expr =>
            OrderBy(expr, dir)
          }
      }
      for {
        fromAlias <- genId[T[SqlExpr], F](deriveIndirection(src))
        orderByExprs <- order.traverse(createOrderBy(fromAlias))
        bucketExprs <- bucket.map((_, orderByExprs.head.sortDir)).traverse(createOrderBy(fromAlias))
      }
        yield {
          Select(
            Selection(*, none, deriveIndirection(src)),
            From(src, fromAlias),
            join = none,
            filter = none,
            groupBy = none,
            orderBy = bucketExprs ++ orderByExprs.toList
          ).embed
        }
    case qscript.Subset(src, from, sel, count) =>
      val fromExpr: F[T[SqlExpr]]  = compile(from, src)
      val countExpr: F[T[SqlExpr]] = compile(count, src)

      sel match {
        case qscript.Drop   => drop(fromExpr, countExpr)
        case qscript.Take   => take(fromExpr, countExpr)
        case qscript.Sample => take(fromExpr, countExpr) // TODO needs better sampling (which connector doesn't?)
      }

    case qscript.Unreferenced() => unref.point[F]

    case qscript.Filter(src, f) =>
      for {
        fromAlias <- genId[T[SqlExpr], F](deriveIndirection(src))
        filterExp <- processFreeMap(f, fromAlias)
      } yield {
      Select(
        Selection(*, none, deriveIndirection(src)),
        From(src, fromAlias),
        join = none,
        Some(Filter(filterExp)),
        groupBy = none,
        orderBy = nil
      ).embed
    }

    case qUnion@qscript.Union(src, left, right) =>
      (compile(left, src) |@| compile(right, src))(Union(_,_).embed)

    case reduce@qscript.Reduce(src, bucket, reducers, repair) => for {
      alias <- genId[T[SqlExpr], F](deriveIndirection(src))
      gbs   <- bucket.traverse(processFreeMap(_, alias))
      rds   <- reducers.traverse(_.traverse(processFreeMap(_, alias)) >>=
        reduceFuncPlanner[T, F].plan)
      rep <- repair.cataM(interpretM(
        _.idx.fold(
          leftIdx => gbs(leftIdx).point[F],
          rightIdx => rds(rightIdx).point[F]
        ),
        Planner.mapFuncPlanner[T, F].plan)
      ).map(idToWildcard[T])
    } yield {
      val (selection, groupBy) = reduce match {
        case DistinctPattern() =>
          (Distinct[T[SqlExpr]](rep).embed, none)
        case _ => (rep, GroupBy(gbs).some)
      }

      Select(
        Selection(selection, none, Default),
        From(src, alias),
        join = none,
        filter = none,
        groupBy = groupBy,
        orderBy = nil
       ).embed
    }

    case qscript.LeftShift(src, struct, ExcludeId, ShiftType.Array, _, repair) =>
      for {
        structAlias <- genId[T[SqlExpr], F](deriveIndirection(src))
        structExpr  <- processFreeMap(struct.linearize, structAlias)
        right = ArrayUnwind(structExpr)
        repaired <- processJoinFunc(mapFuncPlanner)(repair, structAlias, right)
        result = Select[T[SqlExpr]](
          Selection(repaired, None, deriveIndirection(src)), From(src, structAlias), none, none, none, Nil)
      } yield {
        result.embed
      }

    case other =>
      notImplemented(s"QScriptCore: $other", this)
  }

  object DistinctPattern {

    def unapply[A:Equal](qs: QScriptCore[T, A]): Boolean = qs match {
      case Reduce(_, bucket :: Nil, ReduceFuncs.Arbitrary(arb) :: Nil, _) if bucket === arb  => true
      case _ => false
    }
  }
}
