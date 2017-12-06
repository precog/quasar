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

package quasar.physical.rdbms.planner

import slamdata.Predef._
import quasar.common.SortDir
import quasar.fp.ski._
import quasar.{NameGenerator, qscript}
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.physical.rdbms.planner.sql.SqlExpr._
import quasar.physical.rdbms.planner.sql.{SqlExpr, genId}
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.qscript.{FreeMap, MapFunc, QScriptCore}

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.Scalaz._
import scalaz._

class QScriptCorePlanner[T[_[_]]: BirecursiveT,
F[_]: Monad: NameGenerator: PlannerErrorME](
    mapFuncPlanner: Planner[T, F, MapFunc[T, ?]])
    extends Planner[T, F, QScriptCore[T, ?]] {

  def processFreeMap(f: FreeMap[T],
                     alias: SqlExpr.Id[T[SqlExpr]]): F[T[SqlExpr]] =
    f.cataM(interpretM(κ(alias.embed.η[F]), mapFuncPlanner.plan))

  def refsToRowRefs(in: T[SqlExpr]): T[SqlExpr] = {
    (in.project match {
      case Refs(elems) =>
        RefsSelectRow(elems)
      case other =>
        other
    }).embed
  }

  def plan: AlgebraM[F, QScriptCore[T, ?], T[SqlExpr]] = {
    case qscript.Map(src, f) =>

      // TODO there's something wrong with aliases, this should be dealt with correctly
      val aliasG: F[SqlExpr.Id[T[SqlExpr]]] = src.project match {
        case SelectRow(_, From(_, alias), _, _) => alias.η[F]
        case _ => genId[T[SqlExpr], F]
      }

      for {
        generatedAlias <- aliasG
        selection <- processFreeMap(f, generatedAlias)
      } yield
        Select(
          Selection(selection, none),
          From(src, generatedAlias),
          filter = none
        ).embed
    case qscript.Sort(src, bucket, order) =>

      def createOrderBy(id: SqlExpr.Id[T[SqlExpr]]):
      ((FreeMap[T], SortDir)) => F[OrderBy[T[SqlExpr]]] = {
        case (qs, dir) =>
          processFreeMap(qs, id).map { expr =>
            val transformedExpr: T[SqlExpr] = expr.transCataT(refsToRowRefs)
            OrderBy(transformedExpr, dir)
          }
      }

      def orderByPushdown(in: T[SqlExpr]): F[T[SqlExpr]] = {
        in.project match {
          case s @ SqlExpr.SelectRow(_, from, _, _) =>
            for {
              orderByExprs <- order.traverse(createOrderBy(from.alias))
              bucketExprs <- bucket.map((_, orderByExprs.head.sortDir)).traverse(createOrderBy(from.alias))
            }
              yield s.copy(orderBy = bucketExprs ++ orderByExprs.toList).embed

          case other => other.embed.η[F]
        }
      }
      src.transCataTM(orderByPushdown)

    case qscript.Filter(src, f) =>

      def filterPushdown(in: T[SqlExpr]): F[T[SqlExpr]] = {
          in.project match {
            case s @ SqlExpr.SelectRow(_, from, _, _) =>
              processFreeMap(f, from.alias).map(
                ff => s.copy(filter = Some(Filter(ff))).embed.transCataT(refsToRowRefs))
            case other => other.embed.η[F]
          }
      }

      src.transCataTM(filterPushdown)

    case other =>
      PlannerErrorME[F].raiseError(
        InternalError.fromMsg(s"unsupported QScriptCore: $other"))
  }
}
