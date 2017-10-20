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

package quasar.physical.rdbms.planner.sql

import slamdata.Predef._
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.physical.rdbms.planner.Planner
import quasar.{NameGenerator, qscript}
import quasar.qscript.{FreeMap, MapFunc, QScriptCore}
import quasar.fp.ski._
import quasar.qscript
import qscript._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import quasar.physical.rdbms.planner.sql.SqlExpr._
import quasar.physical.rdbms.planner.sql.SqlExpr.Select.AllCols

import scalaz._
import Scalaz._

class QScriptCorePlanner[T[_[_]]: CorecursiveT,
F[_]: Monad: NameGenerator: PlannerErrorME](
    mapFuncPlanner: Planner[T, F, MapFunc[T, ?]])
    extends Planner[T, F, QScriptCore[T, ?]] {

  def processFreeMap(f: FreeMap[T]): F[T[SqlExpr]] =
    f.cataM(
      interpretM(κ(AllCols[T[SqlExpr]]().embed.η[F]), mapFuncPlanner.plan))

  def plan: AlgebraM[F, QScriptCore[T, ?], T[SqlExpr]] = {
    case qscript.Map(src, f) =>
      for {
        generatedAlias <- genId[T[SqlExpr], F]
        selection <- processFreeMap(f)
      } yield
        Select(
          Selection(selection, none),
          From(src, generatedAlias.some),
          filter = none
        ).embed

    case _ =>
      PlannerErrorME[F].raiseError(
        InternalError.fromMsg(s"unreachable QScriptCore"))
  }
}
