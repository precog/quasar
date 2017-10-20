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
import quasar.fp._
import quasar.{NameGenerator, qscript, Data => QData}
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.physical.rdbms.planner.Planner
import qscript.{MapFuncCore, MapFuncsCore => MF}

import matryoshka._
import matryoshka.implicits._
import scalaz._
import Scalaz._
import SqlExpr._

final class MapFuncCorePlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Applicative: NameGenerator: PlannerErrorME]
  extends Planner[T, F, MapFuncCore[T, ?]] {

  val undefined: T[SqlExpr] = Null[T[SqlExpr]]().embed
  def id(str: String): SqlExpr.Id[T[SqlExpr]] = SqlExpr.Id[T[SqlExpr]](str)

  def plan: AlgebraM[F, MapFuncCore[T, ?], T[SqlExpr]] = {
    case MF.Constant(v) =>
      Data[T[SqlExpr]](v.cata(QData.fromEJson)).embed.η[F]
    case MF.Undefined() =>
      undefined.η[F]
    case MF.JoinSideName(n) =>
      unexpected(s"JoinSideName(${n.shows})")
    case MF.Length(expr) =>
      Length[T[SqlExpr]](expr).embed.η[F]
    case MF.Guard(_, _, expr, _) =>
      expr.η[F]
    case MF.ProjectField(_, b) =>
      b.η[F]
    case MF.MakeMap(key, value) =>
      key.project match {
        case Data(QData.Str(keyStr)) =>
          ExprWithAlias[T[SqlExpr]](value, id(keyStr)).embed.η[F]
        case other =>
          unsupported(s"MakeMap with key = $other")
      }
    case MF.ConcatMaps(map1, map2) =>
      ExprPair[T[SqlExpr]](map1, map2).embed.η[F]

    case other => PlannerErrorME[F].raiseError(InternalError.fromMsg(s"unsupported MapFuncCore: $other"))
  }
}