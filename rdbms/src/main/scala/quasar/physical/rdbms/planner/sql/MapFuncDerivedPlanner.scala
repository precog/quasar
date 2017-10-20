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
import quasar.fp.ski._
import quasar.NameGenerator
import quasar.Planner.PlannerErrorME
import quasar.qscript._
import quasar.physical.rdbms.planner.Planner

import matryoshka.{AlgebraM, BirecursiveT, ShowT}
import scalaz.Applicative
import scalaz.Monad

final class MapFuncDerivedPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Applicative: Monad: NameGenerator: PlannerErrorME]
(core: Planner[T, F, MapFuncCore[T, ?]])
  extends Planner[T, F, MapFuncDerived[T, ?]] {

  def plan: AlgebraM[F, MapFuncDerived[T, ?], T[SqlExpr]] = ExpandMapFunc.expand(core.plan, κ(None))
}
