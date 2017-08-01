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

package quasar.physical.couchbase.planner

import quasar.fp.ski.κ
import quasar.physical.couchbase._
import quasar.Planner.{PlannerErrorME, InternalError}

import matryoshka._

final class UnreachablePlanner[T[_[_]], F[_]: PlannerErrorME, QS[_]] extends Planner[T, F, QS] {
  def plan: AlgebraM[F, QS, T[N1QL]] =
    κ(PlannerErrorME[F].raiseError(InternalError.fromMsg("unreachable")))
}
