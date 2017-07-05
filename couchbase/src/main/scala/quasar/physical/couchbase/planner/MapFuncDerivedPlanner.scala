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

import slamdata.Predef._
import quasar.NameGenerator
import quasar.physical.couchbase._
import quasar.Planner.PlannerErrorME
import quasar.qscript._

import scala.Predef.implicitly

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._

final class MapFuncDerivedPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Applicative: Monad: NameGenerator: PlannerErrorME]
  (core: Planner[T, F, MapFuncCore[T, ?]])
  extends Planner[T, F, MapFuncDerived[T, ?]] {

    private val planDerived: AlgebraM[(Option ∘ F)#λ, MapFuncDerived[T, ?], T[N1QL]] = { _ => None }

    def plan: AlgebraM[F, MapFuncDerived[T, ?], T[N1QL]] = { f =>
      planDerived(f).getOrElse(
        Free.roll(ExpandMapFunc.mapFuncDerived[T, MapFuncCore[T, ?]].expand(f)).cataM(
          interpretM(implicitly[Monad[F]].point[T[N1QL]](_), core.plan)
        )
      )
    }
}
