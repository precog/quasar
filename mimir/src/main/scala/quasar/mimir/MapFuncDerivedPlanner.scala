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

package quasar.mimir

import quasar.fp.ski._
import quasar.qscript.{ExpandMapFunc, MapFuncCore, MapFuncDerived}

import matryoshka._
import scalaz.Monad

final class MapFuncDerivedPlanner[T[_[_]]: BirecursiveT, F[_]: Monad]
  (core: MapFuncPlanner[T, F, MapFuncCore[T, ?]])
  extends MapFuncPlanner[T, F, MapFuncDerived[T, ?]] {

  def plan(cake: Precog): AlgebraM[F, MapFuncDerived[T, ?], cake.trans.TransSpec1] =
    ExpandMapFunc.expand(core.plan(cake), κ(None))
}
