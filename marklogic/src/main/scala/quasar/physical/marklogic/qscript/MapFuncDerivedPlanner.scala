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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.fp.ski._
import quasar.physical.marklogic.xquery._
import quasar.qscript.{ExpandMapFunc, MapFuncCore, MapFuncDerived}

import matryoshka._
import scalaz._

private[qscript] final class MapFuncDerivedPlanner[
  F[_]: Monad,
  FMT,
  T[_[_]]: CorecursiveT
](implicit
  CP: MapFuncPlanner[F, FMT, MapFuncCore[T, ?]]
) extends MapFuncPlanner[F, FMT, MapFuncDerived[T, ?]] {

  val plan: AlgebraM[F, MapFuncDerived[T, ?], XQuery] = ExpandMapFunc.expand(CP.plan, κ(None))

}
