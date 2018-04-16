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

package quasar.mimir

import quasar.fp.ski._
import quasar.qscript.{ExpandMapFunc, MapFuncCore, MapFuncDerived, MapFuncsDerived}
import quasar.yggdrasil.bytecode.JType

import matryoshka.{AlgebraM, BirecursiveT}

import scalaz.Monad
import scalaz.syntax.monad._

final class MapFuncDerivedPlanner[T[_[_]]: BirecursiveT, F[_]: Monad]
   (core: MapFuncPlanner[T, F, MapFuncCore[T, ?]])
   extends MapFuncPlanner[T, F, MapFuncDerived[T, ?]] {

  def plan(cake: Precog): PlanApplicator[cake.type] =
    new PlanApplicatorDerived(cake)

  final class PlanApplicatorDerived[P <: Precog](override val cake: P)
    extends PlanApplicator[P](cake) {

    import cake.trans._
    import cake.Library._

    def apply[A <: SourceType](id: TransSpec[A]): AlgebraM[F, MapFuncDerived[T, ?], TransSpec[A]] = {
      case MapFuncsDerived.Ceil(src) => Unary.Ceil.spec(src).point[F]
      case MapFuncsDerived.Floor(src) => Unary.Floor.spec(src).point[F]
      case MapFuncsDerived.Abs(src) => Unary.Abs.spec(src).point[F]
      case MapFuncsDerived.Trunc(src) => Unary.Trunc.spec(src).point[F]
      case MapFuncsDerived.Round(src) => Unary.Round.spec(src).point[F]
      case MapFuncsDerived.Typecheck(src, tpe) =>
        (Typed(src, JType.fromType(tpe)): TransSpec[A]).point[F]
      case x => ExpandMapFunc.expand[T, F, TransSpec[A]](core.plan(cake)(id), κ(None)).apply(x)
    }
  }
}
