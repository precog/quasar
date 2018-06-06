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

package quasar.contrib

import slamdata.Predef.Int
import _root_.matryoshka.Delay
import _root_.scalaz.{Show, Functor, Equal, Traverse, Order}
import _root_.iotaz.{CopK, TListK}

package object iota {
  implicit def copkFunctor[LL <: TListK](implicit M: FunctorMaterializer[LL]): Functor[CopK[LL, ?]] = M.materialize(offset = 0)
  implicit def copkTraverse[LL <: TListK](implicit M: TraverseMaterializer[LL]): Traverse[CopK[LL, ?]] = M.materialize(offset = 0)
  implicit def copkEqual[LL <: TListK](implicit M: EqualKMaterializer[LL]): Delay[Equal, CopK[LL, ?]] = M.materialize(offset = 0)
  implicit def copkShow[LL <: TListK](implicit M: ShowKMaterializer[LL]): Delay[Show, CopK[LL, ?]] = M.materialize(offset = 0)
  implicit def copkOrder[LL <: TListK](implicit M: OrderKMaterializer[LL]): Delay[Order, CopK[LL, ?]] = M.materialize(offset = 0)

  def mkInject[F[_], LL <: TListK](i: Int): CopK.Inject[F, CopK[LL, ?]] = {
    CopK.Inject.injectFromInjectL[F, LL](
      CopK.InjectL.makeInjectL[F, LL](
        new TListK.Pos[LL, F] { val index: Int = i }
      )
    )
  }

  type ACopK[a] = CopK[_, a]
  type :<<:[F[_], G[a] <: ACopK[a]] = CopK.Inject[F, G]

}
