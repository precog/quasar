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

package quasar.physical.marklogic.qscript

import slamdata.Predef._

import quasar.contrib.iota.mkInject
import quasar.physical.marklogic.xquery._
import quasar.qscript._

import matryoshka._
import scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

abstract class MapFuncPlanner[F[_], FMT, MF[_]] {
  def plan: AlgebraM[F, MF, XQuery]
}

object MapFuncPlanner {

  def apply[F[_], FMT, MF[_]](implicit ev: MapFuncPlanner[F, FMT, MF]): MapFuncPlanner[F, FMT, MF] = ev
  
  implicit def copk[M[_], FMT, LL <: TListK](implicit M: Materializer[M, FMT, LL]): MapFuncPlanner[M, FMT, CopK[LL, ?]] =
    M.materialize(offset = 0)
  
  sealed trait Materializer[M[_], FMT, LL <: TListK] {
    def materialize(offset: Int): MapFuncPlanner[M, FMT, CopK[LL, ?]]
  }
  
  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[M[_], FMT, F[_]](
      implicit
      F: MapFuncPlanner[M, FMT, F]
    ): Materializer[M, FMT, F ::: TNilK] = new Materializer[M, FMT, F ::: TNilK] {
      override def materialize(offset: Int): MapFuncPlanner[M, FMT, CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new MapFuncPlanner[M, FMT, CopK[F ::: TNilK, ?]] {
          def plan: AlgebraM[M, CopK[F ::: TNilK, ?], XQuery] = {
            case I(fa) => F.plan(fa)
          }
        }
      }
    }
  
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[M[_], FMT, F[_], LL <: TListK](
      implicit
      F: MapFuncPlanner[M, FMT, F],
      LL: Materializer[M, FMT, LL]
    ): Materializer[M, FMT, F ::: LL] = new Materializer[M, FMT, F ::: LL] {
      override def materialize(offset: Int): MapFuncPlanner[M, FMT, CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new MapFuncPlanner[M, FMT, CopK[F ::: LL, ?]] {
          def plan: AlgebraM[M, CopK[F ::: LL, ?], XQuery] = {
            case I(fa) => F.plan(fa)
            case other => LL.materialize(offset + 1).plan(other.asInstanceOf[CopK[LL, XQuery]])
          }
        }
      }
    }
  }

  implicit def mapFuncCore[M[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT, T[_[_]]: RecursiveT](
    implicit
    SP: StructuralPlanner[M, FMT]
  ): MapFuncPlanner[M, FMT, MapFuncCore[T, ?]] =
    new MapFuncCorePlanner[M, FMT, T]

  implicit def mapFuncDerived[M[_]: Monad, FMT, T[_[_]]: CorecursiveT](
    implicit
    CP: MapFuncPlanner[M, FMT, MapFuncCore[T, ?]]
  ): MapFuncPlanner[M, FMT, MapFuncDerived[T, ?]] =
    new MapFuncDerivedPlanner[M, FMT, T]

}
