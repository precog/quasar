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

import slamdata.Predef._

import quasar.qscript.{MapFuncCore, MapFuncDerived}
import quasar.contrib.iota.mkInject

import matryoshka.{AlgebraM, BirecursiveT, RecursiveT}
import matryoshka.{AlgebraM, RecursiveT}
import scalaz.{Applicative, Monad}
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

abstract class MapFuncPlanner[T[_[_]], F[_]: Applicative, MF[_]] {
  def plan(cake: Precog): PlanApplicator[cake.type]

  // this class is necessary so the [A] param can be to the right of the (cake) param
  abstract class PlanApplicator[P <: Precog](val cake: P) {
    import cake.trans._

    def apply[A <: SourceType](id: cake.trans.TransSpec[A]): AlgebraM[F, MF, TransSpec[A]]
  }
}

object MapFuncPlanner {
  def apply[T[_[_]], F[_], MF[_]]
    (implicit ev: MapFuncPlanner[T, F, MF]): MapFuncPlanner[T, F, MF] =
    ev

  implicit def copk[T[_[_]], G[_], LL <: TListK](implicit M: Materializer[T, G, LL]): MapFuncPlanner[T, G, CopK[LL, ?]] =
    M.materialize(offset = 0)

  sealed trait Materializer[T[_[_]], G[_], LL <: TListK] {
    def materialize(offset: Int): MapFuncPlanner[T, G, CopK[LL, ?]]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T[_[_]], G[_]: Applicative, F[_]](
      implicit
      F: MapFuncPlanner[T, G, F]
    ): Materializer[T, G, F ::: TNilK] = new Materializer[T, G, F ::: TNilK] {
      override def materialize(offset: Int): MapFuncPlanner[T, G, CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new MapFuncPlanner[T, G, CopK[F ::: TNilK, ?]] {
          def plan(cake: Precog): PlanApplicator[cake.type] =
            new PlanApplicatorCopK(cake)

          final class PlanApplicatorCopK[P <: Precog](override val cake: P)
            extends PlanApplicator[P](cake) {
            import cake.trans._

            def apply[A <: SourceType](id: cake.trans.TransSpec[A]): AlgebraM[G, CopK[F ::: TNilK, ?], TransSpec[A]] = {
              case I(fa) => F.plan(cake).apply(id)(fa)
            }
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T[_[_]], G[_]: Applicative, F[_], LL <: TListK](
      implicit
      F: MapFuncPlanner[T, G, F],
      LL: Materializer[T, G, LL]
    ): Materializer[T, G, F ::: LL] = new Materializer[T, G, F ::: LL] {
      override def materialize(offset: Int): MapFuncPlanner[T, G, CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new MapFuncPlanner[T, G, CopK[F ::: LL, ?]] {
          def plan(cake: Precog): PlanApplicator[cake.type] =
            new PlanApplicatorCopK(cake)

          final class PlanApplicatorCopK[P <: Precog](override val cake: P)
            extends PlanApplicator[P](cake) {
            import cake.trans._

            def apply[A <: SourceType](id: cake.trans.TransSpec[A]): AlgebraM[G, CopK[F ::: LL, ?], TransSpec[A]] = {
              case I(fa) => F.plan(cake).apply(id)(fa)
              case other => LL.materialize(offset + 1).plan(cake).apply(id)(other.asInstanceOf[CopK[LL, TransSpec[A]]])
            }
          }
        }
      }
    }
  }

  implicit def mapFuncCore[T[_[_]]: RecursiveT, F[_]: Applicative]
    : MapFuncPlanner[T, F, MapFuncCore[T, ?]] =
    new MapFuncCorePlanner[T, F]

  implicit def mapFuncDerived[T[_[_]]: BirecursiveT, F[_]: Monad]
    : MapFuncPlanner[T, F, MapFuncDerived[T, ?]] =
    new MapFuncDerivedPlanner[T, F](mapFuncCore)
}
