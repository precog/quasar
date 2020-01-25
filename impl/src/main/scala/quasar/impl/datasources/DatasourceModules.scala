/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.datasources

import slamdata.Predef._

import quasar.{RateLimiting, RenderTreeT}
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.impl.DatasourceModule
import quasar.impl.IncompatibleModuleException.linkDatasource
import quasar.connector.{MonadResourceErr, QueryResult}
import quasar.qscript.MonadPlannerErr

import argonaut.Json
import argonaut.Argonaut.jEmptyObject

import fs2.Stream

import cats.{Monad, MonadError}
import cats.effect.{Resource, ConcurrentEffect, ContextShift, Timer, Bracket}
import cats.kernel.Hash
import cats.syntax.applicative._
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{ISet, EitherT, -\/, \/-}

import shims.{monadToScalaz, monadToCats}

import scala.concurrent.ExecutionContext

trait DatasourceModules[T[_[_]], F[_], G[_], I, C, R, P <: ResourcePathType] { self =>
  def create(i: I, ref: DatasourceRef[C]): EitherT[Resource[F, ?], CreateError[C], ManagedDatasource[T, F, G, R, P]]
  def sanitizeRef(inp: DatasourceRef[C]): DatasourceRef[C]
  def supportedTypes: F[ISet[DatasourceType]]

  def withMiddleware[H[_], S, Q <: ResourcePathType](
    f: (I, ManagedDatasource[T, F, G, R, P]) => F[ManagedDatasource[T, F, H, S, Q]])(
    implicit
    AF: Monad[F])
    : DatasourceModules[T, F, H, I, C, S, Q] =
    new DatasourceModules[T, F, H, I, C, S, Q] {
      def create(i: I, ref: DatasourceRef[C]): EitherT[Resource[F, ?], CreateError[C], ManagedDatasource[T, F, H, S, Q]] =
        self.create(i, ref) flatMap { (mds: ManagedDatasource[T, F, G, R, P]) =>
          EitherT.rightT(Resource.liftF(f(i, mds)))
        }
      def sanitizeRef(inp: DatasourceRef[C]): DatasourceRef[C] =
        self.sanitizeRef(inp)
      def supportedTypes: F[ISet[DatasourceType]] =
        self.supportedTypes
    }

  def withFinalizer(f: (I, ManagedDatasource[T, F, G, R, P]) => F[Unit])(implicit F: Monad[F]): DatasourceModules[T, F, G, I, C, R, P] =
    new DatasourceModules[T, F, G, I, C, R, P] {
      def create(i: I, ref: DatasourceRef[C]): EitherT[Resource[F, ?], CreateError[C], ManagedDatasource[T, F, G, R, P]] =
        self.create(i, ref) flatMap { (mds: ManagedDatasource[T, F, G, R, P]) =>
          EitherT.rightT(Resource.make(mds.pure[F])(x => f(i, x)))
        }
      def sanitizeRef(inp: DatasourceRef[C]): DatasourceRef[C] =
        self.sanitizeRef(inp)
      def supportedTypes: F[ISet[DatasourceType]] =
        self.supportedTypes
    }

  def widenPathType[PP >: P <: ResourcePathType](implicit AF: Monad[F]): DatasourceModules[T, F, G, I, C, R, PP] =
    new DatasourceModules[T, F, G, I, C, R, PP] {
      def create(i: I, ref: DatasourceRef[C]): EitherT[Resource[F, ?], CreateError[C], ManagedDatasource[T, F, G, R, PP]] =
        self.create(i, ref) map { ManagedDatasource.widenPathType[T, F, G, R, P, PP](_) }
      def sanitizeRef(inp: DatasourceRef[C]): DatasourceRef[C] =
        self.sanitizeRef(inp)
      def supportedTypes: F[ISet[DatasourceType]] =
        self.supportedTypes
    }

}

object DatasourceModules {
  type Modules[T[_[_]], F[_], I] = DatasourceModules[T, F, Stream[F, ?], I, Json, QueryResult[F], ResourcePathType.Physical]
  type MDS[T[_[_]], F[_]] = ManagedDatasource[T, F, Stream[F, ?], QueryResult[F], ResourcePathType.Physical]

  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: Timer: MonadResourceErr: MonadPlannerErr,
      I, A: Hash](
      modules: List[DatasourceModule],
      rateLimiting: RateLimiting[F, A],
      byteStores: ByteStores[F, I])(
      implicit
      ec: ExecutionContext)
      : Modules[T, F, I] = {

    lazy val moduleSet: ISet[DatasourceType] = ISet.fromList(modules.map(_.kind))
    lazy val moduleMap: Map[DatasourceType, DatasourceModule] = Map(modules.map(ds => (ds.kind, ds)): _*)

    new DatasourceModules[T, F, Stream[F, ?], I, Json, QueryResult[F], ResourcePathType.Physical] {
      def create(i: I, ref: DatasourceRef[Json]): EitherT[Resource[F, ?], CreateError[Json], MDS[T, F]] = moduleMap.get(ref.kind) match {
        case None =>
          EitherT.pureLeft(DatasourceUnsupported(ref.kind, moduleSet))

        case Some(module) =>
          EitherT.rightU[CreateError[Json]](Resource.liftF(byteStores.get(i))) flatMap { store =>
            module match {
              case DatasourceModule.Lightweight(lw) =>
                handleInitErrors(module.kind, lw.lightweightDatasource[F, A](ref.config, rateLimiting, store))
                  .map(ManagedDatasource.lightweight[T](_))

              case DatasourceModule.Heavyweight(hw) =>
                handleInitErrors(module.kind, hw.heavyweightDatasource[T, F](ref.config, store))
                  .map(ManagedDatasource.heavyweight(_))
            }
          }
      }

      def sanitizeRef(inp: DatasourceRef[Json]): DatasourceRef[Json] = moduleMap.get(inp.kind) match {
        case None => inp.copy(config = jEmptyObject)
        case Some(x) => inp.copy(config = x.sanitizeConfig(inp.config))
      }

      def supportedTypes: F[ISet[DatasourceType]] =
        moduleSet.pure[F]
    }
  }

  private def handleInitErrors[F[_]: Bracket[?[_], Throwable]: MonadError[?[_], Throwable], A](
      kind: DatasourceType,
      res: Resource[F, Either[InitializationError[Json], A]])
      : EitherT[Resource[F, ?], CreateError[Json], A] = EitherT {
    linkDatasource(kind, res) map {
      case Right(a) => \/-(a)
      case Left(x) => -\/(x)
    }
  }
}
