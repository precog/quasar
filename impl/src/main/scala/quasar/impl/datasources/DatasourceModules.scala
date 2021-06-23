/*
 * Copyright 2020 Precog Data
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
import quasar.impl.{DatasourceModule, QuasarDatasource}
import quasar.impl.IncompatibleModuleException.linkDatasource
import quasar.connector.{GetAuth, MonadResourceErr, QueryResult}
import quasar.connector.datasource.Reconfiguration
import quasar.qscript.MonadPlannerErr

import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.Argonaut.jEmptyObject

import cats.{~>, Monad, MonadError}
import cats.data.EitherT
import cats.effect.{Resource, ConcurrentEffect, ContextShift, Timer, Bracket}
import cats.implicits._
import cats.kernel.Hash

import fs2.Stream

import matryoshka.{BirecursiveT, EqualT, ShowT}

import scalaz.ISet


trait DatasourceModules[T[_[_]], F[_], G[_], H[_], I, C, R, P <: ResourcePathType] { self =>
  def create(i: I, ref: DatasourceRef[C])
      : EitherT[Resource[F, ?], CreateError[C], QuasarDatasource[T, G, H, R, P]]

  def sanitizeRef(inp: DatasourceRef[C]): F[DatasourceRef[C]]

  def supportedTypes: F[ISet[DatasourceType]]

  def reconfigureRef(original: DatasourceRef[C], patch: C)
      : EitherT[F, CreateError[C], (Reconfiguration, DatasourceRef[C])]

  def withMiddleware[HH[_], S, Q <: ResourcePathType](
      f: (I, QuasarDatasource[T, G, H, R, P]) => F[QuasarDatasource[T, G, HH, S, Q]])(
      implicit
      AF: Monad[F])
      : DatasourceModules[T, F, G, HH, I, C, S, Q] =
    new DatasourceModules[T, F, G, HH, I, C, S, Q] {
      def create(i: I, ref: DatasourceRef[C])
          : EitherT[Resource[F, ?], CreateError[C], QuasarDatasource[T, G, HH, S, Q]] =
        self.create(i, ref) flatMap { (mds: QuasarDatasource[T, G, H, R, P]) =>
          EitherT.right(Resource.eval(f(i, mds)))
        }

      def sanitizeRef(inp: DatasourceRef[C]): F[DatasourceRef[C]] =
        self.sanitizeRef(inp)

      def supportedTypes: F[ISet[DatasourceType]] =
        self.supportedTypes

      def reconfigureRef(original: DatasourceRef[C], patch: C)
          : EitherT[F, CreateError[C], (Reconfiguration, DatasourceRef[C])] =
        self.reconfigureRef(original, patch)
    }

  def withFinalizer(
      f: (I, QuasarDatasource[T, G, H, R, P]) => F[Unit])(
      implicit F: Monad[F])
      : DatasourceModules[T, F, G, H, I, C, R, P] =
    new DatasourceModules[T, F, G, H, I, C, R, P] {
      def create(i: I, ref: DatasourceRef[C])
          : EitherT[Resource[F, ?], CreateError[C], QuasarDatasource[T, G, H, R, P]] =
        self.create(i, ref) flatMap { (mds: QuasarDatasource[T, G, H, R, P]) =>
          EitherT.right(Resource.make(mds.pure[F])(x => f(i, x)))
        }

      def sanitizeRef(inp: DatasourceRef[C]): F[DatasourceRef[C]] =
        self.sanitizeRef(inp)

      def supportedTypes: F[ISet[DatasourceType]] =
        self.supportedTypes

      def reconfigureRef(original: DatasourceRef[C], patch: C)
          : EitherT[F, CreateError[C], (Reconfiguration, DatasourceRef[C])] =
        self.reconfigureRef(original, patch)
    }

  def widenPathType[PP >: P <: ResourcePathType](implicit AF: Monad[F])
      : DatasourceModules[T, F, G, H, I, C, R, PP] =
    new DatasourceModules[T, F, G, H, I, C, R, PP] {
      def create(i: I, ref: DatasourceRef[C])
          : EitherT[Resource[F, ?], CreateError[C], QuasarDatasource[T, G, H, R, PP]] =
        self.create(i, ref) map { QuasarDatasource.widenPathType[T, G, H, R, P, PP](_) }

      def sanitizeRef(inp: DatasourceRef[C]): F[DatasourceRef[C]] =
        self.sanitizeRef(inp)

      def supportedTypes: F[ISet[DatasourceType]] =
        self.supportedTypes

      def reconfigureRef(original: DatasourceRef[C], patch: C)
          : EitherT[F, CreateError[C], (Reconfiguration, DatasourceRef[C])] =
        self.reconfigureRef(original, patch)
    }
}

object DatasourceModules {
  type Modules[T[_[_]], F[_], I] =
    DatasourceModules[T, F, Resource[F, ?], Stream[F, ?], I, Json, QueryResult[F], ResourcePathType.Physical]

  type MDS[T[_[_]], F[_]] =
    QuasarDatasource[T, Resource[F, ?], Stream[F, ?], QueryResult[F], ResourcePathType.Physical]

  private[impl] def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: Timer: MonadResourceErr: MonadPlannerErr,
      I, A: Hash](
      modules: List[DatasourceModule],
      rateLimiting: RateLimiting[F, A],
      byteStores: ByteStores[F, I],
      getAuth: GetAuth[F])(
      implicit
      ec: ExecutionContext)
      : Modules[T, F, I] = {

    lazy val moduleSet: ISet[DatasourceType] =
      ISet.fromList(modules.map(_.kind))

    def findModule(ref: DatasourceRef[Json]): Option[DatasourceModule] =
      modules.find { (m: DatasourceModule) =>
        m.kind.name === ref.kind.name && m.kind.version >= ref.kind.version && ref.kind.version >= m.minVersion
      }

    def findAndMigrate(ref: DatasourceRef[Json])
        : EitherT[F, CreateError[Json], (DatasourceModule, DatasourceRef[Json])] =
      findModule(ref) match {
        case Some(m) if m.kind.version === ref.kind.version =>
          EitherT.pure((m, ref))
        case Some(m) if m.kind.version > ref.kind.version =>
          EitherT(m.migrateConfig(ref.kind.version, m.kind.version, ref.config))
            .leftMap((e: ConfigurationError[Json]) => e: CreateError[Json])
            .map((newConf => (m, ref.copy(config = newConf, kind = m.kind))))
        case _ =>
          EitherT.leftT(DatasourceUnsupported(ref.kind, moduleSet))
      }

    new DatasourceModules[T, F, Resource[F, ?], Stream[F, ?], I, Json, QueryResult[F], ResourcePathType.Physical] {
      def create(i: I, inp: DatasourceRef[Json])
          : EitherT[Resource[F, ?], CreateError[Json], MDS[T, F]] =
        for {
          (module, ref) <- findAndMigrate(inp).mapK(λ[F ~> Resource[F, ?]](Resource.eval(_)))
          store <- EitherT.right[CreateError[Json]](Resource.eval(byteStores.get(i)))
          res <- module match {
            case DatasourceModule.Lightweight(lw) =>
              handleInitErrors(module.kind, lw.lightweightDatasource[F, A](ref.config, rateLimiting, store, getAuth))
                .map(QuasarDatasource.lightweight[T](_))
            case DatasourceModule.Heavyweight(hw) =>
              handleInitErrors(module.kind, hw.heavyweightDatasource[T, F](ref.config, store))
                .map(QuasarDatasource.heavyweight(_))
          }
        } yield res

      def sanitizeRef(inp: DatasourceRef[Json]): F[DatasourceRef[Json]] =
        findAndMigrate(inp).value map {
          case Left(_) => inp.copy(config = jEmptyObject)
          case Right((module, ref)) => ref.copy(config = module.sanitizeConfig(ref.config))
        }

      def supportedTypes: F[ISet[DatasourceType]] =
        moduleSet.pure[F]

      def reconfigureRef(original: DatasourceRef[Json], patch: Json)
          : EitherT[F, CreateError[Json], (Reconfiguration, DatasourceRef[Json])] =
        findAndMigrate(original).flatMap { case (module, ref) =>
          EitherT.fromEither(module.reconfigure(ref.config, patch).map(_.map(c => ref.copy(config = c))))
        }
    }
  }

  private def handleInitErrors[F[_]: Bracket[?[_], Throwable]: MonadError[?[_], Throwable], A](

      kind: DatasourceType,
      res: => Resource[F, Either[InitializationError[Json], A]])
      : EitherT[Resource[F, ?], CreateError[Json], A] =
    EitherT(linkDatasource(kind, res))
}
