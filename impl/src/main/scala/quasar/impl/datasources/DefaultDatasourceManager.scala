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

import quasar.{Condition, RenderTreeT}
import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError.{CreateError, DatasourceUnsupported, InitializationError}
import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.connector.{MonadResourceErr, QueryResult}
import quasar.contrib.scalaz._
import quasar.fp.ski.κ2
import quasar.impl.DatasourceModule
import quasar.impl.IncompatibleModuleException.linkDatasource
import quasar.impl.datasource.{ByNeedDatasource, FailedDatasource, MonadCreateErr}
import quasar.qscript.{InterpretedRead, MonadPlannerErr}

import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.Argonaut.jEmptyObject

import cats.{ApplicativeError, MonadError}
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.syntax.applicativeError._
import fs2.Stream
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{EitherT, IMap, ISet, OptionT, Order, Scalaz}
import Scalaz._
import shims._

final class DefaultDatasourceManager[
    I: Order,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: ConcurrentEffect: ContextShift: MonadCreateErr: MonadPlannerErr: MonadResourceErr: Timer,
    G[_],
    R,
    P <: ResourcePathType] private (
    modules: DefaultDatasourceManager.Modules,
    running: Ref[F, DefaultDatasourceManager.Running[I, T, F, G, R, P]],
    onCreate: (I, DefaultDatasourceManager.MDS[T, F, ResourcePathType.Physical]) => F[ManagedDatasource[T, F, G, R, P]])(
    implicit
    ec: ExecutionContext)
    extends DatasourceManager[I, Json, T, F, G, R, P] { self =>

  import DefaultDatasourceManager._

  type MDS = DefaultDatasourceManager.MDS[T, F, ResourcePathType.Physical]
  type Running = DefaultDatasourceManager.Running[I, T, F, G, R, P]

  def initDatasource(datasourceId: I, ref: DatasourceRef[Json])
      : F[Condition[CreateError[Json]]] = {

    def createDatasource(module: DatasourceModule): Resource[F, MDS] =
      module match {
        case DatasourceModule.Lightweight(lw) =>
          handleInitErrors(module.kind, lw.lightweightDatasource[F](ref.config))
            .map(ManagedDatasource.lightweight[T](_))

        case DatasourceModule.Heavyweight(hw) =>
          handleInitErrors(module.kind, hw.heavyweightDatasource[T, F](ref.config))
            .map(ManagedDatasource.heavyweight(_))
      }

    val inited = for {
      sup <- EitherT.rightU[CreateError[Json]](supportedDatasourceTypes)

      mod <-
        OptionT(modules.lookup(ref.kind).point[F])
          .toRight[CreateError[Json]](DatasourceUnsupported(ref.kind, sup))

      (mds, dispose) <-
        EitherT(MonadError_[F, CreateError[Json]].attempt(createDatasource(mod).allocated))

      ds <- EitherT.rightT(onCreate(datasourceId, mds) onError {
        case _ => dispose
      })

      _ <- EitherT.rightT(modifyAndShutdown { r =>
        (r.insert(datasourceId, (ds, dispose)), r.lookup(datasourceId))
      })
    } yield ()

    inited.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  def managedDatasource(datasourceId: I): F[Option[ManagedDatasource[T, F, G, R, P]]] =
    running.get.map(_.lookup(datasourceId).map(_._1))

  def sanitizedRef(ref: DatasourceRef[Json]): DatasourceRef[Json] =
    modules.lookup(ref.kind)
      .fold(configL.set(jEmptyObject))(m => configL.modify(m.sanitizeConfig))
      .apply(ref)

  def shutdownDatasource(datasourceId: I): F[Unit] =
    modifyAndShutdown(_.updateLookupWithKey(datasourceId, κ2(None)).swap)

  val supportedDatasourceTypes: F[ISet[DatasourceType]] =
    modules.keySet.point[F]

  ////

  private val configL = DatasourceRef.config[Json]

  private def modifyAndShutdown(
      f: Running => (Running, Option[(ManagedDatasource[T, F, G, R, P], F[Unit])]))
      : F[Unit] =
    running.modify(f).flatMap(_.traverse_(_._2))
}

object DefaultDatasourceManager {
  type Modules = IMap[DatasourceType, DatasourceModule]
  type MDS[T[_[_]], F[_], P <: ResourcePathType] = ManagedDatasource[T, F, Stream[F, ?], QueryResult[F], P]
  type Running[I, T[_[_]], F[_], G[_], R, P <: ResourcePathType] =
    IMap[I, (ManagedDatasource[T, F, G, R, P], F[Unit])]

  final class Builder[
      I: Order,
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadCreateErr: Timer,
      G[_],
      R,
      P <: ResourcePathType] private (
      onCreate: (I, MDS[T, F, ResourcePathType.Physical]) => F[ManagedDatasource[T, F, G, R, P]]) {

    def build(modules: Modules, configured: IMap[I, DatasourceRef[Json]])(
        implicit
        ec: ExecutionContext)
        : Resource[F, DatasourceManager[I, Json, T, F, G, R, P]] = {

      val bases =
        configured map { ref =>
          modules.lookup(ref.kind) match {
            case Some(mod) =>
              lazyDatasource[T, F](mod, ref)

            case None =>
              val ds = FailedDatasource[CreateError[Json], F, Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F], ResourcePathType.Physical](
                ref.kind,
                DatasourceUnsupported(ref.kind, modules.keySet))

              ManagedDatasource.lightweight[T](ds).point[Resource[F, ?]]
          }
        }

      val init = for {
        mapped <- bases.toList traverse {
          case (id, res) =>
            res.allocated flatMap {
              case (mds, dispose) =>
                onCreate(id, mds)
                  .map(ds => (id, (ds, dispose)))
                  .onError { case _ => dispose }
            }
        }

        running <- Ref[F].of(IMap.fromList(mapped))

        runningKeys = running.get.map(_.keySet)

      } yield (new DefaultDatasourceManager(modules, running, onCreate), runningKeys)

      val resource = Resource.make(init) {
        case (mgr, keys) => keys flatMap { ks =>
          ks.foldMapLeft1Opt(mgr.shutdownDatasource)((u, k) => u.guarantee(mgr.shutdownDatasource(k)))
            .sequence_
        }
      }

      resource.map(_._1)
    }

    def withMiddleware[H[_], S, Q <: ResourcePathType](
        f: (I, ManagedDatasource[T, F, G, R, P]) => F[ManagedDatasource[T, F, H, S, Q]])
        : Builder[I, T, F, H, S, Q] =
      new Builder[I, T, F, H, S, Q]((i, mds) => onCreate(i, mds).flatMap(f(i, _)))
  }

  object Builder {
    def apply[
        I: Order,
        T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
        F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadCreateErr: Timer]
        : Builder[I, T, F, Stream[F, ?], QueryResult[F], ResourcePathType.Physical] =
      new Builder[I, T, F, Stream[F, ?], QueryResult[F], ResourcePathType.Physical](
        (_, mds) => mds.point[F])
  }

  ////

  private def lazyDatasource[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadCreateErr: Timer](
      module: DatasourceModule,
      ref: DatasourceRef[Json])(
      implicit ec: ExecutionContext)
      : Resource[F, MDS[T, F, ResourcePathType.Physical]] =
    module match {
      case DatasourceModule.Lightweight(lw) =>
        val mklw =
          handleInitErrors(ref.kind, lw.lightweightDatasource[F](ref.config))

        ByNeedDatasource(ref.kind, mklw).map(ManagedDatasource.lightweight[T](_))

      case DatasourceModule.Heavyweight(hw) =>
        val mkhw =
          handleInitErrors(ref.kind, hw.heavyweightDatasource[T, F](ref.config))

        ByNeedDatasource(ref.kind, mkhw).map(ManagedDatasource.heavyweight(_))
    }

  private def handleLinkageError[F[_]: ApplicativeError[?[_], Throwable], A](kind: DatasourceType, fa: => F[A]): F[A] =
    linkDatasource(kind, fa)

  private def handleInitErrors[F[_]: MonadCreateErr: MonadError[?[_], Throwable], A](
      kind: DatasourceType,
      res: => Resource[F, Either[InitializationError[Json], A]])
      : Resource[F, A] = {

    import quasar.contrib.cats.monadError.monadError_CatsMonadError

    implicit val merr: MonadError[F, CreateError[Json]] =
      monadError_CatsMonadError[F, CreateError[Json]](
        MonadError[F, Throwable], MonadError_[F, CreateError[Json]])

    val rmerr = MonadError[Resource[F, ?], CreateError[Json]]

    rmerr.rethrow(rmerr.map(handleLinkageError(kind, res))(_.leftMap(ie => ie: CreateError[Json])))
  }
}
