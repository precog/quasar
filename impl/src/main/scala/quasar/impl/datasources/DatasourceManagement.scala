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

package quasar.impl.datasources

import slamdata.Predef.{Boolean, Exception, None, Option, Some, Unit}
import quasar.{Condition, Disposable, RenderTreeT}
import quasar.api.datasource.{DatasourceError, DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError.{
  CreateError,
  DatasourceUnsupported,
  DiscoveryError,
  ExistentialError
}
import quasar.common.data.Data
import quasar.common.resource.{MonadResourceErr, ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.Datasource
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.{κ, κ2}
import quasar.impl.DatasourceModule
import quasar.impl.datasource.{ByNeedDatasource, ConditionReportingDatasource, FailedDatasource}
import quasar.qscript.{MonadPlannerErr, QScriptEducated}

import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.effect.{ConcurrentEffect, Timer}
import fs2.{Scheduler, Stream}
import fs2.async.{immutable, mutable, Ref}
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{\/, EitherT, IMap, ISet, Monad, OptionT, Order, Scalaz}, Scalaz._
import shims._

final class DatasourceManagement[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: ConcurrentEffect: MonadPlannerErr: MonadResourceErr: Timer,
    I: Order] private (
    modules: DatasourceManagement.Modules,
    errors: Ref[F, IMap[I, Exception]],
    running: mutable.Signal[F, DatasourceManagement.Running[I, T, F]])
    extends DatasourceControl[F, Stream[F, ?], I, Json]
    with DatasourceErrors[F, I] {

  import DatasourceManagement.withErrorReporting

  type DS = DatasourceManagement.DS[T, F]
  type Running = DatasourceManagement.Running[I, T, F]

  // DatasourceControl

  def initDatasource(datasourceId: I, ref: DatasourceRef[Json])
      : F[Condition[CreateError[Json]]] = {

    val init0: DatasourceModule => EitherT[F, CreateError[Json], Disposable[F, DS]] = {
      case DatasourceModule.Lightweight(lw) =>
        EitherT(lw.lightweightDatasource[F](ref.config))
          .bimap(ie => ie: CreateError[Json], _.map(_.left))

      case DatasourceModule.Heavyweight(hw) =>
        EitherT(hw.heavyweightDatasource[T, F](ref.config))
          .bimap(ie => ie: CreateError[Json], _.map(_.right))
    }

    val inited = for {
      sup <- EitherT.rightU[CreateError[Json]](supportedDatasourceTypes)

      mod <-
        OptionT(modules.lookup(ref.kind).point[F])
          .toRight[CreateError[Json]](DatasourceUnsupported(ref.kind, sup))

      ds0 <- init0(mod)

      ds = ds0.map(_.bimap(
        withErrorReporting(errors, datasourceId, _),
        withErrorReporting(errors, datasourceId, _)))

      _ <- EitherT.rightT(modifyAndShutdown { r =>
        (r.insert(datasourceId, ds), r.lookupAssoc(datasourceId))
      })
    } yield ()

    inited.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  def pathIsResource(datasourceId: I, path: ResourcePath)
      : F[ExistentialError[I] \/ Boolean] =
    withDatasource[ExistentialError[I], Boolean](datasourceId)(_.pathIsResource(path))

  def prefixedChildPaths(datasourceId: I, prefixPath: ResourcePath)
      : F[DiscoveryError[I] \/ Stream[F, (ResourceName, ResourcePathType)]] =
    withDatasource[DiscoveryError[I], Option[Stream[F, (ResourceName, ResourcePathType)]]](
      datasourceId)(
      _.prefixedChildPaths(prefixPath))
      .map(_.flatMap(_ \/> DatasourceError.pathNotFound[DiscoveryError[I]](prefixPath)))

  def shutdownDatasource(datasourceId: I): F[Unit] =
    modifyAndShutdown(_.updateLookupWithKey(datasourceId, κ2(None)) match {
      case (v, m) => (m, v strengthL datasourceId)
    })

  val supportedDatasourceTypes: F[ISet[DatasourceType]] =
    modules.keySet.point[F]


  // DatasourceErrors

  def erroredDatasources: F[IMap[I, Exception]] =
    errors.get

  def datasourceError(datasourceId: I): F[Option[Exception]] =
    errors.get.map(_.lookup(datasourceId))


  ////

  private def modifyAndShutdown(f: Running => (Running, Option[(I, Disposable[F, DS])])): F[Unit] =
    for {
      t <- running.modify2(f)

      _  <- t._2.traverse_ {
        case (id, ds) => errors.modify(_ - id) *> ds.dispose
      }
    } yield ()

  private def withDatasource[E >: ExistentialError[I] <: DatasourceError[I, Json], A](
      datasourceId: I)(
      f: Datasource[F, Stream[F, ?], _, _] => F[A])
      : F[E \/ A] =
    running.get.flatMap(_.lookup(datasourceId) match {
      case Some(ds) =>
        f(ds.unsafeValue.merge).map(_.right[E])

      case None =>
        DatasourceError.datasourceNotFound[I, E](datasourceId)
          .left[A].point[F]
    })
}

object DatasourceManagement {
  type Modules = IMap[DatasourceType, DatasourceModule]
  type LDS[F[_]] = Datasource[F, Stream[F, ?], ResourcePath, Stream[F, Data]]
  type HDS[T[_[_]], F[_]] = Datasource[F, Stream[F, ?], T[QScriptEducated[T, ?]], Stream[F, Data]]
  type DS[T[_[_]], F[_]] = LDS[F] \/ HDS[T, F]
  type Running[I, T[_[_]], F[_]] = IMap[I, Disposable[F, DS[T, F]]]

  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: MonadPlannerErr: MonadResourceErr: MonadError_[?[_], CreateError[Json]]: Timer,
      I: Order](
      modules: Modules,
      configured: IMap[I, DatasourceRef[Json]],
      pool: ExecutionContext,
      scheduler: Scheduler)
      : F[(DatasourceControl[F, Stream[F, ?], I, Json] with DatasourceErrors[F, I], immutable.Signal[F, Running[I, T, F]])] = {

    implicit val ec: ExecutionContext = pool

    for {
      errors <- Ref[F, IMap[I, Exception]](IMap.empty)

      assocs <- configured.toList traverse {
        case (id, ref @ DatasourceRef(kind, _, _)) =>
          modules.lookup(kind) match {
            case None =>
              val ds = FailedDatasource[CreateError[Json], F, Stream[F, ?], ResourcePath, Stream[F, Data]](
                kind,
                DatasourceUnsupported(kind, modules.keySet))

              (id, ds.left[HDS[T, F]].point[Disposable[F, ?]]).point[F]

            case Some(mod) =>
              lazyDatasource[T, F](mod, ref, pool, scheduler).strengthL(id)
          }
      }

      running = IMap.fromList(assocs) mapWithKey { (id, ds) =>
        ds.map(_.bimap(
          withErrorReporting(errors, id, _),
          withErrorReporting(errors, id, _)))
      }

      runningS <- mutable.Signal[F, Running[I, T, F]](running)

      mgmt = new DatasourceManagement[T, F, I](modules, errors, runningS)
    } yield (mgmt, runningS)
  }

  def withErrorReporting[F[_]: Monad: MonadError_[?[_], Exception], I: Order, G[_], Q, R](
      errors: Ref[F, IMap[I, Exception]],
      datasourceId: I,
      ds: Datasource[F, G, Q, R])
      : Datasource[F, G, Q, R] =
    ConditionReportingDatasource[Exception, F, G, Q, R](
      c => errors.modify(_.alter(datasourceId, κ(Condition.optionIso.get(c)))).void, ds)

  ////

  private def lazyDatasource[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: MonadPlannerErr: MonadResourceErr: MonadError_[?[_], CreateError[Json]]: Timer](
      module: DatasourceModule,
      ref: DatasourceRef[Json],
      pool: ExecutionContext,
      scheduler: Scheduler)
      : F[Disposable[F, DS[T, F]]] =
    module match {
      case DatasourceModule.Lightweight(lw) =>
        val mklw = MonadError_[F, CreateError[Json]] unattempt {
          lw.lightweightDatasource[F](ref.config)
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDatasource(ref.kind, mklw, pool, scheduler)
          .map(_.map(_.left))

      case DatasourceModule.Heavyweight(hw) =>
        val mkhw = MonadError_[F, CreateError[Json]] unattempt {
          hw.heavyweightDatasource[T, F](ref.config)
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDatasource(ref.kind, mkhw, pool, scheduler)
          .map(_.map(_.right))
    }
}
