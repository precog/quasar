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

import slamdata.Predef.{List, None, Option, Some, Unit}

import quasar.Condition
import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError._
import quasar.api.resource.ResourcePathType
import quasar.contrib.scalaz.{MonadState_, MonadTell_}
import quasar.fp.ski.κ
import quasar.impl.datasource.EmptyDatasource

import MockDatasourceManager.{MonadInit, MonadShutdown}

import scalaz.{ISet, Monad, Order, PlusEmpty}
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

final class MockDatasourceManager[I: Order, C, T[_[_]], F[_]: Monad, G[_]: PlusEmpty, R] private (
    supportedTypes: ISet[DatasourceType],
    initErrors: C => Option[InitializationError[C]],
    sanitize: C => C,
    emptyResult: R)(
    implicit
    initd: MonadInit[F, I],
    sdown: MonadShutdown[F, I])
    extends DatasourceManager[I, C, T, F, G, R, ResourcePathType] {

  def initDatasource(datasourceId: I, ref: DatasourceRef[C])
      : F[Condition[CreateError[C]]] =
    if (supportedTypes.member(ref.kind))
      initErrors(ref.config) match {
        case Some(err) =>
          Condition.abnormal[CreateError[C]](err).point[F]

        case None =>
          for {
            inits <- initd.get
            _ <- inits.member(datasourceId).whenM(shutdownDatasource(datasourceId))
            _ <- initd.put(inits.insert(datasourceId))
          } yield Condition.normal[CreateError[C]]()
      }
    else
      Condition.abnormal[CreateError[C]](
        DatasourceUnsupported(ref.kind, supportedTypes))
        .point[F]

  def managedDatasource(datasourceId: I): F[Option[ManagedDatasource[T, F, G, R, ResourcePathType]]] =
    initd.gets(_.member(datasourceId)) map { exists =>
      supportedTypes.findMin.filter(κ(exists)) map { kind =>
        ManagedDatasource.lightweight[T][F, G, R, ResourcePathType](EmptyDatasource(kind, emptyResult))
      }
    }

  def sanitizedRef(ref: DatasourceRef[C]): DatasourceRef[C] =
    ref.copy(config = sanitize(ref.config))

  def shutdownDatasource(datasourceId: I): F[Unit] =
    sdown.tell(List(datasourceId)) >> initd.modify(_.delete(datasourceId))

  def supportedDatasourceTypes: F[ISet[DatasourceType]] =
    supportedTypes.point[F]
}

object MockDatasourceManager {
  type Initialized[I] = ISet[I]
  type MonadInit[F[_], I] = MonadState_[F, Initialized[I]]

  type Shutdowns[I] = List[I]
  type MonadShutdown[F[_], I] = MonadTell_[F, Shutdowns[I]]

  def apply[I: Order, C, T[_[_]], F[_]: Monad, G[_]: PlusEmpty, R](
      supportedTypes: ISet[DatasourceType],
      initErrors: C => Option[InitializationError[C]],
      sanitize: C => C,
      emptyResult: R)(
      implicit
      MI: MonadInit[F, I],
      MS: MonadShutdown[F, I])
      : DatasourceManager[I, C, T, F, G, R, ResourcePathType] =
    new MockDatasourceManager[I, C, T, F, G, R](supportedTypes, initErrors, sanitize, emptyResult)
}
