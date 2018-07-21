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

package quasar.run

import quasar.api.QueryEvaluator
import quasar.api.datasource.{DatasourceRef, Datasources}
import quasar.common.PhaseResultTell
import quasar.common.data.Data
import quasar.contrib.pathy.ADir
import quasar.contrib.std.uuid._
import quasar.evaluate.FederatingQueryEvaluator
import quasar.impl.DatasourceModule
import quasar.impl.datasource.local.LocalDatasourceModule
import quasar.impl.datasources.{DatasourceManagement, DefaultDatasources}
import quasar.impl.external.{ExternalConfig, ExternalDatasources}
import quasar.mimir.Precog
import quasar.mimir.evaluate.MimirQueryFederation
import quasar.mimir.storage.{MimirIndexedStore, StoreKey}
import quasar.run.implicits._
import quasar.run.optics._

import java.nio.file.Path
import java.util.UUID
import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.JsonScalaz._
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.flatMap._
import fs2.{Scheduler, Stream}
import matryoshka.data.Fix
import pathy.Path._
import scalaz.IMap
import scalaz.syntax.foldable._
import shims._

final class Quasar[F[_]](
    val datasources: Datasources[F, Stream[F, ?], UUID, Json],
    val queryEvaluator: QueryEvaluator[F, SqlQuery, Stream[IO, Data]])

object Quasar {
  // The location the datasource refs tables within `mimir`.
  val DatasourceRefsLocation: ADir =
    rootDir </> dir("quasar") </> dir("datasource-refs")

  /** What it says on the tin.
    *
    * @param mimirDir directory where mimir should store its data
    * @param extConfig datasource plugin configuration
    */
  def apply[F[_]: ConcurrentEffect: MonadQuasarErr: PhaseResultTell: Timer](
      mimirDir: Path,
      extConfig: ExternalConfig,
      pool: ExecutionContext)
      : Stream[F, Quasar[F]] = {

    implicit val ec = pool

    for {
      precog <- Stream.bracket(Precog(mimirDir.toFile).to[F])(
        d => Stream.emit(d.unsafeValue),
        _.dispose.to[F])

      refs =
        MimirIndexedStore.transformValue(
          MimirIndexedStore.transformIndex(
            MimirIndexedStore[F](precog, DatasourceRefsLocation),
            "UUID",
            StoreKey.stringIso composePrism stringUUIDP),
          "DatasourceRef",
          rValueDatasourceRefP(rValueJsonP))

      configured <- refs.entries.fold(IMap.empty[UUID, DatasourceRef[Json]])(_ + _)

      extMods <- ExternalDatasources[F](extConfig, pool)

      modules = extMods.insert(
        LocalDatasourceModule.kind,
        DatasourceModule.Lightweight(LocalDatasourceModule))

      scheduler <- Scheduler(corePoolSize = 1, threadPrefix = "quasar-scheduler")

      mr <- Stream.bracket(DatasourceManagement[Fix, F, UUID](modules, configured, pool, scheduler))(
        Stream.emit(_),
        { case (_, r) => r.get.flatMap(_.traverse_(_.dispose)) })

      (mgmt, running) = mr

      freshUUID = ConcurrentEffect[F].delay(UUID.randomUUID)

      datasources = DefaultDatasources(freshUUID, refs, mgmt, mgmt)

      federation = MimirQueryFederation[Fix, F](precog)

      queryEvaluator =
        Sql2QueryEvaluator(FederatingQueryEvaluator(federation, ResourceRouter(running.get)))

    } yield new Quasar(datasources, queryEvaluator)
  }
}
