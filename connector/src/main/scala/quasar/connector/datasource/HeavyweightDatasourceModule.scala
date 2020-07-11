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

package quasar.connector.datasource

import quasar.RenderTreeT
import quasar.api.datasource.DatasourceType
import quasar.api.datasource.DatasourceError.{ConfigurationError, InitializationError}
import quasar.api.resource.ResourcePathType
import quasar.connector.{ByteStore, QueryResult}
import quasar.qscript.{MonadPlannerErr, QScriptEducated}

import scala.concurrent.ExecutionContext
import scala.util.Either

import argonaut.Json
import cats.effect.{ConcurrentEffect, ContextShift, Timer, Resource}
import fs2.Stream
import matryoshka.{BirecursiveT, EqualT, ShowT}

trait HeavyweightDatasourceModule {
  def kind: DatasourceType

  def sanitizeConfig(config: Json): Json

  def migrateConfig(config: Json)
      : Either[ConfigurationError[Json], Json]

  def reconfigure(original: Json, patch: Json)
      : Either[ConfigurationError[Json], (Reconfiguration, Json)]

  def heavyweightDatasource[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: Timer](
      config: Json,
      byteStore: ByteStore[F])(
      implicit ec: ExecutionContext)
      : Resource[F, Either[InitializationError[Json], HeavyweightDatasourceModule.DS[T, F]]]
}

object HeavyweightDatasourceModule {
  type DS[T[_[_]], F[_]] = Datasource[Resource[F, ?], Stream[F, ?], T[QScriptEducated[T, ?]], QueryResult[F], ResourcePathType.Physical]
}
