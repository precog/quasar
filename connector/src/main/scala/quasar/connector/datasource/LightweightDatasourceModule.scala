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

import quasar.RateLimiting
import quasar.api.auth.Credentials
import quasar.api.datasource.DatasourceType
import quasar.api.datasource.DatasourceError.{ConfigurationError, InitializationError}
import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.connector.{ByteStore, MonadResourceErr, QueryResult}
import quasar.qscript.InterpretedRead

import scala.Option
import scala.concurrent.ExecutionContext
import scala.util.Either

import argonaut.Json
import cats.effect.{ConcurrentEffect, ContextShift, Timer, Resource, Sync}
import cats.kernel.Hash
import fs2.Stream

trait LightweightDatasourceModule {
  def kind: DatasourceType

  def sanitizeConfig(config: Json): Json

  def migrateConfig[F[_]: Sync](config: Json)
      : F[Either[ConfigurationError[Json], Json]]

  def reconfigure(original: Json, patch: Json)
      : Either[ConfigurationError[Json], (Reconfiguration, Json)]

  def lightweightDatasource[
      F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer,
      A: Hash](
      config: Json,
      rateLimiting: RateLimiting[F, A],
      byteStore: ByteStore[F],
      auth: Credentials[F])(
      implicit ec: ExecutionContext)
      : Resource[F, Either[InitializationError[Json], LightweightDatasourceModule.DS[F]]]
}

object LightweightDatasourceModule {
  type DSP[F[_], P <: ResourcePathType] = Datasource[Resource[F, ?], Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F], P]
  type DS[F[_]] = DSP[F, ResourcePathType.Physical]
}
