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

package quasar.impl.local

import slamdata.Predef._

import quasar.RateLimiting
import quasar.api.auth.Credentials
import quasar.api.datasource.DatasourceType
import quasar.api.datasource.DatasourceError.{
  ConfigurationError,
  InitializationError,
  MalformedConfiguration,
  malformedConfiguration
}
import quasar.common.data.RValue
import quasar.{concurrent => qc}
import quasar.connector._
import quasar.connector.datasource.{LightweightDatasourceModule, Reconfiguration}

import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.Argonaut._

import cats.effect._
import cats.kernel.Hash
import cats.implicits._

object LocalParsedDatasourceModule extends LightweightDatasourceModule with LocalDestinationModule {
  // FIXME this is side effecting
  override lazy val blocker: Blocker =
    qc.Blocker.cached("local-datasource")

  val kind: DatasourceType = LocalParsedType

  def sanitizeConfig(config: Json): Json = config

  def migrateConfig[F[_]: Sync](config: Json)
      : F[Either[ConfigurationError[Json], Json]] = {
    val back = config.as[LocalConfig].toOption match {
      case None => Left(MalformedConfiguration(kind, config, s"Failed to migrate config: $config"): ConfigurationError[Json])
      case Some(c) => Right(c.asJson)
    }
    back.pure[F]
  }

  // there are no sensitive components, so we use the entire patch
  def reconfigure(original: Json, patch: Json)
      : Either[ConfigurationError[Json], (Reconfiguration, Json)] =
    Right((Reconfiguration.Preserve, patch))

  def lightweightDatasource[
      F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer,
      A: Hash](
      config: Json,
      rateLimiting: RateLimiting[F, A],
      stateStore: ByteStore[F],
      auth: Credentials[F])(
      implicit ec: ExecutionContext)
      : Resource[F, Either[InitializationError[Json], LightweightDatasourceModule.DS[F]]] = {
    val ds = for {
      lc <- attemptConfig[F, LocalConfig, InitializationError[Json]](
        config,
        "Failed to decode LocalDatasource config: ")(
        (c, d) => malformedConfiguration((kind, c, d)))

      root <- validatedPath(lc.rootDir, "Invalid path: ") { d =>
        malformedConfiguration((kind, config, d))
      }
    } yield {
      LocalParsedDatasource[F, RValue](
        root,
        lc.readChunkSizeBytes,
        lc.format,
        blocker)
    }

    Resource.liftF(ds.value)
  }
}
