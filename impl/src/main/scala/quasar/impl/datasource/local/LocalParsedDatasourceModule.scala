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

package quasar.impl.datasource.local

import quasar.RateLimiter
import quasar.api.datasource.DatasourceType
import quasar.api.datasource.DatasourceError.{
  InitializationError,
  malformedConfiguration
}
import quasar.common.data.RValue
import quasar.concurrent.BlockingContext
import quasar.connector._, LightweightDatasourceModule.DS

import scala.concurrent.ExecutionContext
import scala.util.Either

import argonaut.Json
import cats.effect._

object LocalParsedDatasourceModule extends LightweightDatasourceModule with LocalDestinationModule {
  // FIXME this is side effecting
  override lazy val blockingPool: BlockingContext =
    BlockingContext.cached("local-datasource")

  val kind: DatasourceType = LocalParsedType

  def sanitizeConfig(config: Json): Json = config

  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      rateLimiter: RateLimiter[F])(
      implicit ec: ExecutionContext)
      : Resource[F, Either[InitializationError[Json], DS[F]]] = {
    val ds = for {
      lc <- attemptConfig[F, LocalConfig, InitializationError[Json]](
        config,
        "Failed to decode LocalDatasource config: ")(
        (c, d) => malformedConfiguration((LocalParsedType, c, d)))

      root <- validatedPath(lc.rootDir, "Invalid path: ") { d =>
        malformedConfiguration((LocalParsedType, config, d))
      }
    } yield {
      LocalParsedDatasource[F, RValue](
        root,
        lc.readChunkSizeBytes,
        lc.format,
        blockingPool)
    }

    Resource.liftF(ds.value)
  }
}
