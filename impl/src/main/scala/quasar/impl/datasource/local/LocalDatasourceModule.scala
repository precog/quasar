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

package quasar.impl.datasource.local

import quasar.Disposable
import quasar.api.datasource.{DatasourceError, DatasourceType}, DatasourceError._
import quasar.common.data.Data
import quasar.common.resource.{MonadResourceErr, ResourcePath}
import quasar.connector.{Datasource, LightweightDatasourceModule}

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import cats.effect.{ConcurrentEffect, Timer}
import fs2.Stream
import scalaz.{\/, EitherT}
import scalaz.syntax.applicative._
import shims._

object LocalDatasourceModule extends LightweightDatasourceModule {

  val kind: DatasourceType = LocalType

  def lightweightDatasource[F[_]: ConcurrentEffect: MonadResourceErr: Timer](config: Json)
      : F[InitializationError[Json] \/ Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, Stream[F, Data]]]] = {

    val F = ConcurrentEffect[F]

    val ds = for {
      lc <-
        EitherT.fromEither(config.as[LocalConfig].toEither.point[F])
          .leftMap[InitializationError[Json]] {
            case (s, _) => MalformedConfiguration(kind, config, "Failed to decode LocalDatasource config: " + s)
          }

      root <-
        EitherT.fromEither(F.attempt(F.delay(Paths.get(lc.rootDir))))
          .leftMap[InitializationError[Json]](
            t => MalformedConfiguration(kind, config, "Invalid path: " + t.getMessage))
    } yield LocalDatasource[F](root, lc.readChunkSizeBytes, global).point[Disposable[F, ?]]

    ds.run
  }
}
