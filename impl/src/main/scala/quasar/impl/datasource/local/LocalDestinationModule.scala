/*
 * Copyright 2014–2020 SlamData Inc.
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

import quasar.api.destination.Destination
import quasar.api.destination.DestinationError.{
  InitializationError,
  malformedConfiguration
}
import quasar.connector.{DestinationModule, MonadResourceErr}

import scala.util.Either

import argonaut.Json
import cats.effect.{Blocker, ContextShift, ConcurrentEffect, Resource, Timer}

trait LocalDestinationModule extends DestinationModule {
  def blocker: Blocker

  val destinationType = LocalDestinationType

  def sanitizeDestinationConfig(config: Json): Json = config

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)
      : Resource[F, Either[InitializationError[Json], Destination[F]]] = {
    val dest = for {
      ld <- attemptConfig[F, LocalDestinationConfig, InitializationError[Json]](
        config,
        "Failed to decode LocalDestination config: ")(
        (c, d) => malformedConfiguration((destinationType, c, d)))

      root <- validatedPath(ld.rootDir, "Invalid destination path: ") { d =>
        malformedConfiguration((destinationType, config, d))
      }
    } yield LocalDestination[F](root, blocker): Destination[F]

    Resource.liftF(dest.value)
  }
}
