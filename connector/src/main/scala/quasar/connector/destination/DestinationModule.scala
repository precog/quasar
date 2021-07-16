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

package quasar.connector.destination

import quasar.api.destination.DestinationType
import quasar.api.destination.DestinationError.InitializationError
import quasar.connector.{MonadResourceErr, GetAuth}

import scala.util.Either

import argonaut.Json

import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}

trait DestinationModule {
  def destinationType: DestinationType

  def sanitizeDestinationConfig(config: Json): Json

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      pushPull: PushmiPullyu[F],
      auth: GetAuth[F])
      : Resource[F, Either[InitializationError[Json], Destination[F]]]
}
