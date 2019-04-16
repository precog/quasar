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

package quasar.connector

import quasar.Disposable
import quasar.api.datasource.DatasourceError.InitializationError

import argonaut.Json
import cats.effect.{Effect, ContextShift}
import fs2.Stream
import scalaz.\/

trait DestinationModule {
  type Dest[F[_]] = Destination[F, Stream[F, ?], ResultSet[F]]

  def destinationKind: DestinationType

  def sanitizeDestinationConfig(config: Json): Json

  def destination[F[_]: Effect: ContextShift: MonadResourceErr](
      config: Json)
      : F[InitializationError[Json] \/ Disposable[F, Dest[F]]]
}