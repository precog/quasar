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

package quasar.impl.destinations

import slamdata.Predef._

import argonaut.Json

import cats.Applicative
import cats.data.NonEmptyList
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.implicits._

import eu.timepit.refined.auto._

import fs2.Stream

import quasar.api.destination.DestinationType
import quasar.api.destination.DestinationError.InitializationError
import quasar.connector.MonadResourceErr
import quasar.connector.destination._
import quasar.connector.render.RenderConfig

object MockDestinationModule extends DestinationModule {
  def destinationType = DestinationType("mock", 1L)
  def sanitizeDestinationConfig(config: Json) =
    Json.jString("sanitized")

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](config: Json)
      : Resource[F, Either[InitializationError[Json], Destination[F]]] =
    (new MockDestination[F]: Destination[F])
      .asRight[InitializationError[Json]]
      .pure[Resource[F, ?]]
}

class MockDestination[F[_]: Applicative] extends UntypedDestination[F] {

  def destinationType: DestinationType =
    MockDestinationModule.destinationType

  def sinks = NonEmptyList.one(mockCsvSink)

  val mockCsvSink = ResultSink.create[F, Unit](RenderConfig.Csv())(_ => Stream(()))
}
