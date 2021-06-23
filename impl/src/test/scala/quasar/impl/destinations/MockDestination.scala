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

import fs2.Stream

import quasar.api.destination.DestinationType
import quasar.api.destination.DestinationError.InitializationError
import quasar.connector.{MonadResourceErr, GetAuth}
import quasar.connector.destination._
import quasar.connector.render.RenderConfig

final class MockDestinationModule private (initErrs: Map[Json, InitializationError[Json]])
    extends DestinationModule {

  val destinationType = MockDestinationModule.MockType

  def sanitizeDestinationConfig(config: Json) =
    Json.jString("sanitized")

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      pushPull: PushmiPullyu[F],
      auth: GetAuth[F])
      : Resource[F, Either[InitializationError[Json], Destination[F]]] =
    Resource.pure[F, Either[InitializationError[Json], Destination[F]]](
      initErrs.get(config) match {
        case Some(e) =>
          Left(e)

        case None =>
          Right(new MockDestination[F]: Destination[F])
      })
}

object MockDestinationModule {
  val MockType: DestinationType = DestinationType("mock", 1)

  def apply(m: Map[Json, InitializationError[Json]]): DestinationModule =
    new MockDestinationModule(m)
}

class MockDestination[F[_]: Applicative] extends UntypedDestination[F] {

  def destinationType: DestinationType =
    MockDestinationModule.MockType

  def sinks = NonEmptyList.one(mockCsvSink)

  val mockCsvSink = ResultSink.create[F, Unit, Byte] { (_, _) =>
    (RenderConfig.Csv(), _ => Stream(()))
  }
}
