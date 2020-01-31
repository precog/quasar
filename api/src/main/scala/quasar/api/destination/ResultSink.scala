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

package quasar.api.destination

import slamdata.Predef._

import quasar.api.push.RenderConfig
import quasar.api.resource.ResourcePath

import cats.data.NonEmptyList

import fs2.Stream

sealed trait ResultSink[F[_], T]

object ResultSink {
  final case class Csv[F[_], T](
      config: RenderConfig.Csv,
      run: (ResourcePath, NonEmptyList[DestinationColumn[T]], Stream[F, Byte]) => Stream[F, Unit])
      extends ResultSink[F, T]

  def csv[F[_], T](
      config: RenderConfig.Csv)(
      run: (ResourcePath, NonEmptyList[DestinationColumn[T]], Stream[F, Byte]) => Stream[F, Unit])
      : ResultSink[F, T] =
    Csv[F, T](config, run)
}
