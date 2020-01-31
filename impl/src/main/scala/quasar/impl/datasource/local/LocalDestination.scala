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

import slamdata.Predef._

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.{Blocker, ContextShift, Effect}

import fs2.{io, Stream}

import quasar.api.destination.{ResultSink, UntypedDestination}
import quasar.api.push.RenderConfig
import quasar.connector.{MonadResourceErr, ResourceError}

import java.nio.file.{Path => JPath}

final class LocalDestination[F[_]: Effect: ContextShift: MonadResourceErr] private (
    root: JPath,
    blocker: Blocker) extends UntypedDestination[F] {

  val destinationType = LocalDestinationType

  def sinks: NonEmptyList[ResultSink[F, Unit]] =
    NonEmptyList.of(csvSink(root, blocker))

  private def csvSink(root: JPath, blocker: Blocker): ResultSink[F, Unit] =
    ResultSink.csv(RenderConfig.Csv())((dst, columns, bytes) =>
        Stream.eval(resolvedResourcePath[F](root, dst)) >>= {
          case Some(writePath) =>
            val fileSink = io.file.writeAll[F](writePath, blocker)

            bytes.through(fileSink)

          case None =>
            Stream.eval(
              MonadResourceErr[F].raiseError(ResourceError.notAResource(dst)))
        })
}

object LocalDestination {
  def apply[F[_]: Effect: ContextShift: MonadResourceErr](
      root: JPath,
      blocker: Blocker)
      : LocalDestination[F] =
    new LocalDestination[F](root, blocker)
}
