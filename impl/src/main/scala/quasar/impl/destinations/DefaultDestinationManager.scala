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

package quasar.impl.destinations

import slamdata.Predef._

import quasar.Condition
import quasar.api.destination.DestinationError
import quasar.api.destination.DestinationError.CreateError
import quasar.api.destination.{DestinationRef, DestinationType}
import quasar.connector.{Destination, DestinationModule, MonadResourceErr}
import quasar.impl.IncompatibleModuleException.linkDestination

import argonaut.Json
import argonaut.Argonaut.jEmptyObject
import cats.effect.concurrent.Ref
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.std.either._
import scalaz.syntax.unzip._
import scalaz.{EitherT, IMap, ISet, OptionT, Order}
import shims._

class DefaultDestinationManager[I: Order, F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer] (
  modules: IMap[DestinationType, DestinationModule],
  running: Ref[F, IMap[I, (Destination[F], F[Unit])]],
  currentErrors: Ref[F, IMap[I, Exception]]) extends DestinationManager[I, Json, F] {

  def initDestination(destinationId: I, ref: DestinationRef[Json])
      : F[Condition[CreateError[Json]]] =
    (for {
      supported <- EitherT.rightT(supportedDestinationTypes)

      mod <- EitherT.fromDisjunction[F](
        modules.lookup(ref.kind)
          .map(m => linkDestination(ref.kind, m.destination[F](ref.config)))
          .toRight(DestinationError.destinationUnsupported[CreateError[Json]](ref.kind, supported))
          .disjunction)

      runningNow <- EitherT.rightT(running.get)
      _ <- EitherT.rightT(runningNow.lookup(destinationId).fold(().point[F]) { case (_, shutdown) => shutdown })

      (dest0, disposeM) <- EitherT.rightT(mod.allocated)
      dest <- EitherT.either[F, CreateError[Json], Destination[F]](dest0.disjunction)

      added <- EitherT.rightT(
        running.update(r => r.insert(destinationId, (dest, disposeM))))
    } yield added).run.map(Condition.disjunctionIso.reverseGet(_))

  def destinationOf(destinationId: I): F[Option[Destination[F]]] =
    running.get.map(_.lookup(destinationId).firsts)

  def sanitizedRef(ref: DestinationRef[Json]): DestinationRef[Json] =
    // return an empty object in case we don't find an appropriate
    // sanitizeDestinationConfig implementation
    modules.lookup(ref.kind).map(_.sanitizeDestinationConfig(ref.config))
      .fold(ref.copy(config = jEmptyObject))(nc => ref.copy(config = nc))

  def shutdownDestination(destinationId: I): F[Unit] =
    OptionT(
      running.modify(r =>
        (r.delete(destinationId), r.lookup(destinationId).seconds)))
      .getOrElse(().point[F]).join

  def supportedDestinationTypes: F[ISet[DestinationType]] =
    modules.keySet.point[F]

  def errors: F[IMap[I, Exception]] =
    currentErrors.get

  def errorsOf(i: I): F[Option[Exception]] =
    errors.map(_.lookup(i))
}

object DefaultDestinationManager {
  def apply[I: Order, F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    modules: IMap[DestinationType, DestinationModule],
    running: Ref[F, IMap[I, (Destination[F], F[Unit])]],
    currentErrors: Ref[F, IMap[I, Exception]]): DefaultDestinationManager[I, F] =
    new DefaultDestinationManager[I, F](modules, running, currentErrors)
}
