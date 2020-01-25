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

package quasar.impl.destinations

import slamdata.Predef._

import quasar.api.destination._
import quasar.api.destination.DestinationError._
import quasar.connector.{DestinationModule, MonadResourceErr}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.IncompatibleModuleException.linkDestination

import argonaut.Json
import argonaut.Argonaut.jEmptyObject

import cats.MonadError
import cats.effect.{Resource, ConcurrentEffect, ContextShift, Timer}
import cats.syntax.applicative._
import cats.syntax.bifunctor._
import cats.instances.either._

import scalaz.ISet

trait DestinationModules[F[_], I, C] {
  def create(ref: DestinationRef[C]): Resource[F, Destination[F]]
  def sanitizeRef(inp: DestinationRef[C]): DestinationRef[C]
  def supportedTypes: F[ISet[DestinationType]]
}

object DestinationModules {
  def apply[
      F[_]: ConcurrentEffect: ContextShift: Timer: MonadResourceErr: MonadError_[?[_], CreateError[Json]],
      I](modules: List[DestinationModule]): DestinationModules[F, I, Json] = {
    lazy val moduleSet: ISet[DestinationType] = ISet.fromList(modules.map(_.destinationType))
    lazy val moduleMap: Map[DestinationType, DestinationModule] = Map(modules.map(ds => (ds.destinationType, ds)):_*)

    new DestinationModules[F, I, Json] {
      def create(ref: DestinationRef[Json]): Resource[F, Destination[F]] = moduleMap.get(ref.kind) match {
        case None =>
          Resource.liftF(MonadError_[F, CreateError[Json]].raiseError(DestinationUnsupported(ref.kind, moduleSet)))
        case Some(module) =>
          handleInitErrors(ref.kind, module.destination[F](ref.config))
      }

      def sanitizeRef(inp: DestinationRef[Json]): DestinationRef[Json] = moduleMap.get(inp.kind) match {
        case None => inp.copy(config = jEmptyObject)
        case Some(x) => inp.copy(config = x.sanitizeDestinationConfig(inp.config))
      }

      def supportedTypes: F[ISet[DestinationType]] =
        moduleSet.pure[F]
    }
  }

  private def handleInitErrors[F[_]: MonadError_[?[_], CreateError[Json]]: MonadError[?[_], Throwable], A](
      kind: DestinationType,
      res: Resource[F, Either[InitializationError[Json], A]])
      : Resource[F, A] = {
    import quasar.contrib.cats.monadError.monadError_CatsMonadError

    implicit val merr: MonadError[F, CreateError[Json]] =
      monadError_CatsMonadError[F, CreateError[Json]](
        MonadError[F, Throwable], MonadError_[F, CreateError[Json]])

    val rmerr = MonadError[Resource[F, ?], CreateError[Json]]

    rmerr.rethrow(rmerr.map(linkDestination(kind, res))(_.leftMap(ie => ie: CreateError[Json])))

  }
}
