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

package quasar.impl.external

import slamdata.Predef._
import quasar.connector.DestinationModule

import java.lang.{
  Class,
  ClassCastException,
  ClassLoader,
  ExceptionInInitializerError,
  IllegalAccessException,
  IllegalArgumentException,
  NoSuchFieldException,
  NullPointerException
}

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Stream
import org.slf4s.Logging

object ExternalDestinations extends Logging {
  def apply[F[_]: ContextShift: Timer](
      config: ExternalConfig,
      blocker: Blocker)(
      implicit F: ConcurrentEffect[F])
      : F[List[DestinationModule]] = {
    val destinationModuleStream: Stream[F, DestinationModule] =
      ExternalModules(config, blocker)
        .filter {
          case (_, _, PluginType.Destination) => true
          case _ => false
        }
        .map {
          case (name, loader, _) => (name, loader)
        }
        .flatMap((loadDestinationModule[F](_, _)).tupled)

    for {
      _ <- ConcurrentEffect[F].delay(log.info("Loading destinations"))
      dts <- destinationModuleStream.compile.toList
      _ <- ConcurrentEffect[F].delay(log.info(s"Loaded ${dts.length} destination(s)"))
    } yield dts
  }

  ////

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def loadDestinationModule[F[_]: Sync](
    className: ClassName, classLoader: ClassLoader)
      : Stream[F, DestinationModule] = {
    def handleFailedDestination[A](s: Stream[F, A]): Stream[F, A] =
      s recoverWith {
        case e @ (_: NoSuchFieldException | _: IllegalAccessException | _: IllegalArgumentException | _: NullPointerException) =>
          ExternalModules.warnStream[F](s"Destination module '$className' does not appear to be a singleton object", Some(e))

        case e: ExceptionInInitializerError =>
          ExternalModules.warnStream[F](s"Destination module '$className' failed to load with exception", Some(e))

        case _: ClassCastException =>
          ExternalModules.warnStream[F](s"Datasource module '$className' is not actually a subtype of DestinationModule", None)
      }

    def loadDestination(clazz: Class[_]): Stream[F, DestinationModule] =
      ExternalModules.loadModule(clazz)(_.asInstanceOf[DestinationModule])

    for {
      clazz <- ExternalModules.loadClass(className, classLoader)
      destination <- handleFailedDestination(loadDestination(clazz))
    } yield destination
  }
}
