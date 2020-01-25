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

import quasar.EffectfulQSpec

import quasar.api.destination._
import quasar.api.destination.DestinationError._
import quasar.api.push.RenderConfig
import quasar.connector._
import quasar.contrib.scalaz.MonadError_

import cats.Show
import cats.instances.int._
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.effect.{ConcurrentEffect, ContextShift, IO, Resource, Timer}

import argonaut.Json
import argonaut.JsonScalaz._

import eu.timepit.refined.auto._

import fs2.Stream

import scala.concurrent.ExecutionContext.Implicits.global

import scalaz.{NonEmptyList, ISet}

import shims.{showToCats, showToScalaz}

object DestinationModulesSpec extends EffectfulQSpec[IO] {
  implicit val tmr = IO.timer(global)

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  final case class CreateErrorException(ce: CreateError[Json])
      extends Exception(Show[DestinationError[Int, Json]].show(ce))

  implicit val ioCreateErrorME: MonadError_[IO, CreateError[Json]] =
    new MonadError_[IO, CreateError[Json]] {
      def raiseError[A](e: CreateError[Json]): IO[A] =
        IO.raiseError(new CreateErrorException(e))

      def handleError[A](fa: IO[A])(f: CreateError[Json] => IO[A]): IO[A] =
        fa.recoverWith {
          case CreateErrorException(e) => f(e)
        }
    }

  def module(kind: DestinationType, err: Option[InitializationError[Json]] = None) = new DestinationModule {
    def destinationType = kind
    def sanitizeDestinationConfig(inp: Json) = Json.jString("sanitized")
    def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](config: Json) = {
      val dest: Destination[F] = new Destination[F] {
        def destinationType = kind
        def sinks = NonEmptyList(mock)
        val mock = ResultSink.csv[F](RenderConfig.Csv()) {
          case _ => Stream(())
        }
      }
      err match {
        case None => dest.asRight[InitializationError[Json]].pure[Resource[F, ?]]
        case Some(a) => a.asLeft[Destination[F]].pure[Resource[F, ?]]
      }
    }
  }

  def mkModules(lst: List[DestinationModule]) = DestinationModules[IO, String](lst)


  "supported types" >> {
    "empty" >>* {
      for {
        actual <- mkModules(List()).supportedTypes
      } yield {
        actual === ISet.empty
      }
    }
    "non-empty" >>* {
      val expected = ISet.fromList(List(DestinationType("foo", 1L), DestinationType("bar", 2L)))
      for {
        actual <- mkModules(List(module(DestinationType("foo", 1L)), module(DestinationType("bar", 2L)))).supportedTypes
      } yield {
        actual === expected
      }
    }
  }

  "sanitizeRef" >> {
    val kind = DestinationType("foo", 1L)
    val modules = mkModules(List(module(kind)))
    val supported = DestinationRef(kind, DestinationName("supported"), Json.jString("foo"))
    val unsupported = DestinationRef(DestinationType("bar", 2L), DestinationName("unsupported"), Json.jString("bar"))

    modules.sanitizeRef(supported) === supported.copy(config = Json.jString("sanitized"))
    modules.sanitizeRef(unsupported) === unsupported.copy(config = Json.jEmptyObject)
  }

  "create" >> {
    "work for supported" >>* {
      val kind = DestinationType("foo", 1L)
      val ref = DestinationRef(kind, DestinationName("supported"), Json.jString(""))
      for {
        (mod, finish) <- mkModules(List(module(kind))).create(ref).allocated
        _ <- finish
      } yield {
        mod.destinationType === kind
      }
    }
    "doesn't work for unsupported" >>* {
      val kind = DestinationType("foo", 1L)
      val ref = DestinationRef(DestinationType("bar", 2L), DestinationName("unsupported"), Json.jString(""))
      for {
        res <- mkModules(List(module(kind))).create(ref).allocated.attempt
      } yield {
        res must beLike {
          case Left(CreateErrorException(ce)) =>
            ce === DestinationUnsupported(DestinationType("bar", 2L), ISet.singleton(DestinationType("foo", 1L)))
        }
      }
    }
    "errors with initialization error" >>* {
      val malformed =
        DestinationType("malformed", 1L)
      val malformedRef =
        DestinationRef(malformed, DestinationName("doesn't matter"), Json.jString("malformed-config"))
      val invalid =
        DestinationType("invalid", 1L)
      val invalidRef =
        DestinationRef(invalid, DestinationName("doesn't matter"), Json.jString("invalid-config"))
      val connFailed =
        DestinationType("conn-failed", 1L)
      val connFailedRef =
        DestinationRef(connFailed, DestinationName("doesn't matter"), Json.jString("conn-failed-config"))
      val accessDenied =
        DestinationType("access-denied", 1L)
      val accessDeniedRef =
        DestinationRef(accessDenied, DestinationName("doesn't matter"), Json.jString("access-denied-config"))
      val modules = mkModules(List(
          module(malformed, Some(MalformedConfiguration(malformed, Json.jString("a"), "malformed configuration"))),
          module(invalid, Some(InvalidConfiguration(invalid, Json.jString("b"), NonEmptyList("invalid configuration")))),
          module(connFailed, Some(ConnectionFailed(connFailed, Json.jString("c"), new Exception("conn failed")))),
          module(accessDenied, Some(AccessDenied(accessDenied, Json.jString("d"), "access denied")))))

      for {
        malformedDs <- modules.create(malformedRef).allocated.attempt
        invalidDs <- modules.create(invalidRef).allocated.attempt
        connFailedDs <- modules.create(connFailedRef).allocated.attempt
        accessDeniedDs <- modules.create(accessDeniedRef).allocated.attempt
      } yield {
        malformedDs must beLike {
          case Left(CreateErrorException(ce)) =>
            ce === MalformedConfiguration(malformed, Json.jString("a"), "malformed configuration")
        }
        invalidDs must beLike {
          case Left(CreateErrorException(ce)) =>
            ce === InvalidConfiguration(invalid, Json.jString("b"), NonEmptyList("invalid configuration"))
        }
        connFailedDs must beLike {
          case Left(CreateErrorException(ConnectionFailed(kind, config, cause))) =>
            kind === connFailed
            config === Json.jString("c")
            cause.getMessage === "conn failed"
        }
        accessDeniedDs must beLike {
          case Left(CreateErrorException(ce)) =>
            ce === AccessDenied(accessDenied, Json.jString("d"), "access denied")
        }
      }
    }
  }
}
