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
import quasar.api.destination.{DestinationError, DestinationName, DestinationRef, DestinationType}
import quasar.connector.{Destination, DestinationModule, ResourceError}
import quasar.contrib.scalaz.MonadError_

import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import cats.effect.IO
import cats.effect.concurrent.Ref
import eu.timepit.refined.auto._
import scalaz.{IMap, ISet}
import scalaz.std.anyVal._
import scalaz.syntax.applicative._
import shims._

object DefaultDestinationManagerSpec extends quasar.Qspec {
  implicit val cs = IO.contextShift(global)
  implicit val tmr = IO.timer(global)
  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val MockDestinationType = MockDestinationModule.destinationType

  val modules: IMap[DestinationType, DestinationModule] =
    IMap(MockDestinationModule.destinationType -> MockDestinationModule)

  val runningManager: IMap[Int, (Destination[IO], IO[Unit])] => IO[DestinationManager[Int, Json, IO]] =
    running =>
      (Ref[IO].of(running) |@| Ref.of[IO, IMap[Int, Exception]](IMap.empty))((run, errs) =>
        (DefaultDestinationManager[Int, IO](modules, run, errs)))

  val emptyManager: IO[DestinationManager[Int, Json, IO]] = runningManager(IMap.empty)

  val testRef = DestinationRef(MockDestinationType, DestinationName("foo_mock"), Json.jEmptyString)

  "default destination manager" >> {
    "initialization" >> {
      "returns configured destination types" >> {
        emptyManager.flatMap(_.supportedDestinationTypes).unsafeRunSync must_== ISet.singleton(MockDestinationType)
      }

      "initializes a destination" >> {
        val inited = for {
          mgr <- emptyManager
          _ <- mgr.initDestination(1, testRef)
          justSaved <- mgr.destinationOf(1)
        } yield justSaved

        inited.unsafeRunSync.map(_.destinationType) must beSome(MockDestinationType)
      }

      "rejects a destination of an unknown type" >> {
        val notKnown = DestinationType("notknown", 1L)
        val ref = DestinationRef(notKnown, DestinationName("notknown"), Json.jEmptyString)

        emptyManager.flatMap(_.initDestination(1, ref)).unsafeRunSync must beLike {
          case Condition.Abnormal(DestinationError.DestinationUnsupported(k, s)) =>
            k must_== notKnown
            s must_== ISet.singleton(MockDestinationType)
        }
      }

      "disposes existing destination when initializing a new destination with the same id" >> {
        val DestId = 42

        val runningDest: Ref[IO, List[Int]] => IMap[Int, (Destination[IO], IO[Unit])] =
          r => IMap(DestId -> ((new MockDestination, r.set(List(DestId)))))

        val testRun = for {
          disposed <- Ref.of[IO, List[Int]](List.empty)
          running = runningDest(disposed)
          mgr <- runningManager(running)
          before <- disposed.get
          _ <- mgr.initDestination(DestId, testRef)
          after <- disposed.get
        } yield (before, after)

        val (beforeInit, afterInit) = testRun.unsafeRunSync

        beforeInit must_== List()
        afterInit must_== List(DestId)
      }
    }

    "shutdown" >> {
      "removes a destination on shutdown" >> {
        val shutdown = for {
          mgr <- emptyManager
          _ <- mgr.initDestination(1, testRef)
          beforeShutdown <- mgr.destinationOf(1)
          _ <- mgr.shutdownDestination(1)
          afterShutdown <- mgr.destinationOf(1)
        } yield (beforeShutdown, afterShutdown)

        val (before, after) = shutdown.unsafeRunSync

        before must beSome
        after must beNone
      }

      "disposes a destination on shutdown" >> {
        val DestId = 42

        val runningDest: Ref[IO, List[Int]] => IMap[Int, (Destination[IO], IO[Unit])] =
          r => IMap(DestId -> ((new MockDestination, r.set(List(DestId)))))

        val testRun = for {
          disposed <- Ref.of[IO, List[Int]](List.empty)
          running = runningDest(disposed)
          mgr <- runningManager(running)
          beforeShutdown <- disposed.get
          _ <- mgr.shutdownDestination(DestId)
          afterShutdown <- disposed.get
        } yield (beforeShutdown, afterShutdown)

        val (beforeDispose, afterDispose) = testRun.unsafeRunSync

        beforeDispose must_== List()
        afterDispose must_== List(DestId)
      }

      "nop with non-existent destination" >> {
        val testRun = for {
          mgr <- emptyManager
          before <- mgr.destinationOf(1)
          _ <- mgr.shutdownDestination(1)
          after <- mgr.destinationOf(1)
        } yield (before, after)

        val (before, after) = testRun.unsafeRunSync

        before must beNone
        after must beNone
      }
    }

    "sanitize refs" >> {
      "returns an empty object when the destination is unknown" >> {
        val notKnown = DestinationType("notknown", 1L)
        val notKnownRef = DestinationRef(notKnown, DestinationName("notknown"), Json.jString("baz"))

        val sanitized = for {
          mgr <- emptyManager
          s = mgr.sanitizedRef(notKnownRef)
        } yield s

        sanitized.unsafeRunSync must_== DestinationRef(notKnown, DestinationName("notknown"), Json.jEmptyObject)
      }

      "uses module-specific implementation to sanitize references" >> {
        val sanitized = for {
          mgr <- emptyManager
          s = mgr.sanitizedRef(testRef)
        } yield s

        sanitized.unsafeRunSync must_== DestinationRef(MockDestinationType, DestinationName("foo_mock"), Json.jString("sanitized"))
      }
    }
  }
}
