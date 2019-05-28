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

import slamdata.Predef.{Stream => _, _}

import quasar.{Condition, ConditionMatchers}
import quasar.api.destination.{DestinationError, DestinationMeta, DestinationName, DestinationRef, DestinationType, Destinations}
import quasar.connector.{Destination, DestinationModule, ResourceError}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.RefIndexedStore

import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import cats.effect.IO
import cats.effect.concurrent.Ref
import eu.timepit.refined.auto._
import fs2.Stream
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.unzip._
import scalaz.{IMap, ISet}
import shims._

object DefaultDestinationsSpec extends quasar.Qspec with ConditionMatchers {
  implicit val cs = IO.contextShift(global)
  implicit val tmr = IO.timer(global)
  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  def freshId(ref: Ref[IO, Int]): IO[Int] =
    ref.modify(i => (i + 1, i + 1))

  val mockModules: IMap[DestinationType, DestinationModule] =
    IMap(MockDestinationModule.destinationType -> MockDestinationModule)

  def manager(
    running: IMap[Int, (Destination[IO], IO[Unit])],
    errors: IMap[Int, Exception],
    modules: IMap[DestinationType, DestinationModule]): IO[DestinationManager[Int, Json, IO]] =
    (Ref[IO].of(running) |@| Ref[IO].of(errors))(
      DefaultDestinationManager[Int, IO](modules, _, _))

  def mkDestinations(
    store: IMap[Int, DestinationRef[Json]],
    running: IMap[Int, (Destination[IO], IO[Unit])],
    errors: IMap[Int, Exception]): IO[Destinations[IO, Stream[IO, ?], Int, Json]] =
    for {
      initialIdRef <- Ref.of[IO, Int](0)
      fId = freshId(initialIdRef)
      storeRef <- Ref[IO].of(store)
      store = RefIndexedStore[Int, DestinationRef[Json]](storeRef)
      mgr <- manager(running, errors, mockModules)
    } yield DefaultDestinations[Int, Json, IO](fId, store, mgr)

  def emptyDestinations: IO[Destinations[IO, Stream[IO, ?], Int, Json]] =
    mkDestinations(IMap.empty, IMap.empty, IMap.empty)

  val MockDestinationType = MockDestinationModule.destinationType

  val testRef =
    DestinationRef(MockDestinationType, DestinationName("foo-mock"), Json.jEmptyString)

  "destinations" >> {
    "add destination" >> {
      "creates and saves destinations" >> {
        val testRun = for {
          dests <- emptyDestinations
          result <- dests.addDestination(testRef)
          foundRef <- result.toOption.map(dests.destinationRef(_)).sequence
        } yield (result, foundRef)

        val (res, found) = testRun.unsafeRunSync

        res must be_\/-
        found must beSome
      }

      "rejects duplicate names" >> {
        val testRun = for {
          dests <- emptyDestinations
          original <- dests.addDestination(testRef)
          duplicate <- dests.addDestination(testRef)
        } yield (original, duplicate)

        val (original, duplicate) = testRun.unsafeRunSync

        original must be_\/-
        duplicate must be_-\/(
          DestinationError.destinationNameExists(DestinationName("foo-mock")))
      }

      "rejects unknown destination" >> {
        val unknownType = DestinationType("unknown", 1L)
        val unknownRef = DestinationRef.kind.set(unknownType)(testRef)

        val testRun = for {
          dests <- emptyDestinations
          addResult <- dests.addDestination(unknownRef)
        } yield addResult

        testRun.unsafeRunSync must be_-\/(
          DestinationError.destinationUnsupported(unknownType, ISet.singleton(MockDestinationType)))
      }
    }

    "replace destination" >> {
      "replaces a destination" >> {
        val newRef = DestinationRef.name.set(DestinationName("foo-mock-2"))(testRef)
        val testRun = for {
          dests <- mkDestinations(
            IMap(1 -> testRef),
            IMap(1 -> ((new MockDestination, ().point[IO]))),
            IMap.empty)
          beforeReplace <- dests.destinationRef(1)
          replaceResult <- dests.replaceDestination(1, newRef)
          afterReplace <- dests.destinationRef(1)
        } yield (beforeReplace, replaceResult, afterReplace)

        val (beforeReplace, replaceResult, afterReplace) = testRun.unsafeRunSync

        val sanitize = DestinationRef.config.set(Json.jString("sanitized"))

        beforeReplace must be_\/-(sanitize(testRef))
        replaceResult must beNormal
        afterReplace must be_\/-(sanitize(newRef))
      }

      "shuts down replaced destination" >> {
        def mkRunning(ref: Ref[IO, List[Int]], id: Int): IMap[Int, (Destination[IO], IO[Unit])] =
          IMap(id -> ((new MockDestination[IO], ref.set(List(id)))))

        val testRun = for {
          disposes <- Ref.of[IO, List[Int]](List.empty)
          running = mkRunning(disposes, 1)
          dests <- mkDestinations(IMap(1 -> testRef), running, IMap.empty)
          beforeReplace <- disposes.get
          replaceResult <- dests.replaceDestination(1, testRef)
          afterReplace <- disposes.get
        } yield (beforeReplace, replaceResult, afterReplace)

        val (beforeReplace, replaceResult, afterReplace) = testRun.unsafeRunSync

        beforeReplace must_== List()
        replaceResult must beNormal
        afterReplace must_== List(1)
      }
    }

    "destination status" >> {
      "returns an error for an unknown destination" >> {
        val testRun = for {
          dests <- emptyDestinations
          res <- dests.destinationStatus(42)
        } yield res

        testRun.unsafeRunSync must be_-\/(DestinationError.destinationNotFound(42))
      }

      "return a normal condition when no errors are present" >> {
        val testRun = for {
          dests <- mkDestinations(IMap(1 -> testRef), IMap.empty, IMap.empty)
          res <- dests.destinationStatus(1)
        } yield res

        testRun.unsafeRunSync must be_\/-(beNormal[Exception])
      }

      "returns error for destinations with errors" >> {
        val ex = new Exception("oh noes")
        val testRun = for {
          dests <- mkDestinations(IMap(1 -> testRef), IMap.empty, IMap(1 -> ex))
          res <- dests.destinationStatus(1)
        } yield res

        testRun.unsafeRunSync must be_\/-(beAbnormal(ex))
      }
    }

    "destination metadata" >> {
      "includes exception when a destination has errored" >> {
        val ex = new Exception("oh noes")

        val testRun = for {
          dests <- mkDestinations(IMap(1 -> testRef), IMap.empty, IMap(1 -> ex))
          st <- dests.allDestinationMetadata
          allMeta <- st.compile.toList
        } yield allMeta.seconds

        testRun.unsafeRunSync must_== List(
          DestinationMeta(
            MockDestinationModule.destinationType,
            DestinationName("foo-mock"),
            Condition.abnormal(ex)))
      }
    }

    "destination removal" >> {
      "removes a destination" >> {
        val testRun = for {
          dests <- mkDestinations(IMap(10 -> testRef), IMap.empty, IMap.empty)
          removed <- dests.removeDestination(10)
          retrieved <- dests.destinationRef(10)
        } yield (removed, retrieved)

        val (removed, retrieved) = testRun.unsafeRunSync

        removed must beNormal
        retrieved must be_-\/(DestinationError.destinationNotFound(10))
      }
    }

    "destination lookup" >> {
      "returns sanitized refs" >> {
        val testRun = for {
          dests <- emptyDestinations
          addResult <- dests.addDestination(testRef)
          found <- addResult.toOption.map(dests.destinationRef(_)).sequence
          foundRef = found >>= (_.toOption)
        } yield foundRef

        testRun.unsafeRunSync must beSome(
          DestinationRef.config.set(Json.jString("sanitized"))(testRef))
      }
    }
  }
}
