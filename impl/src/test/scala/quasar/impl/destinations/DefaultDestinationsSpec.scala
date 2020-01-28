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

import quasar.ConditionMatchers
import quasar.api.destination._
import quasar.api.destination.DestinationError._
import quasar.{concurrent => qc}
import quasar.connector.ResourceError
import quasar.contrib.scalaz.MonadError_
import quasar.impl.ResourceManager
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}

import argonaut.Json
import argonaut.JsonScalaz._

import cats.Show
import cats.instances.int._
import cats.instances.string._
import cats.instances.option._
import cats.effect.{Blocker, IO, Resource}
import cats.effect.concurrent.Ref
import cats.syntax.applicativeError._
import cats.syntax.traverse._

import eu.timepit.refined.auto._

import scalaz.ISet

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext.Implicits.global

import shims.{showToCats, showToScalaz, orderToScalaz, applicativeToScalaz}

object DefaultDestinationsSpec extends quasar.EffectfulQSpec[IO] with ConditionMatchers {
  sequential
  implicit val tm = IO.timer(global)
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

  val blocker: Blocker = qc.Blocker.cached("rdestinations-spec")

  def mkDestinations = {
    val freshId = IO(java.util.UUID.randomUUID().toString())
    val fRefs: IO[IndexedStore[IO, String, DestinationRef[Json]]] =
      IO(new ConcurrentHashMap[String, DestinationRef[Json]]()).map { (mp: ConcurrentHashMap[String, DestinationRef[Json]]) =>
        ConcurrentMapIndexedStore.unhooked[IO, String, DestinationRef[Json]](mp, blocker)
      }
    val rCache = ResourceManager[IO, String, Destination[IO]]
    val modules = DestinationModules[IO, String](List(MockDestinationModule))
    for {
      refs <- Resource.liftF(fRefs)
      cache <- rCache
      result <- Resource.liftF(DefaultDestinations(freshId, refs, cache, modules))
    } yield (refs, result, cache)
  }

  def emptyDestinations = mkDestinations map (_._2)

  val MockDestinationType = MockDestinationModule.destinationType

  val testRef =
    DestinationRef(MockDestinationType, DestinationName("foo-mock"), Json.jEmptyString)

  val sanitize = DestinationRef.config.set(Json.jString("sanitized"))

  "destinations" >> {
    "add destination" >> {
      "creates and saves destinations" >>* {
        for {
          (dests, finalize) <- emptyDestinations.allocated
          result <- dests.addDestination(testRef)
          foundRef <- result.toOption.map(dests.destinationRef(_)).sequence
          _ <- finalize
        } yield {
          result must be_\/-
          foundRef must beSome
        }
      }
      "rejects duplicate names" >>* {
        for {
          (dests, finalize) <- emptyDestinations.allocated
          original <- dests.addDestination(testRef)
          duplicate <- dests.addDestination(testRef)
          _ <- finalize
        } yield {
          original must be_\/-
          duplicate must be_-\/(destinationNameExists(DestinationName("foo-mock")))
        }
      }
      "rejects unknown destination" >>* {
        val unknownType = DestinationType("unknown", 1L)
        val unknownRef = DestinationRef.kind.set(unknownType)(testRef)
        for {
          (dests, finalize) <- emptyDestinations.allocated
          addResult <- dests.addDestination(unknownRef)
        } yield {
          addResult must be_-\/(destinationUnsupported(unknownType, ISet.singleton(MockDestinationType)))
        }
      }
    }
    "replace destination" >> {
      "replaces a destination" >>* {
        val newRef = DestinationRef.name.set(DestinationName("foo-mock-2"))(testRef)
        for {
          ((store, dests, _), finalize) <- mkDestinations.allocated
          _ <- store.insert("1", testRef)
          beforeReplace <- dests.destinationRef("1")
          replaceResult <- dests.replaceDestination("1", newRef)
          afterReplace <- dests.destinationRef("1")
          _ <- finalize
        } yield {
          beforeReplace must be_\/-(sanitize(testRef))
          replaceResult must beNormal
          afterReplace must be_\/-(sanitize(newRef))
        }
      }
      "verifies name uniqueness on replacement" >>* {
        val testRef2 =
          DestinationRef.name.set(DestinationName("foo-mock-2"))(testRef)
        val testRef3 =
          DestinationRef.name.set(DestinationName("foo-mock-2"))(testRef)
        for {
          (dests, finalize) <- emptyDestinations.allocated
          addStatus1 <- dests.addDestination(testRef)
          addStatus2 <- dests.addDestination(testRef2)
          replaceStatus <- addStatus1.toOption.traverse { id => dests.replaceDestination(id, testRef2) }
          _ <- finalize
        } yield {
          addStatus1 must be_\/-
          addStatus2 must be_\/-
          replaceStatus must beLike {
            case Some(x) => x must beAbnormal(destinationNameExists[DestinationError[Int, Json]](testRef2.name))
          }
        }
      }
      "allows replacement with the same name" >>* {
        val testRef2 = DestinationRef.config.set(Json.jString("modified"))(testRef)
        for {
          (dests, finalize) <- emptyDestinations.allocated
          addStatus <- dests.addDestination(testRef)
          replaceStatus <- addStatus.toOption.traverse { id => dests.replaceDestination(id, testRef2) }
          _ <- finalize
        } yield {
          addStatus must be_\/-
          replaceStatus must beLike { case Some(x) => x must beNormal }
        }
      }
      "shuts down replaced destination" >>* {
        def tracking(ref: Ref[IO, Option[String]], i: String): (Destination[IO], IO[Unit]) =
          (new MockDestination[IO], ref.set(Some(i)))
        val testRef2 =
          DestinationRef.config.set(Json.jString("modified"))(testRef)

        for {
          ((store, dests, cache), finalize) <- mkDestinations.allocated
          addStatus <- dests.addDestination(testRef)
          i0 = addStatus.fold(x => "", x => x)
          disposes <- Ref.of[IO, Option[String]](None)
          beforeHack <- cache.get(i0)
          _ <- cache.shutdown(i0)
          _ <- cache.manage(i0, tracking(disposes, i0))
          _ <- dests.replaceDestination(i0, testRef2)
          afterHack <- cache.get(i0)
          result <- disposes.get
          _ <- finalize
        } yield {
          beforeHack must beSome
          afterHack must beSome
          result must beSome(i0)
        }
      }
    }
    "destination status" >> {
      "returns an error for an unknown destination" >>* {
        for {
          (dests, finalize) <- emptyDestinations.allocated
          res <- dests.destinationStatus("foo")
          _ <- finalize
        } yield {
          res must be_-\/(destinationNotFound("foo"))
        }
      }
      "returns a normal condition for known destination" >>* {
        for {
          (dests, finalize) <- emptyDestinations.allocated
          a <- dests.addDestination(testRef)
          res <- dests.destinationStatus(a.fold(x => "", x => x))
          _ <- finalize
        } yield {
          res must be_\/-(beNormal[Exception])
        }
      }
    }
    "destination removal" >> {
      "removes a destination" >>* {
        for {
          (dests, finalize) <- emptyDestinations.allocated
          i <- dests.addDestination(testRef)
          removed <- i.toOption.traverse(id => dests.removeDestination(id))
          _ <- finalize
        } yield {
          removed must beLike {
            case Some(x) => x must beNormal
          }
        }
      }
    }
    "destination lookup" >> {
      "returns sanitized refs" >>* {
        for {
          (dests, finalize) <- emptyDestinations.allocated
          addResult <- dests.addDestination(testRef)
          found <- addResult.toOption.traverse(dests.destinationRef(_))
          foundRef = found.flatMap(_.toOption)
        } yield {
          foundRef must beSome(DestinationRef.config.set(Json.jString("sanitized"))(testRef))
        }
      }
    }
  }
  "clustering" >> {
    "new ref appeared" >>* {
      for {
        ((store, dests, cache), finalize) <- mkDestinations.allocated
        _ <- store.insert("foo", testRef)
        d <- dests.destinationOf("foo")
        _ <- finalize
      } yield {
        d must be_\/-
      }
    }
    "ref disappered" >>* {
      for {
        ((store, dests, cache), finalize) <- mkDestinations.allocated
        _ <- store.insert("foo", testRef)
        d0 <- dests.destinationOf("foo")
        _ <- store.delete("foo")
        d1 <- dests.destinationOf("foo")
        _ <- finalize
      } yield {
        d0 must be_\/-
        d1 must be_-\/
      }
    }
    "ref updated" >>* {
      def tracking(ref: Ref[IO, Option[String]], i: String): (Destination[IO], IO[Unit]) =
        (new MockDestination[IO], ref.set(Some(i)))
      val testRef2 =
        DestinationRef.config.set(Json.jString("modified"))(testRef)

      for {
        ((store, dests, cache), finalize) <- mkDestinations.allocated
        _ <- store.insert("foo", testRef)
        _ <- dests.destinationOf("foo")
        trackingRef <- Ref.of[IO, Option[String]](None)
        _ <- cache.manage("foo", tracking(trackingRef, "foo"))
        tracked0 <- trackingRef.get
        _ <- store.insert("foo", testRef2)
        _ <- dests.destinationOf("foo")
        tracked1 <- trackingRef.get
        _ <- finalize
      } yield {
        tracked0 must beNone
        tracked1 must beSome("foo")
      }
    }
  }
}
