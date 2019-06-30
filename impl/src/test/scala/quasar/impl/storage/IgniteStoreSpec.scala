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

package quasar.impl.storage

import slamdata.Predef._

import quasar.concurrent.BlockingContext
import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.{IO, Resource}
import cats.effect.concurrent.Deferred
import cats.syntax.functor._

import scalaz.std.anyVal._
import scalaz.std.string._
import scala.util.Random

import org.apache.ignite._

final class IgniteStoreSpec extends IndexedStoreSpec[IO, Int, String] {
  sequential

  val blockingPool: BlockingContext = BlockingContext.cached("ignite-indexed-store-spec")
  val freshIndex: IO[Int] = IO(Random.nextInt)
  val ignite: Resource[IO, Ignite] = {
    def mkCreationListener(d: Deferred[IO, Ignite]): IgnitionListener = new IgnitionListener {
      def onStateChange(name: String, state: IgniteState): Unit = state match {
        case IgniteState.STARTED =>
          d.complete(Ignition.ignite()).unsafeRunSync
        case y =>
      }
    }

    val create: IO[Ignite] = for {
      d <- Deferred.tryable[IO, Ignite]
      ignite <- IO(Ignition.start()).start
      creationListener = mkCreationListener(d)
      _ <- IO(Ignition.addListener(creationListener))
      i <- d.get
      _ <- IO(Ignition.removeListener(creationListener))
    } yield i

    def mkStoppingListener(d: Deferred[IO, Unit]): IgnitionListener = new IgnitionListener {
      def onStateChange(name: String, state: IgniteState): Unit = state match {
        case IgniteState.STARTED => ()
        case _ => d.complete(()).unsafeRunSync
      }
    }

    def stop(i: Ignite): IO[Unit] = for {
      _ <- IO(Ignition.stop(true)).start
      d <- Deferred.tryable[IO, Unit]
      stoppingListener = mkStoppingListener(d)
      _ <- IO(Ignition.addListener(stoppingListener))
      _ <- d.get
      _ <- IO(Ignition.removeListener(stoppingListener))
    } yield (())
    Resource.make(IO.suspend(create))(x => IO.suspend(stop(x)))
  }

  val emptyStore: Resource[IO, IndexedStore[IO, Int, String]] = ignite evalMap { (ig: Ignite) =>
    val cache = ig.getOrCreateCache[Int, String]("test")
    IO(IgniteStore[IO, Int, String](cache, blockingPool))
  }
  val valueA = "A"
  val valueB = "B"
}
